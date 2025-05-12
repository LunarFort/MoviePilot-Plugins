import re
import time
import warnings
from datetime import datetime, timedelta
from multiprocessing.dummy import Pool as ThreadPool
from threading import Lock
from typing import Optional, Any, List, Dict, Tuple

import pytz
import requests
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from ruamel.yaml import CommentedMap

from app.core.config import settings
# For V2, ensure eventmanager and relevant types are correctly imported
from app.core.event import eventmanager, Event
from app.db.site_oper import SiteOper
from app.helper.browser import PlaywrightHelper
# Assuming ModuleHelper is still the way to load these specific types of modules in V2,
# or a similar mechanism exists in V2's ModuleManager for this purpose.
from app.helper.module import ModuleHelper
from app.helper.sites import SitesHelper
from app.log import logger
from app.plugins import _PluginBase
from app.plugins.sitestatistic.siteuserinfo import ISiteUserInfo
# Updated imports for V2 EventType and NotificationType
from app.schemas.types import EventType, NotificationType, MessageChannel

warnings.filterwarnings("ignore", category=FutureWarning)

lock = Lock()


class SiteUnreadMsg(_PluginBase):
    # 插件名称
    plugin_name = "站点未读消息test"
    # 插件描述
    plugin_desc = "站点未读消息test。"
    # 插件图标
    plugin_icon = "Synomail_A.png"
    # 插件版本 (Consider updating for V2, e.g., "2.0" or "1.9.1-v2")
    plugin_version = "2.0"  # Updated version example
    # 插件作者
    plugin_author = "test"
    # 作者主页
    author_url = "https://github.com/LunarFort"
    # 插件配置项ID前缀
    plugin_config_prefix = "siteunreadmsg2_"
    # 加载顺序
    plugin_order = 1
    # 可使用的用户级别
    auth_level = 2

    # Private attributes
    sites: Optional[SitesHelper] = None
    siteoper: Optional[SiteOper] = None
    _scheduler: Optional[BackgroundScheduler] = None
    _history: List[Dict[str, Any]] = []
    _exits_key: List[str] = []
    _site_schema: List[ISiteUserInfo] = [] # Ensure it's initialized

    # Configuration attributes
    _enabled: bool = False
    _onlyonce: bool = False
    _cron: str = ""
    _notify: bool = False
    _queue_cnt: int = 5
    _history_days: int = 30
    _unread_sites: List[str] = [] # Store site IDs as strings if they come from JSON, or ensure type consistency

    # V2 compatibility flag
    is_v2: bool = False

    def init_plugin(self, config: dict = None):
        self.is_v2 = hasattr(settings, 'VERSION_FLAG') # Check if running in a V2 environment

        self.sites = SitesHelper()
        self.siteoper = SiteOper()
        # Stop existing tasks
        self.stop_service()

        # Configuration
        if config:
            self._enabled = config.get("enabled", False)
            self._onlyonce = config.get("onlyonce", False)
            self._cron = config.get("cron", "5 1 * * *")
            self._notify = config.get("notify", True)
            self._queue_cnt = config.get("queue_cnt", 5)
            self._history_days = config.get("history_days") or 30
            raw_unread_sites = config.get("unread_sites") or []
            # Ensure _unread_sites contains strings if site IDs are strings
            self._unread_sites = [str(s_id) for s_id in raw_unread_sites]


            # Filter out deleted sites
            all_sites_config = [site for site in self.sites.get_indexers() if not site.get("public")] + self.__custom_sites()
            
            # Ensure site IDs are consistently handled (e.g., as strings)
            valid_site_ids = {str(site.get("id")) for site in all_sites_config if site.get("id") is not None}
            
            self._unread_sites = [site_id for site_id in self._unread_sites if site_id in valid_site_ids and 
                                  any(str(s_conf.get("id")) == site_id and not s_conf.get("public") for s_conf in all_sites_config)]
            self.__update_config()

        if self._enabled or self._onlyonce:
            # Load modules
            # Assuming ModuleHelper.load is compatible or has a V2 equivalent for this use-case
            # For V2, one might use ModuleManager if it provides a similar loading mechanism.
            # For now, keeping ModuleHelper.load as per V1 structure for non-service modules.
            loaded_modules = ModuleHelper.load('app.plugins.sitestatistic.siteuserinfo',
                                               filter_func=lambda _, obj: hasattr(obj, 'schema'))
            if loaded_modules:
                self._site_schema = loaded_modules
                self._site_schema.sort(key=lambda x: x.order)
            else:
                self._site_schema = []
                logger.warning(f"{self.plugin_name}: No site user info schemas found. Plugin might not function correctly.")


            # Scheduler service
            self._scheduler = BackgroundScheduler(timezone=settings.TZ)

            # Run once immediately
            if self._onlyonce:
                logger.info(f"{self.plugin_name} service started, running once immediately.")
                self._scheduler.add_job(self.refresh_all_site_unread_msg, 'date',
                                        run_date=datetime.now(
                                            tz=pytz.timezone(settings.TZ)) + timedelta(seconds=3),
                                        name=self.plugin_name)
                # Turn off the one-time switch
                self._onlyonce = False
                self.__update_config() # Save config

            # Periodic run
            if self._cron:
                try:
                    self._scheduler.add_job(func=self.refresh_all_site_unread_msg,
                                            trigger=CronTrigger.from_crontab(self._cron),
                                            name=self.plugin_name)
                except Exception as err:
                    logger.error(f"Error configuring scheduled task: {err}")
                    # For V2, use the NoticeMessage event for system messages if appropriate
                    # This was self.systemmessage.put in V1.
                    # If it's an error for the admin, a SystemError event might be better
                    # or a NoticeMessage to a specific admin channel if configured.
                    eventmanager.send_event(
                        EventType.SystemError, # Or NoticeMessage
                        {
                           "title": f"{self.plugin_name} Configuration Error",
                           "text": f"Error in cron expression '{self._cron}': {err}",
                           # Add other NoticeMessage fields if using that event
                        }
                    )


            # Start tasks
            if self._scheduler.get_jobs():
                self._scheduler.print_jobs()
                self._scheduler.start()

    def get_state(self) -> bool:
        return self._enabled

    @staticmethod
    def get_command() -> List[Dict[str, Any]]:
        pass

    def get_api(self) -> List[Dict[str, Any]]:
        pass

    def get_form(self) -> Tuple[List[dict], Dict[str, Any]]:
        customSites = self.__custom_sites()
        site_options = ([{"title": site.name, "value": str(site.id)} # Ensure value is string for consistency
                         for site in self.siteoper.list_order_by_pri()]
                        + [{"title": site.get("name"), "value": str(site.get("id"))} # Ensure value is string
                           for site in customSites if site.get("id") is not None])

        return [
            {
                'component': 'VForm',
                'content': [
                    {
                        'component': 'VRow',
                        'content': [
                            {
                                'component': 'VCol', 'props': {'cols': 12, 'md': 4},
                                'content': [{'component': 'VSwitch', 'props': {'model': 'enabled', 'label': '启用插件'}} ]
                            },
                            {
                                'component': 'VCol', 'props': {'cols': 12, 'md': 4},
                                'content': [{'component': 'VSwitch', 'props': {'model': 'notify', 'label': '发送通知'}} ]
                            },
                            {
                                'component': 'VCol', 'props': {'cols': 12, 'md': 4},
                                'content': [{'component': 'VSwitch', 'props': {'model': 'onlyonce', 'label': '立即运行一次'}} ]
                            }
                        ]
                    },
                    {
                        'component': 'VRow',
                        'content': [
                            {
                                'component': 'VCol', 'props': {'cols': 12, 'md': 4},
                                'content': [{'component': 'VTextField', 'props': {'model': 'cron', 'label': '执行周期', 'placeholder': '5位cron表达式，留空不执行定时任务'}}]
                            },
                            {
                                'component': 'VCol', 'props': {'cols': 12, 'md': 4},
                                'content': [{'component': 'VTextField', 'props': {'model': 'queue_cnt', 'label': '队列数量', 'type': 'number'}}]
                            },
                            {
                                'component': 'VCol', 'props': {'cols': 12, 'md': 4},
                                'content': [{'component': 'VTextField', 'props': {'model': 'history_days', 'label': '保留历史天数', 'type': 'number'}}]
                            }
                        ]
                    },
                    {
                        'component': 'VRow',
                        'content': [
                            {
                                'component': 'VCol',
                                'content': [{'component': 'VSelect', 'props': {'chips': True, 'multiple': True, 'model': 'unread_sites', 'label': '未读消息站点', 'items': site_options}}]
                            }
                        ]
                    },
                    {
                        'component': 'VRow',
                        'content': [
                            {
                                'component': 'VCol', 'props': {'cols': 12},
                                'content': [{'component': 'VAlert', 'props': {'type': 'info', 'variant': 'tonal', 'text': '依赖于[站点数据统计]插件的解析逻辑，解析邮件失败请去[站点数据统计]插件仓库提交issue。'}}]
                            }
                        ]
                    }
                ]
            }
        ], {
            "enabled": False,
            "onlyonce": False,
            "notify": True,
            "cron": "5 1 * * *",
            "queue_cnt": 5,
            "history_days": 30,
            "unread_sites": []
        }

    def get_page(self) -> List[dict]:
        unread_data = self.get_data("history")
        if not unread_data:
            return [{'component': 'div', 'text': '暂无数据', 'props': {'class': 'text-center'}}]

        unread_data = sorted(unread_data, key=lambda item: item.get('time') or "", reverse=True)

        unread_msgs = [
            {
                'component': 'tr', 'props': {'class': 'text-sm'},
                'content': [
                    {'component': 'td', 'props': {'class': 'whitespace-nowrap break-keep text-high-emphasis'}, 'text': data.get("site")},
                    {'component': 'td', 'text': data.get("head")},
                    {'component': 'td', 'text': data.get("content")},
                    {'component': 'td', 'text': data.get("time")}
                ]
            } for data in unread_data
        ]

        return [
            {
                'component': 'VRow',
                'content': [
                    {
                        'component': 'VCol', 'props': {'cols': 12},
                        'content': [
                            {
                                'component': 'VTable', 'props': {'hover': True},
                                'content': [
                                    {'component': 'thead', 'content': [
                                        {'component': 'th', 'props': {'class': 'text-start ps-4'}, 'text': '站点'},
                                        {'component': 'th', 'props': {'class': 'text-start ps-4'}, 'text': '标题'},
                                        {'component': 'th', 'props': {'class': 'text-start ps-4'}, 'text': '内容'},
                                        {'component': 'th', 'props': {'class': 'text-start ps-4'}, 'text': '时间'},
                                    ]},
                                    {'component': 'tbody', 'content': unread_msgs}
                                ]
                            }
                        ]
                    }
                ]
            }
        ]

    def stop_service(self):
        try:
            if self._scheduler:
                self._scheduler.remove_all_jobs()
                if self._scheduler.running:
                    self._scheduler.shutdown()
                self._scheduler = None
        except Exception as e:
            logger.error(f"Error stopping plugin service: {str(e)}")

    def __build_class(self, html_text: str) -> Optional[Type[ISiteUserInfo]]:
        if not self._site_schema: # Add check
            logger.warning(f"{self.plugin_name}: No site schemas loaded, cannot build class.")
            return None
        for site_schema_cls in self._site_schema:
            try:
                if site_schema_cls.match(html_text):
                    return site_schema_cls
            except Exception as e:
                logger.error(f"Error matching site schema {getattr(site_schema_cls, '__name__', 'UnknownSchema')}: {e}")
        return None

    def build(self, site_info: CommentedMap) -> Optional[ISiteUserInfo]:
        site_cookie = site_info.get("cookie")
        if not site_cookie:
            return None
        site_name = site_info.get("name")
        apikey = site_info.get("apikey")
        token = site_info.get("token")
        url = site_info.get("url")
        proxy_enabled = site_info.get("proxy", False) # Default to False if not set
        ua = site_info.get("ua")
        
        session = requests.Session()
        proxies = settings.PROXY if proxy_enabled else None
        proxy_server = settings.PROXY_SERVER if proxy_enabled else None
        render = site_info.get("render")

        logger.debug(f"Site {site_name} url={url} cookie_present={bool(site_cookie)} ua_present={bool(ua)}")
        
        html_text = None
        try:
            if render:
                html_text = PlaywrightHelper().get_page_source(url=url,
                                                               cookies=site_cookie,
                                                               ua=ua,
                                                               proxies=proxy_server)
            else:
                res = RequestUtils(cookies=site_cookie,
                                   session=session,
                                   ua=ua,
                                   proxies=proxies).get_res(url=url)
                if res and res.status_code == 200:
                    res.encoding = "utf-8" if re.search(r"charset=\"?utf-8\"?", res.text, re.IGNORECASE) else res.apparent_encoding
                    html_text = res.text
                    # Anti-bot detection for first login
                    if "<title>" not in html_text.lower() and "window.location" in html_text:
                        i = html_text.find("window.location")
                        if i != -1: # Check if "window.location" was actually found
                            location_part = html_text[i:html_text.find(";", i)] # Search for ; after i
                            tmp_url_path = location_part.split('=')[-1].strip().replace("\"", "").replace("+", "").replace(" ", "")
                            if not tmp_url_path.startswith(('http:', 'https:')): # Handle relative paths
                                from urllib.parse import urljoin
                                tmp_url = urljoin(url, tmp_url_path)
                            else:
                                tmp_url = tmp_url_path

                            res = RequestUtils(cookies=site_cookie, session=session, ua=ua, proxies=proxies).get_res(url=tmp_url)
                            if res and res.status_code == 200:
                                res.encoding = "UTF-8" if "charset=utf-8" in res.text.lower() else res.apparent_encoding
                                html_text = res.text
                            elif res:
                                logger.error(f"Site {site_name} anti-bot redirect failed to load: {tmp_url}, Status: {res.status_code}")
                                return None
                            else:
                                logger.error(f"Site {site_name} anti-bot redirect failed to connect: {tmp_url}")
                                return None
                elif res:
                    logger.error(f"Site {site_name} inaccessible: {url}, Status: {res.status_code}")
                    return None
                else:
                    logger.error(f"Site {site_name} connection failed: {url}")
                    return None
            
            if not html_text:
                 logger.error(f"Site {site_name} resulted in empty HTML content from {url}")
                 return None

            # Compatibility for fake homepages
            if '"search"' not in html_text and '"csrf-token"' not in html_text:
                index_php_url = url.rstrip('/') + "/index.php"
                res = RequestUtils(cookies=site_cookie, session=session, ua=ua, proxies=proxies).get_res(url=index_php_url)
                if res and res.status_code == 200:
                    res.encoding = "utf-8" if re.search(r"charset=\"?utf-8\"?", res.text, re.IGNORECASE) else res.apparent_encoding
                    html_text = res.text
                elif res: # if res is not None but status code is not 200
                    logger.debug(f"Site {site_name} attempt to fetch /index.php failed, Status: {res.status_code}. Using original HTML.")
                # If /index.php fails or original HTML is used
            
            if not html_text: # Final check if html_text is still None
                logger.error(f"Site {site_name} failed to retrieve valid HTML content after all attempts for URL: {url}")
                return None

            site_schema_cls = self.__build_class(html_text)
            if not site_schema_cls:
                logger.error(f"Site {site_name} could not identify site type from HTML.")
                return None
            
            return site_schema_cls(
                site_name=site_name, url=url, site_cookie=site_cookie,
                apikey=apikey, token=token, index_html=html_text,
                session=session, ua=ua, proxy=proxy_enabled
            )

        except Exception as e:
            logger.error(f"Error building site info for {site_name} ({url}): {e}", exc_info=True)
            return None
        finally:
            session.close()


    def __refresh_site_data(self, site_info: CommentedMap):
        site_name = site_info.get('name')
        site_url = site_info.get('url')
        if not site_url:
            logger.warning(f"Skipping site data refresh for site with no URL (Name: {site_name or 'Unknown'}).")
            return

        try:
            site_user_info: Optional[ISiteUserInfo] = self.build(site_info=site_info)
            if site_user_info:
                logger.debug(f"Site {site_name} starting parsing with schema {site_user_info.site_schema()}.")
                site_user_info.parse()
                logger.debug(f"Site {site_name} parsing complete.")

                if site_user_info.err_msg and site_user_info.message_unread <= 0:
                    logger.error(f"Site {site_name} parsing failed: {site_user_info.err_msg}, Unread: {site_user_info.message_unread}")
                    return

                # Send notification for unread messages
                self.__notify_unread_msg(site_name, site_user_info)
            else:
                 logger.warning(f"Could not build site user info for {site_name}, skipping.")
        except Exception as e:
            logger.error(f"Site {site_name} failed to get traffic data: {str(e)}", exc_info=True)

    def __notify_unread_msg(self, site_name: str, site_user_info: ISiteUserInfo):
        if not self._notify: # Check if notifications are globally enabled for this plugin
            return

        if site_user_info.message_unread <= 0:
            logger.debug(f"Site {site_name} has no new messages.")
            return

        if site_user_info.message_unread_contents:
            logger.debug(f"Processing {len(site_user_info.message_unread_contents)} unread messages for site {site_name}.")
            for head, date_str, content in site_user_info.message_unread_contents:
                msg_title = f"【站点 {site_user_info.site_name} 消息】"
                msg_text = f"时间：{date_str}\n标题：{head}\n内容：\n{content}"
                
                # Prevent duplicate notifications for the same message
                # Using a simplified key. Consider a more robust hash if content can be very long.
                key_content_part = content[:100] # Use a part of content for key to avoid very long keys
                key = f"{site_user_info.site_name}_{date_str}_{head}_{key_content_part}"

                if key not in self._exits_key:
                    self._exits_key.append(key)
                    # V2 Notification
                    eventmanager.send_event(
                        EventType.NoticeMessage,
                        {
                            "channel": None, # Or MessageChannel.All or specific channel if configured
                            "type": NotificationType.SiteMessage,
                            "title": msg_title,
                            "text": msg_text,
                            "image": None, 
                            "userid": None 
                        }
                    )
                    self._history.append({
                        "site": site_name,
                        "head": head,
                        "content": content,
                        "time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                        "date": date_str,
                    })
                else:
                    logger.debug(f"Message with key {key} already processed for site {site_name}.")
        else:
            # V2 Notification for generic unread count
            title = f"站点 {site_user_info.site_name} 收到 {site_user_info.message_unread} 条新消息，请登陆查看"
            text = f"请登录站点 {site_user_info.site_name} ({site_user_info.url}) 查看详情。"
            eventmanager.send_event(
                EventType.NoticeMessage,
                {
                    "channel": None,
                    "type": NotificationType.SiteMessage,
                    "title": title,
                    "text": text,
                    "image": None,
                    "userid": None
                }
            )
            # Optionally add to history if desired for generic notifications
            # For now, history is only for specific messages.


    def refresh_all_site_unread_msg(self):
        if not self.sites or not self.sites.get_indexers(): # Add check for self.sites
            logger.info(f"{self.plugin_name}: No sites configured, skipping refresh.")
            return

        logger.info("Starting refresh of unread messages for sites...")

        with lock:
            all_sites_config = [site for site in self.sites.get_indexers() if not site.get("public")] + self.__custom_sites()
            
            refresh_sites_config = []
            if not self._unread_sites: # If no specific sites selected, use all non-public sites
                refresh_sites_config = [s for s in all_sites_config if not s.get("public")]
            else:
                # Ensure IDs are strings for comparison
                selected_site_ids = {str(s_id) for s_id in self._unread_sites}
                refresh_sites_config = [s for s in all_sites_config if str(s.get("id")) in selected_site_ids and not s.get("public")]

            if not refresh_sites_config:
                logger.info(f"{self.plugin_name}: No sites selected or available for unread message check.")
                return

            self._history = self.get_data("history") or []
            # Clear old keys to allow re-notification if messages persist after being "read" by key
            self._exits_key = [
                f"{rec['site']}_{rec['date']}_{rec['head']}_{rec['content'][:100]}" 
                for rec in self._history
            ]


            pool_size = min(len(refresh_sites_config), int(self._queue_cnt or 5))
            if pool_size > 0 :
                with ThreadPool(pool_size) as p:
                    p.map(self.__refresh_site_data, refresh_sites_config)
            else:
                logger.info(f"{self.plugin_name}: No sites to process in thread pool.")


            if self._history:
                cutoff_timestamp = time.time() - (int(self._history_days) * 24 * 60 * 60)
                try:
                    self._history = [
                        record for record in self._history
                        if datetime.strptime(record["time"], '%Y-%m-%d %H:%M:%S').timestamp() >= cutoff_timestamp
                    ]
                except ValueError as ve: # Handle cases where 'time' might be missing or malformed
                    logger.error(f"Error parsing date in history record: {ve}. Record might be skipped.")
                    # Decide on recovery: skip record, or try to fix, or log and keep
                    temp_history = []
                    for record in self._history:
                        try:
                            if datetime.strptime(record["time"], '%Y-%m-%d %H:%M:%S').timestamp() >= cutoff_timestamp:
                                temp_history.append(record)
                        except (ValueError, KeyError): # Catch missing 'time' key too
                             logger.warning(f"Skipping history record due to malformed/missing time: {record.get('site')}, {record.get('head')}")
                    self._history = temp_history


                self.save_data("history", self._history)

            logger.info("Site unread message refresh completed.")

    def __custom_sites(self) -> List[Any]:
        custom_sites_plugin_config = self.get_plugin_config("CustomSites") # Use get_plugin_config
        if custom_sites_plugin_config and custom_sites_plugin_config.get("enabled"):
            return custom_sites_plugin_config.get("sites", [])
        return []

    def __update_config(self):
        self.update_config({
            "enabled": self._enabled,
            "onlyonce": self._onlyonce,
            "cron": self._cron,
            "notify": self._notify,
            "queue_cnt": self._queue_cnt,
            "history_days": self._history_days,
            "unread_sites": self._unread_sites,
        })

    @eventmanager.register(EventType.SiteDeleted)
    def site_deleted(self, event: Event): # Added type hint for event
        site_id_deleted = event.event_data.get("site_id")
        if site_id_deleted is None: # Could be 0, so check for None
            # If site_id is not provided, this might mean all sites or an unknown context.
            # Current plugin logic seems to expect a specific site_id or clears all.
            # For safety, if no site_id, we might not want to clear everything unless intended.
            # However, following original logic:
            logger.info(f"{self.plugin_name}: SiteDeleted event received without specific site_id. Assuming clear all selected.")
            self._unread_sites = []
        else:
            # Ensure site_id_deleted is string for comparison
            site_id_deleted_str = str(site_id_deleted)
            # _unread_sites should already be list of strings
            self._unread_sites = [s_id for s_id in self._unread_sites if s_id != site_id_deleted_str]
            logger.info(f"{self.plugin_name}: Site {site_id_deleted_str} removed from selection due to SiteDeleted event.")

        if not self._unread_sites:
            self._enabled = False # Disable plugin if no sites are selected
            logger.info(f"{self.plugin_name}: No sites remaining in selection, disabling plugin.")
            
        self.__update_config()
