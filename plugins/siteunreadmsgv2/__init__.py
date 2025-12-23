import re
import time
import warnings
from datetime import datetime, timedelta
from multiprocessing.dummy import Pool as ThreadPool
from threading import Lock
from typing import Optional, Any, List, Dict, Tuple, Type

import pytz
import requests
import urllib3
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from ruamel.yaml import CommentedMap

from app.core.config import settings
from app.core.event import eventmanager, Event
from app.helper.browser import PlaywrightHelper
from app.helper.module import ModuleHelper
from app.log import logger
# === 修改点1：替换 SiteManager 为 SiteOper ===
from app.db.site_oper import SiteOper
from app.plugins import _PluginBase
from app.schemas.types import EventType, NotificationType

# 禁用 requests 的 SSL 警告
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# 尝试导入依赖插件的类型，如果不存在则定义为 Any 以防止报错
try:
    from app.plugins.sitestatistic.siteuserinfo import ISiteUserInfo
except ImportError:
    ISiteUserInfo = Any
    logger.warning("依赖插件 [站点数据统计] 未安装或无法导入，本插件将无法正常工作。")

warnings.filterwarnings("ignore", category=FutureWarning)

lock = Lock()


class SiteUnreadMsgV2(_PluginBase):
    # 插件名称
    plugin_name = "站点未读消息(V2)"
    # 插件描述
    plugin_desc = "轮询站点未读消息并发送通知，适配 MoviePilot V2。"
    # 插件图标
    plugin_icon = "Synomail_A.png"
    # 插件版本
    plugin_version = "2.3"
    # 插件作者
    plugin_author = "test"
    # 作者主页
    author_url = "https://github.com/LunarFort"
    # 插件配置项ID前缀
    plugin_config_prefix = "siteunreadmsg_v2_"
    # 加载顺序
    plugin_order = 1
    # 可使用的用户级别
    auth_level = 2

    # Private attributes
    _scheduler: Optional[BackgroundScheduler] = None
    _history: List[Dict[str, Any]] = []
    _exits_key: List[str] = []
    _site_schema: List[Type[ISiteUserInfo]] = []
    # === 修改点2：属性声明改为 SiteOper ===
    _site_oper: SiteOper = None

    # Configuration attributes
    _enabled: bool = False
    _onlyonce: bool = False
    _cron: str = ""
    _notify: bool = False
    _queue_cnt: int = 5
    _history_days: int = 30
    _unread_sites: List[str] = []

    def init_plugin(self, config: dict = None):
        # === 修改点3：初始化 SiteOper ===
        self._site_oper = SiteOper()
        
        # Stop existing tasks
        self.stop_service()

        # Configuration
        if config:
            self._enabled = config.get("enabled", False)
            self._onlyonce = config.get("onlyonce", False)
            self._cron = config.get("cron", "5 1 * * *")
            self._notify = config.get("notify", True)
            self._queue_cnt = int(config.get("queue_cnt", 5))
            self._history_days = int(config.get("history_days", 30))
            
            raw_unread_sites = config.get("unread_sites") or []
            self._unread_sites = [str(s_id) for s_id in raw_unread_sites]

            # === 修改点4：使用 SiteOper 获取站点 ===
            all_sites = self._site_oper.list()
            custom_sites = self.__custom_sites()
            
            # 合并内置站点和自定义站点ID
            valid_site_ids = {str(site.get("id")) for site in all_sites}
            valid_site_ids.update({str(site.get("id")) for site in custom_sites if site.get("id")})
            
            # 清理配置中无效的站点ID
            self._unread_sites = [
                site_id for site_id in self._unread_sites 
                if site_id in valid_site_ids
            ]
            self.__update_config()

        if self._enabled or self._onlyonce:
            # Load modules from sitestatistic plugin
            try:
                loaded_modules = ModuleHelper.load(
                    'app.plugins.sitestatistic.siteuserinfo',
                    filter_func=lambda _, obj: hasattr(obj, 'schema')
                )
                if loaded_modules:
                    self._site_schema = loaded_modules
                    self._site_schema.sort(key=lambda x: getattr(x, 'order', 99))
                else:
                    self._site_schema = []
                    logger.warning(f"{self.plugin_name}: 未找到站点适配规则，请确保[站点数据统计]插件已安装并启用。")
            except Exception as e:
                logger.error(f"{self.plugin_name}: 加载依赖模块失败: {e}")
                self._site_schema = []

            # Scheduler service
            self._scheduler = BackgroundScheduler(timezone=settings.TZ)

            # Run once immediately
            if self._onlyonce:
                logger.info(f"{self.plugin_name} 服务启动，立即运行一次。")
                self._scheduler.add_job(self.refresh_all_site_unread_msg, 'date',
                                        run_date=datetime.now(
                                            tz=pytz.timezone(settings.TZ)) + timedelta(seconds=3),
                                        name=self.plugin_name)
                # Turn off the one-time switch
                self._onlyonce = False
                self.__update_config()

            # Periodic run
            if self._cron:
                try:
                    self._scheduler.add_job(func=self.refresh_all_site_unread_msg,
                                            trigger=CronTrigger.from_crontab(self._cron),
                                            name=self.plugin_name)
                    logger.info(f"{self.plugin_name} 定时任务已注册: {self._cron}")
                except Exception as err:
                    logger.error(f"配置定时任务失败: {err}")
                    eventmanager.send_event(
                        EventType.SystemError,
                        {
                           "title": f"{self.plugin_name} 配置错误",
                           "text": f"Cron表达式错误 '{self._cron}': {err}",
                        }
                    )

            # Start tasks
            if self._scheduler.get_jobs():
                self._scheduler.start()

    def get_state(self) -> bool:
        return self._enabled

    @staticmethod
    def get_command() -> List[Dict[str, Any]]:
        pass

    def get_api(self) -> List[Dict[str, Any]]:
        pass

    def get_form(self) -> Tuple[List[dict], Dict[str, Any]]:
        # === 修改点5：使用 SiteOper 获取站点列表 ===
        sites = self._site_oper.list()
        custom_sites = self.__custom_sites()
        
        # 构建选项列表
        site_options = []
        # 内置站点
        for site in sites:
            site_options.append({"title": site.get("name"), "value": str(site.get("id"))})
        # 自定义站点
        for site in custom_sites:
            if site.get("id"):
                site_options.append({"title": site.get("name"), "value": str(site.get("id"))})

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
                                'content': [{'component': 'VTextField', 'props': {'model': 'cron', 'label': '执行周期', 'placeholder': '5位cron表达式'}}]
                            },
                            {
                                'component': 'VCol', 'props': {'cols': 12, 'md': 4},
                                'content': [{'component': 'VTextField', 'props': {'model': 'queue_cnt', 'label': '并发数量', 'type': 'number'}}]
                            },
                            {
                                'component': 'VCol', 'props': {'cols': 12, 'md': 4},
                                'content': [{'component': 'VTextField', 'props': {'model': 'history_days', 'label': '历史保留天数', 'type': 'number'}}]
                            }
                        ]
                    },
                    {
                        'component': 'VRow',
                        'content': [
                            {
                                'component': 'VCol',
                                'content': [{'component': 'VSelect', 'props': {'chips': True, 'multiple': True, 'model': 'unread_sites', 'label': '监控站点', 'items': site_options}}]
                            }
                        ]
                    },
                    {
                        'component': 'VRow',
                        'content': [
                            {
                                'component': 'VCol', 'props': {'cols': 12},
                                'content': [{'component': 'VAlert', 'props': {'type': 'info', 'variant': 'tonal', 'text': '本插件强依赖[站点数据统计]插件的解析逻辑，请确保已安装该插件。如解析失败请检查[站点数据统计]插件是否更新。'}}]
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
            return [{'component': 'div', 'text': '暂无未读消息记录', 'props': {'class': 'text-center'}}]

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
            logger.error(f"停止插件服务失败: {str(e)}")

    def __build_class(self, html_text: str) -> Optional[Type[ISiteUserInfo]]:
        if not self._site_schema:
            return None
        for site_schema_cls in self._site_schema:
            try:
                # 兼容不同版本的接口
                if hasattr(site_schema_cls, 'match'):
                    if site_schema_cls.match(html_text):
                        return site_schema_cls
            except Exception as e:
                logger.debug(f"Schema matching error for {getattr(site_schema_cls, '__name__', 'Unknown')}: {e}")
        return None

    def build(self, site_info: CommentedMap) -> Optional[ISiteUserInfo]:
        site_cookie = site_info.get("cookie")
        if not site_cookie:
            return None
        site_name = site_info.get("name")
        apikey = site_info.get("apikey")
        token = site_info.get("token")
        url = site_info.get("url")
        proxy_enabled = site_info.get("proxy", False)
        ua = site_info.get("ua")
        render = site_info.get("render")

        session = requests.Session()
        
        # 手动设置代理
        if proxy_enabled and settings.PROXY:
            session.proxies.update(settings.PROXY)
            logger.debug(f"Site {site_name}: 已启用代理")
        
        # 手动设置 UA
        if ua:
            session.headers.update({"User-Agent": ua})
        else:
            session.headers.update({"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36"})
        
        html_text = None
        try:
            if render:
                try:
                    html_text = PlaywrightHelper().get_page_source(url=url,
                                                                   cookies=site_cookie,
                                                                   ua=ua)
                except Exception as e:
                    logger.error(f"Playwright rendering failed for {site_name}: {e}")
            else:
                try:
                    res = session.get(url, cookies=site_cookie, timeout=30, verify=False)
                    if res and res.status_code == 200:
                        res.encoding = "utf-8" if re.search(r"charset=\"?utf-8\"?", res.text, re.IGNORECASE) else res.apparent_encoding
                        html_text = res.text
                        
                        # 简单的反爬跳转处理
                        if "<title>" not in html_text.lower() and "window.location" in html_text:
                            match = re.search(r'window\.location\s*=\s*["\']([^"\']+)["\']', html_text)
                            if match:
                                tmp_url_path = match.group(1)
                                if not tmp_url_path.startswith(('http:', 'https:')):
                                    from urllib.parse import urljoin
                                    tmp_url = urljoin(url, tmp_url_path)
                                else:
                                    tmp_url = tmp_url_path
                                
                                res = session.get(tmp_url, cookies=site_cookie, timeout=30, verify=False)
                                if res and res.status_code == 200:
                                    res.encoding = "UTF-8" if "charset=utf-8" in res.text.lower() else res.apparent_encoding
                                    html_text = res.text

                    elif res:
                        logger.debug(f"Site {site_name} request failed: {res.status_code}")
                except Exception as req_e:
                    logger.debug(f"Site {site_name} connection error: {req_e}")

            if not html_text:
                 return None

            # 兼容性检查：如果是假首页，尝试获取 index.php
            if '"search"' not in html_text and '"csrf-token"' not in html_text:
                index_php_url = url.rstrip('/') + "/index.php"
                try:
                    res = session.get(index_php_url, cookies=site_cookie, timeout=30, verify=False)
                    if res and res.status_code == 200:
                        html_text = res.text
                except Exception:
                    pass

            if not html_text:
                return None

            site_schema_cls = self.__build_class(html_text)
            if not site_schema_cls:
                logger.debug(f"Site {site_name} schema not found from HTML content.")
                return None
            
            # 初始化 Schema 类
            return site_schema_cls(
                site_name=site_name, url=url, site_cookie=site_cookie,
                apikey=apikey, token=token, index_html=html_text,
                session=session, ua=ua, proxy=proxy_enabled
            )

        except Exception as e:
            logger.error(f"Error building site info for {site_name}: {e}")
            return None
        finally:
            session.close()

    def __refresh_site_data(self, site_info: CommentedMap):
        site_name = site_info.get('name')
        if not site_info.get('url'):
            return

        try:
            site_user_info: Optional[ISiteUserInfo] = self.build(site_info=site_info)
            if site_user_info:
                site_user_info.parse()

                if site_user_info.err_msg and site_user_info.message_unread <= 0:
                    logger.debug(f"Site {site_name} parse warning: {site_user_info.err_msg}")
                    return

                self.__notify_unread_msg(site_name, site_user_info)
            else:
                 logger.debug(f"Could not build site user info for {site_name}")
        except Exception as e:
            logger.error(f"Site {site_name} refresh error: {str(e)}")

    def __notify_unread_msg(self, site_name: str, site_user_info: ISiteUserInfo):
        if not self._notify:
            return

        if site_user_info.message_unread <= 0:
            return

        # 获取具体消息内容 (依赖 Schema 实现)
        if hasattr(site_user_info, 'message_unread_contents') and site_user_info.message_unread_contents:
            logger.info(f"Site {site_name} has {len(site_user_info.message_unread_contents)} unread messages.")
            for head, date_str, content in site_user_info.message_unread_contents:
                msg_title = f"【站点 {site_user_info.site_name} 消息】"
                msg_text = f"时间：{date_str}\n标题：{head}\n内容：\n{content}"
                
                # 生成去重Key
                key_content_part = str(content)[:50] if content else ""
                key = f"{site_user_info.site_name}_{date_str}_{head}_{key_content_part}"

                if key not in self._exits_key:
                    self._exits_key.append(key)
                    # 发送通知
                    eventmanager.send_event(
                        EventType.NoticeMessage,
                        {
                            "type": NotificationType.SiteMessage,
                            "title": msg_title,
                            "text": msg_text,
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
            # 只有数量没有详情的 fallback 通知
            title = f"{site_user_info.site_name} 有 {site_user_info.message_unread} 条未读消息"
            text = f"请登录站点查看详情: {site_user_info.url}"
            # 简单的去重Key，防止短时间内重复发送
            key = f"{site_name}_count_{site_user_info.message_unread}_{datetime.now().strftime('%Y%m%d%H')}"
            
            if key not in self._exits_key:
                self._exits_key.append(key)
                eventmanager.send_event(
                    EventType.NoticeMessage,
                    {
                        "type": NotificationType.SiteMessage,
                        "title": title,
                        "text": text,
                    }
                )

    def refresh_all_site_unread_msg(self):
        if not self._site_schema:
             logger.warning(f"{self.plugin_name}: 缺少依赖插件模块，跳过运行。")
             return

        logger.info(f"{self.plugin_name}: 开始检查未读消息...")

        with lock:
            # === 修改点6：使用 SiteOper 获取站点列表 ===
            all_sites = self._site_oper.list()
            custom_sites = self.__custom_sites()
            
            # 所有的站点配置列表
            all_sites_config = [site for site in all_sites] + custom_sites
            
            refresh_sites_config = []
            if not self._unread_sites:
                # 默认全选
                refresh_sites_config = all_sites_config
            else:
                selected_site_ids = set(self._unread_sites)
                refresh_sites_config = [
                    s for s in all_sites_config 
                    if str(s.get("id")) in selected_site_ids
                ]

            if not refresh_sites_config:
                logger.info(f"{self.plugin_name}: 未配置监控站点。")
                return

            self._history = self.get_data("history") or []
            # 重建 Key 防止重复
            self._exits_key = [
                f"{rec['site']}_{rec.get('date')}_{rec['head']}_{str(rec.get('content'))[:50]}" 
                for rec in self._history
            ]

            pool_size = min(len(refresh_sites_config), self._queue_cnt)
            if pool_size > 0 :
                with ThreadPool(pool_size) as p:
                    p.map(self.__refresh_site_data, refresh_sites_config)

            # 清理历史记录
            if self._history:
                cutoff_timestamp = time.time() - (self._history_days * 24 * 60 * 60)
                new_history = []
                for record in self._history:
                    try:
                        t_str = record.get("time")
                        if t_str and datetime.strptime(t_str, '%Y-%m-%d %H:%M:%S').timestamp() >= cutoff_timestamp:
                            new_history.append(record)
                    except Exception:
                        pass
                
                self._history = new_history
                self.save_data("history", self._history)

            logger.info(f"{self.plugin_name}: 检查完成。")

    def __custom_sites(self) -> List[Any]:
        # 从 CustomSites 插件获取配置
        custom_sites_plugin_config = self.get_plugin_config("CustomSites")
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
    def site_deleted(self, event: Event):
        site_id_deleted = event.event_data.get("site_id")
        if site_id_deleted:
            site_id_str = str(site_id_deleted)
            if site_id_str in self._unread_sites:
                self._unread_sites = [s for s in self._unread_sites if s != site_id_str]
                self.__update_config()
                logger.info(f"{self.plugin_name}: 站点 {site_id_str} 已从监控列表中移除。")
