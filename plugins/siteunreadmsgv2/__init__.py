import time
import warnings
from datetime import datetime, timedelta
from multiprocessing.dummy import Pool as ThreadPool
from threading import Lock
from typing import Optional, Any, List, Dict, Tuple

import pytz
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from ruamel.yaml import CommentedMap

from app.core.config import settings
from app.core.event import eventmanager, Event
from app.log import logger
from app.db.site_oper import SiteOper
from app.plugins import _PluginBase
from app.schemas.types import EventType, NotificationType
from app.modules.indexer.parser import SiteParserBase
from app.helper.module import ModuleHelper
from app.utils.string import StringUtils

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
    plugin_version = "3.0"
    # 插件作者
    plugin_author = "Test"
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
    _site_oper: SiteOper = None
    _site_schemas: List[SiteParserBase] = []

    # Configuration attributes
    _enabled: bool = False
    _onlyonce: bool = False
    _cron: str = ""
    _notify: bool = False
    _queue_cnt: int = 5
    _history_days: int = 30
    _unread_sites: List[str] = []

    def init_plugin(self, config: dict = None):
        self._site_oper = SiteOper()

        # Stop existing tasks
        self.stop_service()

        # Load site schemas
        self._site_schemas = ModuleHelper.load(
            'app.modules.indexer.parser',
            filter_func=lambda _, obj: hasattr(obj, 'schema') and getattr(obj, 'schema') is not None
        )

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

            # 获取所有站点
            all_sites = self._site_oper.list()

            # 使用 getattr 安全获取对象属性
            valid_site_ids = set()
            for site in all_sites:
                # 兼容对象(site.id)和字典(site['id'])
                if isinstance(site, dict):
                    s_id = site.get("id")
                else:
                    s_id = getattr(site, "id", None)

                if s_id:
                    valid_site_ids.add(str(s_id))

            # 清理配置
            self._unread_sites = [
                site_id for site_id in self._unread_sites
                if site_id in valid_site_ids
            ]
            self.__update_config()

        if self._enabled or self._onlyonce:
            # Scheduler service
            self._scheduler = BackgroundScheduler(timezone=settings.TZ)

            # Run once immediately
            if self._onlyonce:
                logger.info(f"{self.plugin_name} 服务启动，立即运行一次。")
                self._scheduler.add_job(self.refresh_all_site_unread_msg, 'date',
                                        run_date=datetime.now(
                                            tz=pytz.timezone(settings.TZ)) + timedelta(seconds=3),
                                        name=self.plugin_name)
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
        try:
            # 安全获取站点列表
            if self._site_oper:
                sites = self._site_oper.list()
            else:
                self._site_oper = SiteOper()
                sites = self._site_oper.list()

            site_options = []
            for site in sites:
                # 使用 getattr 安全获取属性
                # 判断是字典还是对象，分别处理
                if isinstance(site, dict):
                    site_id = str(site.get("id", ""))
                    site_name = site.get("name", "")
                else:
                    site_id = str(getattr(site, "id", ""))
                    site_name = getattr(site, "name", "")

                if site_id and site_name:
                    site_options.append({"title": site_name, "value": site_id})

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
                                    'content': [{'component': 'VAlert', 'props': {'type': 'info', 'variant': 'tonal', 'text': '本插件使用MoviePilot内置站点解析逻辑，无需依赖其他插件。'}}]
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
        except Exception as e:
            logger.error(f"获取配置页面失败: {str(e)}")
            return [
                {
                    'component': 'VAlert',
                    'props': {'type': 'error', 'text': f'加载配置出错: {str(e)}'}
                }
            ], {}

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

    def build(self, site_info: CommentedMap) -> Optional[SiteParserBase]:
        """
        使用MoviePilot内置的站点解析器构建站点用户信息
        """
        site_cookie = site_info.get("cookie")
        if not site_cookie:
            return None

        site_name = site_info.get("name")
        url = site_info.get("url")
        if not url:
            return None

        # 获取站点schema
        schema = site_info.get("schema")
        if not schema:
            logger.debug(f"Site {site_name} has no schema defined")
            return None

        # 查找匹配的解析器
        site_parser = None
        for site_schema in self._site_schemas:
            if site_schema.schema.value == schema:
                site_parser = site_schema
                break

        if not site_parser:
            logger.debug(f"Site {site_name} schema {schema} not found")
            return None

        apikey = site_info.get("apikey")
        token = site_info.get("token")
        proxy_enabled = site_info.get("proxy", False)
        ua = site_info.get("ua")

        try:
            # 使用MoviePilot内置的站点解析器
            site_obj = site_parser(
                site_name=site_name,
                url=url,
                site_cookie=site_cookie,
                apikey=apikey,
                token=token,
                ua=ua,
                proxy=proxy_enabled
            )

            # 执行解析
            site_obj.parse()

            return site_obj

        except Exception as e:
            logger.error(f"Error building site info for {site_name}: {e}")
            return None

    def __refresh_site_data(self, site_info: CommentedMap):
        site_name = site_info.get('name')
        if not site_info.get('url'):
            return

        try:
            site_user_info: Optional[SiteParserBase] = self.build(site_info=site_info)
            if site_user_info:
                if site_user_info.err_msg and site_user_info.message_unread <= 0:
                    logger.debug(f"Site {site_name} parse warning: {site_user_info.err_msg}")
                    return

                self.__notify_unread_msg(site_name, site_user_info)
            else:
                 logger.debug(f"Could not build site user info for {site_name}")
        except Exception as e:
            logger.error(f"Site {site_name} refresh error: {str(e)}")

    def __notify_unread_msg(self, site_name: str, site_user_info: SiteParserBase):
        if not self._notify:
            return

        if site_user_info.message_unread <= 0:
            return

        if hasattr(site_user_info, 'message_unread_contents') and site_user_info.message_unread_contents:
            logger.info(f"Site {site_name} has {len(site_user_info.message_unread_contents)} unread messages.")
            for head, date_str, content in site_user_info.message_unread_contents:
                msg_title = f"【站点 {site_user_info.site_name} 消息】"
                msg_text = f"时间：{date_str}\n标题：{head}\n内容：\n{content}"

                key_content_part = str(content)[:50] if content else ""
                key = f"{site_user_info.site_name}_{date_str}_{head}_{key_content_part}"

                if key not in self._exits_key:
                    self._exits_key.append(key)
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
            title = f"{site_user_info.site_name} 有 {site_user_info.message_unread} 条未读消息"
            text = f"请登录站点查看详情: {site_user_info.url}"
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
        if not self._site_schemas:
            logger.warning(f"{self.plugin_name}: 未加载到站点解析器，跳过运行。")
            return

        logger.info(f"{self.plugin_name}: 开始检查未读消息...")

        with lock:
            all_sites = self._site_oper.list()

            refresh_sites_config = []
            if not self._unread_sites:
                refresh_sites_config = all_sites
            else:
                selected_site_ids = set(self._unread_sites)

                refresh_sites_config = []
                for s in all_sites:
                    if isinstance(s, dict):
                        sid = str(s.get("id", ""))
                    else:
                        sid = str(getattr(s, "id", ""))

                    if sid in selected_site_ids:
                        refresh_sites_config.append(s)

            if not refresh_sites_config:
                logger.info(f"{self.plugin_name}: 未配置监控站点。")
                return

            self._history = self.get_data("history") or []
            self._exits_key = [
                f"{rec['site']}_{rec.get('date')}_{rec['head']}_{str(rec.get('content'))[:50]}"
                for rec in self._history
            ]

            pool_size = min(len(refresh_sites_config), self._queue_cnt)
            if pool_size > 0 :
                with ThreadPool(pool_size) as p:
                    p.map(self.__refresh_site_data, refresh_sites_config)

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
