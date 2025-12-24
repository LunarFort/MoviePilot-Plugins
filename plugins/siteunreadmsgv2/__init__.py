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
from app.helper.sites import SitesHelper
from app.chain.site import SiteChain
from app.schemas import Notification

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
    plugin_version = "3.1"
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
    _sites_helper: SitesHelper = None

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
        self._sites_helper = SitesHelper()

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

            # 验证站点配置
            self._validate_sites()

            self.__update_config()

        if self._enabled or self._onlyonce:
            logger.info(f"{self.plugin_name}: 插件已启用，准备启动服务")

            # Scheduler service
            self._scheduler = BackgroundScheduler(timezone=settings.TZ)

            # Run once immediately
            if self._onlyonce:
                logger.info(f"{self.plugin_name}: 服务启动，立即运行一次。")
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
                    logger.info(f"{self.plugin_name}: 定时任务已注册: {self._cron}")
                except Exception as err:
                    logger.error(f"{self.plugin_name}: 配置定时任务失败: {err}")
                    eventmanager.send_event(
                        EventType.SystemError,
                        {
                           "title": f"{self.plugin_name} 配置错误",
                           "text": f"Cron表达式错误 '{self._cron}': {err}",
                        }
                    )

            if self._scheduler.get_jobs():
                self._scheduler.start()
                logger.info(f"{self.plugin_name}: 定时任务调度器已启动")

    def _validate_sites(self):
        """验证站点配置"""
        if not self._unread_sites:
            return

        # 获取所有可用站点
        all_sites = self._sites_helper.get_indexers()
        valid_site_ids = {str(site.get("id", "")) for site in all_sites if site.get("id")}

        # 清理无效站点
        self._unread_sites = [
            site_id for site_id in self._unread_sites
            if site_id in valid_site_ids
        ]

        if len(self._unread_sites) == 0:
            logger.warning(f"{self.plugin_name}: 配置的站点ID都无效，将检查所有站点")

    def get_state(self) -> bool:
        return self._enabled

    @staticmethod
    def get_command() -> List[Dict[str, Any]]:
        pass

    def get_api(self) -> List[Dict[str, Any]]:
        pass

    def get_form(self) -> Tuple[List[dict], Dict[str, Any]]:
        try:
            # 获取所有可用站点
            all_sites = self._sites_helper.get_indexers()
            site_options = [
                {"title": site.get("name", ""), "value": str(site.get("id", ""))}
                for site in all_sites
                if site.get("id") and site.get("name")
            ]

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
                                    'content': [{'component': 'VSelect', 'props': {'chips': True, 'multiple': True, 'model': 'unread_sites', 'label': '监控站点（不选则监控所有）', 'items': site_options}}]
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
            logger.error(f"{self.plugin_name}: 获取配置页面失败: {str(e)}")
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
            } for data in unread_data[:50]  # 只显示最近50条
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
            logger.error(f"{self.plugin_name}: 停止插件服务失败: {str(e)}")

    def refresh_all_site_unread_msg(self):
        """主任务：刷新所有站点未读消息"""
        logger.info(f"{self.plugin_name}: 开始检查未读消息...")

        try:
            # 获取要检查的站点
            sites = self._get_target_sites()
            if not sites:
                logger.info(f"{self.plugin_name}: 没有可检查的站点")
                return

            logger.info(f"{self.plugin_name}: 将检查 {len(sites)} 个站点")

            # 加载历史数据
            self._history = self.get_data("history") or []
            self._exits_key = [
                f"{rec['site']}_{rec.get('date')}_{rec['head']}_{str(rec.get('content'))[:50]}"
                for rec in self._history
            ]

            # 使用SiteChain刷新数据
            site_chain = SiteChain()
            checked_sites = []
            lock = Lock()

            # 并发处理函数
            def process_site(site):
                site_name = site.get("name", "Unknown")
                try:
                    logger.info(f"[{site_name}] 开始刷新...")
                    userdata = site_chain.refresh_userdata(site)

                    if userdata:
                        with lock:
                            checked_sites.append(site_name)
                        logger.info(f"[{site_name}] 未读消息数: {userdata.message_unread}, 详情数量: {len(userdata.message_unread_contents) if userdata.message_unread_contents else 0}")

                        # 调试：查看消息详情
                        if userdata.message_unread_contents:
                            logger.info(f"[{site_name}] 消息详情: {userdata.message_unread_contents}")

                        # 处理未读消息
                        if userdata.message_unread > 0:
                            self._handle_unread_messages(site_name, userdata)
                    else:
                        logger.info(f"[{site_name}] 刷新失败或无数据")

                except Exception as e:
                    logger.error(f"[{site_name}] 刷新出错: {e}")

            # 使用线程池并发处理
            pool_size = min(len(sites), self._queue_cnt)
            if pool_size > 1:
                logger.info(f"{self.plugin_name}: 使用 {pool_size} 个线程并发处理")
                with ThreadPool(pool_size) as pool:
                    pool.map(process_site, sites)
            else:
                # 单线程处理
                for site in sites:
                    process_site(site)

            # 清理历史记录
            self._cleanup_history()

            logger.info(f"{self.plugin_name}: 检查完成，成功检查 {len(checked_sites)} 个站点: {', '.join(checked_sites)}")

        except Exception as e:
            logger.error(f"{self.plugin_name}: 主任务执行失败: {e}")

    def _get_target_sites(self) -> List[dict]:
        """获取要检查的站点列表"""
        all_sites = self._sites_helper.get_indexers()

        # 过滤启用的站点
        active_sites = [s for s in all_sites if s.get("is_active")]

        if not self._unread_sites:
            return active_sites

        # 按配置过滤
        target_ids = set(self._unread_sites)
        return [s for s in active_sites if str(s.get("id", "")) in target_ids]

    def _handle_unread_messages(self, site_name: str, userdata):
        """处理未读消息"""
        logger.info(f"[{site_name}] 开始处理未读消息，数量: {userdata.message_unread}")

        if not self._notify:
            logger.info(f"[{site_name}] 通知功能未启用，跳过发送")
            return

        # 有详细内容
        if userdata.message_unread_contents and len(userdata.message_unread_contents) > 0:
            logger.info(f"[{site_name}] 检测到 {len(userdata.message_unread_contents)} 条消息内容")
            for head, date_str, content in userdata.message_unread_contents:
                msg_title = f"【站点 {site_name} 消息】"
                msg_text = f"时间：{date_str}\n标题：{head}\n内容：\n{content}"

                key_content_part = str(content)[:50] if content else ""
                key = f"{site_name}_{date_str}_{head}_{key_content_part}"

                logger.info(f"[{site_name}] 消息key: {key}")
                logger.info(f"[{site_name}] 已存在key: {self._exits_key}")

                if key not in self._exits_key:
                    logger.info(f"[{site_name}] 新消息，准备发送通知")
                    self._exits_key.append(key)

                    # 发送通知
                    self.post_message(
                        mtype=NotificationType.SiteMessage,
                        title=msg_title,
                        text=msg_text,
                        link=userdata.url if hasattr(userdata, 'url') else None
                    )
                    logger.info(f"[{site_name}] 通知已发送: {msg_title}")

                    # 保存历史
                    self._history.append({
                        "site": site_name,
                        "head": head,
                        "content": content,
                        "time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                        "date": date_str,
                    })
                else:
                    logger.info(f"[{site_name}] 消息已存在，跳过发送")
        else:
            # 只有数量没有内容
            logger.info(f"[{site_name}] 无消息详情内容，仅数量: {userdata.message_unread}")
            title = f"站点 {site_name} 有 {userdata.message_unread} 条未读消息"
            text = f"请登录站点查看详情"
            key = f"{site_name}_count_{userdata.message_unread}_{datetime.now().strftime('%Y%m%d%H')}"

            if key not in self._exits_key:
                self._exits_key.append(key)
                self.post_message(
                    mtype=NotificationType.SiteMessage,
                    title=title,
                    text=text,
                    link=userdata.url if hasattr(userdata, 'url') else None
                )
                logger.info(f"[{site_name}] 数量通知已发送")
            else:
                logger.info(f"[{site_name}] 数量通知已存在，跳过")

    def _cleanup_history(self):
        """清理历史记录"""
        if not self._history:
            return

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

    @eventmanager.register(EventType.SiteDeleted)
    def site_deleted(self, event: Event):
        """处理站点删除事件"""
        site_id_deleted = event.event_data.get("site_id")
        if site_id_deleted:
            site_id_str = str(site_id_deleted)
            if site_id_str in self._unread_sites:
                self._unread_sites = [s for s in self._unread_sites if s != site_id_str]
                self.__update_config()
                logger.info(f"{self.plugin_name}: 站点 {site_id_str} 已从监控列表中移除。")

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
