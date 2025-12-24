import time
import warnings
from datetime import datetime, timedelta
from multiprocessing.dummy import Pool as ThreadPool
from threading import Lock
from typing import Optional, Any, List, Dict, Tuple
from urllib.parse import urljoin

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
from app.schemas import SiteUserData
from app.modules.indexer.parser import SiteParserBase
from app.helper.module import ModuleHelper

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
    plugin_version = "2.0"
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

    # Private attributes - 在 init_plugin 中初始化
    _scheduler: Optional[BackgroundScheduler] = None
    _history: List[Dict[str, Any]] = None
    _exits_key: List[str] = None
    _site_oper: Optional[SiteOper] = None
    _sites_helper: Optional[SitesHelper] = None
    _site_schemas: List = None

    # Configuration attributes
    _enabled: bool = False
    _onlyonce: bool = False
    _cron: str = ""
    _notify: bool = False
    _queue_cnt: int = 5
    _history_days: int = 30
    _unread_sites: List[str] = None

    def init_plugin(self, config: dict = None):
        # 初始化私有属性
        self._history = []
        self._exits_key = []
        self._unread_sites = []
        self._site_schemas = []

        self._site_oper = SiteOper()
        self._sites_helper = SitesHelper()

        # 加载站点解析器
        self._site_schemas = ModuleHelper.load(
            'app.modules.indexer.parser',
            filter_func=lambda _, obj: hasattr(obj, 'schema') and getattr(obj, 'schema') is not None
        )

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
        return []

    def get_api(self) -> List[Dict[str, Any]]:
        return []

    def get_service(self) -> List[Dict[str, Any]]:
        """
        注册公共定时服务
        """
        if not self._enabled or not self._cron:
            return []

        try:
            return [{
                "id": "SiteUnreadMsgV2",
                "name": "站点未读消息检查",
                "trigger": CronTrigger.from_crontab(self._cron),
                "func": self.refresh_all_site_unread_msg,
                "kwargs": {}
            }]
        except Exception as e:
            logger.error(f"{self.plugin_name}: 注册服务失败: {e}")
            return []

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
                                    'content': [{'component': 'VAlert', 'props': {'type': 'info', 'variant': 'tonal', 'text': '本插件已内置春天等特殊站点的支持，无需额外配置。'}}]
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

            # 使用线程池处理
            with ThreadPool(self._queue_cnt) as pool:
                results = pool.map(self._check_site, sites)

            # 统计结果
            checked_sites = [r for r in results if r]

            # 清理历史记录
            self._cleanup_history()

            logger.info(f"{self.plugin_name}: 检查完成，成功检查 {len(checked_sites)} 个站点: {', '.join(checked_sites)}")

        except Exception as e:
            logger.error(f"{self.plugin_name}: 主任务执行失败: {e}")

    def _check_site(self, site: dict) -> Optional[str]:
        """检查单个站点"""
        site_name = site.get("name", "Unknown")
        try:
            logger.info(f"[{site_name}] 开始刷新...")

            # 直接使用解析器，绕过SiteChain的限制
            userdata = self._get_site_userdata(site)

            if not userdata:
                logger.info(f"[{site_name}] 刷新失败或无数据")
                return None

            # 详细诊断 - 所有站点
            logger.info(f"[{site_name}] 未读消息数: {userdata.message_unread}, 详情数量: {len(userdata.message_unread_contents) if userdata.message_unread_contents else 0}")

            # 处理未读消息
            if userdata.message_unread > 0 or (userdata.message_unread_contents and len(userdata.message_unread_contents) > 0):
                self._handle_unread_messages(site_name, userdata)
                return site_name

            return site_name

        except Exception as e:
            logger.error(f"[{site_name}] 刷新出错: {e}")
            return None

    def _get_site_userdata(self, site: dict) -> Optional[SiteUserData]:
        """获取站点用户数据 - 智能解析策略"""
        def __get_site_obj() -> Optional[SiteParserBase]:
            """获取站点解析器"""
            for site_schema in self._site_schemas:
                if site_schema.schema.value == site.get("schema"):
                    return site_schema(
                        site_name=site.get("name"),
                        url=site.get("url"),
                        site_cookie=site.get("cookie"),
                        apikey=site.get("apikey"),
                        token=site.get("token"),
                        ua=site.get("ua"),
                        proxy=site.get("proxy"))
            return None

        site_obj = __get_site_obj()
        if not site_obj:
            if not site.get("public"):
                logger.warning(f"站点 {site.get('name')} 未找到站点解析器，schema：{site.get('schema')}")
            return None

        try:
            site_name = site.get("name")
            site_id = str(site.get("id", ""))
            logger.info(f"站点 {site_name} 开始以 {site.get('schema')} 模型解析数据...")
            site_obj.parse()
            logger.debug(f"站点 {site_name} 数据解析完成")

            # 春天站点特殊处理：消息详情页面结构不同，需要自定义解析
            # 无论标准解析器是否解析到未读数，都使用自定义解析器获取消息内容
            if site_name == "春天":
                logger.info(f"[{site_name}] 使用自定义解析器...")
                # 清空标准解析器可能解析到的不完整内容
                site_obj.message_unread_contents.clear()
                self._parse_ssd_messages(site_obj)

            return SiteUserData(
                domain=site.get("url"),
                userid=site_obj.userid,
                username=site_obj.username,
                user_level=site_obj.user_level,
                join_at=site_obj.join_at,
                upload=site_obj.upload,
                download=site_obj.download,
                ratio=site_obj.ratio,
                bonus=site_obj.bonus,
                seeding=site_obj.seeding,
                seeding_size=site_obj.seeding_size,
                seeding_info=site_obj.seeding_info.copy() if site_obj.seeding_info else [],
                leeching=site_obj.leeching,
                leeching_size=site_obj.leeching_size,
                message_unread=site_obj.message_unread,
                message_unread_contents=site_obj.message_unread_contents.copy() if site_obj.message_unread_contents else [],
                updated_day=datetime.now().strftime('%Y-%m-%d'),
                err_msg=site_obj.err_msg
            )
        finally:
            site_obj.clear()

    def _parse_ssd_messages(self, site_obj: SiteParserBase):
        """
        春天站点特殊处理：
        检测首页是否有"有新短讯"图片，有则获取消息详情
        """
        from lxml import etree
        from app.utils.string import StringUtils

        site_name = getattr(site_obj, '_site_name', '未知站点')

        try:
            # 获取首页
            index_html = site_obj._get_page_content(
                url=site_obj._base_url,
                params=site_obj._user_basic_params,
                headers=site_obj._user_basic_headers
            )

            if not index_html:
                logger.info(f"[{site_name}] 无法获取首页")
                return

            html = etree.HTML(index_html)
            if not StringUtils.is_valid_html_element(html):
                logger.info(f"[{site_name}] 首页HTML无效")
                return

            # 检测是否包含"有新短讯"图片
            new_msg_indicator = html.xpath('//img[contains(@title, "有新短讯")]')
            if not new_msg_indicator:
                logger.debug(f"[{site_name}] 无新短讯")
                return

            logger.info(f"[{site_name}] 检测到'有新短讯'，开始获取消息...")
            self._parse_ssd_unread_msgs(site_obj)
            logger.info(f"[{site_name}] 解析完成: {len(site_obj.message_unread_contents)} 条消息")

        except Exception as e:
            logger.error(f"[{site_name}] 解析出错: {e}")
            import traceback
            logger.error(f"[{site_name}] 详细错误: {traceback.format_exc()}")

    def _parse_ssd_unread_msgs(self, site_obj: SiteParserBase):
        """
        春天站点自定义消息解析
        """
        from lxml import etree
        from app.utils.string import StringUtils
        from urllib.parse import urljoin

        site_name = getattr(site_obj, '_site_name', '未知站点')
        base_url = site_obj._base_url

        # 获取未读消息列表页面
        unread_msg_links = []
        for box_type in ["1", "-2"]:  # 1=收件箱, -2=系统消息
            list_url = f"messages.php?action=viewmailbox&box={box_type}&unread=yes"
            list_html = site_obj._get_page_content(
                url=urljoin(base_url, list_url),
                params=site_obj._mail_unread_params,
                headers=site_obj._mail_unread_headers
            )
            if not list_html:
                continue

            html = etree.HTML(list_html)
            if not StringUtils.is_valid_html_element(html):
                continue

            # 获取未读消息链接 - 排除已读消息（有 Read 图标的）
            msg_links = html.xpath('//tr[not(./td/img[@alt="Read"])]/td/a[contains(@href, "viewmessage")]/@href')
            unread_msg_links.extend(msg_links)
            logger.debug(f"[{site_name}] box={box_type} 找到 {len(msg_links)} 条未读消息链接")

        # 更新未读消息数
        if unread_msg_links:
            site_obj.message_unread = len(unread_msg_links)

        # 解析每条消息的内容
        for msg_link in unread_msg_links:
            msg_url = urljoin(base_url, msg_link)
            logger.debug(f"[{site_name}] 获取消息: {msg_url}")

            msg_html = site_obj._get_page_content(
                url=msg_url,
                params=site_obj._mail_content_params,
                headers=site_obj._mail_content_headers
            )
            if not msg_html:
                continue

            head, date, content = self._parse_ssd_message_content(msg_html, site_name)
            logger.debug(f"[{site_name}] 解析结果 - 标题: {head}, 时间: {date}, 内容: {content[:50] if content else None}...")
            site_obj.message_unread_contents.append((head, date, content))

    def _parse_ssd_message_content(self, html_text: str, site_name: str = "春天"):
        """
        解析春天站点消息详情页面

        春天站点消息结构：
        - 标题: h1 标签
        - 时间: <span title="2025-12-24 13:36:41"> 的 title 属性
        - 内容: <td class="rowfollow" colspan="2">
        """
        from lxml import etree
        from app.utils.string import StringUtils

        html = etree.HTML(html_text)
        try:
            if not StringUtils.is_valid_html_element(html):
                return None, None, None

            message_head_text = None
            message_date_text = None
            message_content_text = None

            # === 解析标题 ===
            # 春天站点标题在 h1 标签
            message_head = html.xpath('//h1/text()')
            if message_head:
                message_head_text = message_head[-1].strip()

            # === 解析时间 ===
            # 春天站点时间在 span 的 title 属性中，格式: <span title="2025-12-24 13:36:41">
            date_span = html.xpath('//td[@class="rowfollow"]//span[@title]/@title')
            if date_span:
                message_date_text = date_span[0].strip()

            # === 解析内容 ===
            # 春天站点内容在 colspan="2" 的 td 中
            content_td = html.xpath('//td[@class="rowfollow" and @colspan="2"]')
            if content_td:
                message_content_text = content_td[0].xpath("string(.)").strip()

            logger.debug(f"[{site_name}] 消息解析 - 标题: {message_head_text}, 时间: {message_date_text}, 内容: {message_content_text[:50] if message_content_text else None}")

            return message_head_text, message_date_text, message_content_text

        except Exception as e:
            logger.error(f"[{site_name}] 解析消息内容出错: {e}")
            return None, None, None
        finally:
            if html is not None:
                del html

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

                    with lock:
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
                    with lock:
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
                with lock:
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
                logger.info(f"{self.plugin_name}: 站点 {site_id_str} 已从配置中移除。")

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
