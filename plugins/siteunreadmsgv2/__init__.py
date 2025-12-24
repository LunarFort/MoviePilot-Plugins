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
    plugin_version = "3.5"
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
    _site_schemas: List = []

    # Configuration attributes
    _enabled: bool = False
    _onlyonce: bool = False
    _cron: str = ""
    _notify: bool = False
    _queue_cnt: int = 5
    _history_days: int = 30
    _unread_sites: List[str] = []
    _force_check_sites: List[str] = []  # 强制检查的站点ID列表

    def init_plugin(self, config: dict = None):
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

            raw_force_check_sites = config.get("force_check_sites") or []
            self._force_check_sites = [str(s_id) for s_id in raw_force_check_sites]

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
                                    'component': 'VCol',
                                    'content': [{'component': 'VSelect', 'props': {'chips': True, 'multiple': True, 'model': 'force_check_sites', 'label': '强制检查站点（首页不显示消息数的站点）', 'items': site_options}}]
                                }
                            ]
                        },
                        {
                            'component': 'VRow',
                            'content': [
                                {
                                    'component': 'VCol', 'props': {'cols': 12},
                                    'content': [{'component': 'VAlert', 'props': {'type': 'info', 'variant': 'tonal', 'text': '本插件使用MoviePilot内置站点解析逻辑。春天站点会自动使用特殊解析器。强制检查站点会额外请求消息页面，仅在首页不显示消息数时使用。'}}]
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
                "unread_sites": [],
                "force_check_sites": []
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
                logger.warn(f"站点 {site.get('name')} 未找到站点解析器，schema：{site.get('schema')}")
            return None

        try:
            site_name = site.get("name")
            logger.info(f"站点 {site_name} 开始以 {site.get('schema')} 模型解析数据...")
            site_obj.parse()
            logger.debug(f"站点 {site_name} 数据解析完成")

            # 智能策略：如果首页检测到消息数，正常流程已处理
            # 如果首页未检测到消息数，但用户配置了强制检查，则读取消息
            if site_obj.message_unread == 0 and self._notify:
                # 检查是否需要强制读取（基于配置或特定站点）
                if self._should_force_check(site):
                    logger.info(f"[{site_name}] 首页未检测到消息，尝试强制检查消息页面...")
                    site_obj.message_read_force = True
                    site_obj._pase_unread_msgs()

            # 春天站点特殊处理：总是使用自定义解析以确保消息被正确提取
            # 因为春天站点使用layui框架，标准NexusPHP解析器无法正确解析
            if site_name == "春天":
                logger.info(f"[春天] 使用自定义解析器处理消息...")
                # 清除可能由标准解析器产生的错误数据
                site_obj.message_unread_contents.clear()
                site_obj.message_unread = 0
                self._parse_chun_tian_messages(site_obj)

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

    def _parse_chun_tian_messages(self, site_obj: SiteParserBase):
        """自定义解析春天站点的layui框架消息内容"""
        from lxml import etree
        from app.utils.string import StringUtils

        # 获取站点名称用于日志
        site_name = getattr(site_obj, '_site_name', '未知站点')

        try:
            # 获取消息列表页面
            msg_list_html = site_obj._get_page_content(
                url=urljoin(site_obj._base_url, site_obj._user_mail_unread_page),
                params=site_obj._mail_unread_params,
                headers=site_obj._mail_unread_headers
            )

            if not msg_list_html:
                logger.info(f"[{site_name}] 无法获取消息列表页面")
                return

            # === 自定义解析消息链接（针对layui框架）===
            msg_links = []
            html = etree.HTML(msg_list_html)
            if not StringUtils.is_valid_html_element(html):
                logger.info(f"[{site_name}] 消息列表HTML无效")
                return

            # 方法1: 标准NexusPHP格式
            links = html.xpath('//tr[not(./td/img[@alt="Read"])]/td/a[contains(@href, "viewmessage")]/@href')
            if links:
                msg_links.extend(links)
                logger.debug(f"[{site_name}] 方法1找到 {len(links)} 条链接")

            # 方法2: layui框架的卡片式布局
            if not msg_links:
                links = html.xpath('//div[contains(@class, "layui-card")]//a[contains(@href, "viewmessage")]/@href')
                if links:
                    msg_links.extend(links)
                    logger.debug(f"[{site_name}] 方法2找到 {len(links)} 条链接")

            # 方法3: 任意包含viewmessage的链接
            if not msg_links:
                links = html.xpath('//a[contains(@href, "viewmessage")]/@href')
                if links:
                    msg_links.extend(links)
                    logger.debug(f"[{site_name}] 方法3找到 {len(links)} 条链接")

            # 去重
            msg_links = list(set(msg_links))

            if not msg_links:
                logger.info(f"[{site_name}] 未找到消息链接")
                return

            logger.info(f"[{site_name}] 找到 {len(msg_links)} 条消息链接，开始逐个解析...")

            # 逐个解析消息内容
            for msg_link in msg_links:
                msg_url = urljoin(site_obj._base_url, msg_link)
                msg_html = site_obj._get_page_content(
                    url=msg_url,
                    params=site_obj._mail_content_params,
                    headers=site_obj._mail_content_headers
                )

                if not msg_html:
                    logger.info(f"[{site_name}] 无法获取消息内容: {msg_url}")
                    continue

                # 使用自定义XPath解析
                html = etree.HTML(msg_html)
                if not StringUtils.is_valid_html_element(html):
                    logger.info(f"[{site_name}] HTML无效: {msg_url}")
                    continue

                # === 标题解析 ===
                head = None
                head_nodes = html.xpath('//h1/text()')
                if head_nodes:
                    head = head_nodes[0].strip()
                    logger.debug(f"[{site_name}] 标题: {head}")

                # === 时间解析 ===
                date = None

                # 方法1: layui-card-header 中的 span
                date_nodes = html.xpath('//div[@class="layui-card-header"]//span[2]/text()')
                if date_nodes:
                    date = date_nodes[0].strip()

                # 方法2: layui-card-header 中 span 的 title 属性
                if not date:
                    date_nodes = html.xpath('//div[@class="layui-card-header"]//span/@title')
                    if date_nodes:
                        date = date_nodes[0].strip()

                # 方法3: 标准NexusPHP格式
                if not date:
                    date_nodes = html.xpath('//h1/following-sibling::table[.//tr/td[@class="colhead"]]//tr[2]/td[2]//text()')
                    if date_nodes:
                        date = date_nodes[0].strip()

                # 方法4: span title 属性
                if not date:
                    date_nodes = html.xpath('//span[@title]/@title')
                    if date_nodes:
                        date = date_nodes[0].strip()

                # 方法5: 表格第二行
                if not date:
                    date_nodes = html.xpath('//table//tr[2]//td[2]//text()')
                    if date_nodes:
                        date = date_nodes[0].strip()

                if date:
                    date = StringUtils.unify_datetime_str(date)

                # === 内容解析 ===
                content = None

                # 方法1: layui-card-body
                content_nodes = html.xpath('//div[@class="layui-card-body"]//text()')
                if content_nodes:
                    content = ' '.join([n.strip() for n in content_nodes if n.strip()])

                # 方法2: 标准NexusPHP格式
                if not content:
                    content_nodes = html.xpath('//h1/following-sibling::table[.//tr/td[@class="colhead"]]//tr[3]/td//text()')
                    if content_nodes:
                        content = ' '.join([n.strip() for n in content_nodes if n.strip()])

                # 方法3: 表格第三行 [colspan="2"]
                if not content:
                    content_nodes = html.xpath('//table//tr[3]//td[@colspan="2"]//text()')
                    if content_nodes:
                        content = ' '.join([n.strip() for n in content_nodes if n.strip()])

                # 方法4: 包含"感谢"的文本
                if not content:
                    content_nodes = html.xpath('//td[contains(text(), "感谢")]/text()')
                    if content_nodes:
                        content = ' '.join([n.strip() for n in content_nodes if n.strip()])

                # 方法5: 查找所有 td 的文本
                if not content:
                    content_nodes = html.xpath('//td//text()')
                    if content_nodes:
                        content = ' '.join([n.strip() for n in content_nodes if n.strip()])

                logger.debug(f"[{site_name}] 解析结果 - 标题='{head}', 时间='{date}', 内容='{content[:50] if content else None}...'")

                # 添加到结果
                if head or date or content:
                    # 如果某个字段缺失，从上下文补全
                    if not head:
                        head = "无标题"
                    if not date:
                        date = "未知时间"
                    if not content:
                        content = "无内容"
                    site_obj.message_unread_contents.append((head, date, content))

            # 更新未读消息数
            if site_obj.message_unread_contents:
                site_obj.message_unread = len(site_obj.message_unread_contents)

            logger.info(f"[{site_name}] 自定义解析完成: {len(site_obj.message_unread_contents)} 条消息")

        except Exception as e:
            logger.error(f"[{site_name}] 自定义解析出错: {e}")
            import traceback
            logger.error(f"[{site_name}] 详细错误: {traceback.format_exc()}")

    def _should_force_check(self, site: dict) -> bool:
        """判断是否需要强制检查消息页面"""
        site_id = str(site.get("id", ""))
        site_name = site.get("name", "")

        # 策略1：用户配置的强制检查站点（最高优先级）
        if site_id in self._force_check_sites:
            logger.debug(f"[{site_name}] 在强制检查列表中")
            return True

        # 策略2：基于站点名称判断（向后兼容）
        # 这些站点首页不显示消息数，需要强制检查
        force_sites = ["春天"]
        if site_name in force_sites:
            return True

        return False

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
                msg_text = f"时间：{date_str}\\n标题：{head}\\n内容：\\n{content}"

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
            "force_check_sites": self._force_check_sites,
        })
