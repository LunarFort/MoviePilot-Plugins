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
from app.core.event import eventmanager
from app.db.site_oper import SiteOper
from app.helper.browser import PlaywrightHelper
from app.helper.module import ModuleHelper
from app.helper.sites import SitesHelper
# V2 Imports
from app.helper.notification import NotificationHelper
from app.helper.service import ServiceConfigHelper # Renamed from ServiceConfigHelper in guide to match common pattern
                                                 # Assuming it's app.helper.service.ServiceConfigHelper
                                                 # Or directly from app.helper.service_config import ServiceConfigHelper
                                                 # Based on guide's example: from app.db.systemconfig_oper import SystemConfigOper

from app.log import logger
from app.plugins import _PluginBase
from app.plugins.sitestatistic.siteuserinfo import ISiteUserInfo # Assuming path is correct for V2
from app.schemas.types import EventType, NotificationType # Assuming path is correct for V2
from app.utils.http import RequestUtils

warnings.filterwarnings("ignore", category=FutureWarning)

lock = Lock()


class SiteUnreadMsg(_PluginBase):
    # 插件名称 (Typically sourced from package.v2.json in V2)
    plugin_name = "站点未读消息"
    # 插件描述 (Typically sourced from package.v2.json in V2)
    plugin_desc = "发送站点未读消息。"
    # 插件图标 (Typically sourced from package.v2.json in V2)
    plugin_icon = "Synomail_A.png"
    # 插件版本 (Typically sourced from package.v2.json in V2)
    plugin_version = "2.0" # Updated
    # 插件作者 (Typically sourced from package.v2.json in V2)
    plugin_author = "test"
    # 作者主页 (Typically sourced from package.v2.json in V2)
    author_url = "https://github.com/LunarFort"
    # 插件配置项ID前缀 (Typically sourced from package.v2.json in V2)
    plugin_config_prefix = "siteunreadmsg2_"
    # 加载顺序
    plugin_order = 1
    # 可使用的用户级别
    auth_level = 2

    # 私有属性
    sites: Optional[SitesHelper] = None
    siteoper: Optional[SiteOper] = None
    _scheduler: Optional[BackgroundScheduler] = None
    _history: list = []
    _exits_key: list = []
    _site_schema: Optional[List[ISiteUserInfo]] = None

    # V2 Service Helpers
    notification_helper: Optional[NotificationHelper] = None
    # service_config_helper: Optional[ServiceConfigHelper] = None # Not directly used in methods yet

    # 配置属性
    _enabled: bool = False
    _onlyonce: bool = False
    _cron: str = ""
    _notify: bool = False # This plugin config 'notify' will gate our notification logic
    _queue_cnt: int = 5
    _history_days: int = 30
    _unread_sites: list = []

    def init_plugin(self, config: dict = None):
        self.sites = SitesHelper()
        self.siteoper = SiteOper()
        # V2 Service Helpers Initialization
        self.notification_helper = NotificationHelper()
        # self.service_config_helper = ServiceConfigHelper() # If needed

        # 停止现有任务
        self.stop_service()

        # 配置
        if config:
            self._enabled = config.get("enabled", False)
            self._onlyonce = config.get("onlyonce", False)
            self._cron = config.get("cron", "")
            self._notify = config.get("notify", True) # Default to True if using notifications
            self._queue_cnt = config.get("queue_cnt", 5)
            self._history_days = config.get("history_days") or 30
            self._unread_sites = config.get("unread_sites") or []

            # 过滤掉已删除的站点
            all_sites = [site for site in self.sites.get_indexers() if not site.get("public")] + self.__custom_sites()
            self._unread_sites = [site.get("id") for site in all_sites if
                                  not site.get("public") and site.get("id") in self._unread_sites]
            self.__update_config() # Save potentially cleaned _unread_sites

        if self._enabled or self._onlyonce:
            # 加载模块
            # Assuming ModuleHelper.load is still the way for these types of schemas in V2
            # and ISiteUserInfo is correctly located.
            # The V2 guide's ModuleManager seems for core services (Downloaders, etc.)
            try:
                self._site_schema = ModuleHelper.load('app.plugins.sitestatistic.siteuserinfo',
                                                      filter_func=lambda _, obj: hasattr(obj, 'schema'))
                if not self._site_schema:
                    logger.warning("站点未读消息：未能加载任何站点用户信息解析模块 (ISiteUserInfo)。")
                    # Optionally disable the plugin or prevent scheduler start if this is critical
                    # self._enabled = False 
            except Exception as e:
                logger.error(f"站点未读消息：加载站点用户信息解析模块失败: {e}")
                # Optionally disable
                # self._enabled = False
                return # Stop further initialization if schemas can't be loaded

            if not self._site_schema: # Double check after loading
                 logger.error("站点未读消息：站点用户信息解析模块列表为空，插件功能可能受限。")
                 # return # Or allow to run with limited/no parsing capability

            # 定时服务
            self._scheduler = BackgroundScheduler(timezone=settings.TZ)
            if self._site_schema: # Only sort if schemas were loaded
                self._site_schema.sort(key=lambda x: x.order)

            # 立即运行一次
            if self._onlyonce:
                logger.info("站点未读消息服务启动，立即运行一次")
                self._scheduler.add_job(self.refresh_all_site_unread_msg, 'date',
                                        run_date=datetime.now(
                                            tz=pytz.timezone(settings.TZ)) + timedelta(seconds=3),
                                        name="站点未读消息")
                # 关闭一次性开关
                self._onlyonce = False
                # 保存配置
                self.__update_config()

            # 周期运行
            if self._cron and self._enabled: # Ensure plugin is enabled for cron
                try:
                    self._scheduler.add_job(func=self.refresh_all_site_unread_msg,
                                            trigger=CronTrigger.from_crontab(self._cron),
                                            name="站点未读消息")
                except Exception as err:
                    logger.error(f"定时任务配置错误：{err}")
                    # V1: self.systemmessage.put(f"执行周期配置错误：{err}")
                    # V2: Use proper notification if self.systemmessage is not available/deprecated
                    # For now, logging is the primary feedback.
                    # A direct notification here might be too noisy for a config error.
                    # Admin notification could be considered.
                    if hasattr(self, 'post_system_message'): # Check if a V2 system message method exists
                        self.post_system_message(f"【站点未读消息】执行周期配置错误：{err}")
                    else: # Fallback to standard notification if available and appropriate
                        self._send_notification_v2(
                            title="【站点未读消息】配置错误",
                            text=f"执行周期配置错误：{err}",
                            force_send=True # Send even if global notify is off for errors
                        )


            # 启动任务
            if self._scheduler and self._scheduler.get_jobs():
                self._scheduler.print_jobs()
                self._scheduler.start()
            elif self._enabled: # If enabled but no jobs (e.g. onlyonce was true, now false, and no cron)
                logger.info("站点未读消息：已启用但未配置执行周期或立即运行任务。")

    def _send_notification_v2(self, title: str, text: str, mtype: NotificationType = NotificationType.SiteMessage, force_send: bool = False):
        """
        Generalized V2 notification sending method.
        Uses NotificationHelper and ServiceConfigHelper.
        """
        if not self._notify and not force_send: # Check plugin's own notify toggle
            logger.debug(f"站点未读消息：插件通知功能已关闭，未发送消息 - {title}")
            return

        if not self.notification_helper:
            logger.error("站点未读消息：NotificationHelper 未初始化。")
            return

        # Check system-level notification switch for this message type
        # Assuming ServiceConfigHelper is available.
        # The V2 guide shows ServiceConfigHelper.get_notification_switch,
        # but it's a static method, so we don't strictly need an instance of ServiceConfigHelper.
        try:
            action = ServiceConfigHelper.get_notification_switch(mtype)
        except Exception as e:
            logger.error(f"站点未读消息：无法获取消息通知开关 ({mtype.value}): {e}. 默认允许发送。")
            action = "all" # Fallback to attempt sending if switch check fails

        if not action:
            logger.info(f"站点未读消息：系统级消息通知 ({mtype.value}) 未启用或未配置动作。")
            return

        notifiers_to_use_instances = []
        service_infos = {}

        if action == 'all':
            service_infos = self.notification_helper.get_services()
            for service_info in service_infos.values():
                if service_info.instance and hasattr(service_info.instance, 'send'):
                    notifiers_to_use_instances.append(service_info.instance)
        elif isinstance(action, list) or isinstance(action, str):
            # If action is a comma-separated string, convert to list
            action_list = action.split(',') if isinstance(action, str) else action
            for notifier_name in action_list:
                notifier_name = notifier_name.strip()
                if not notifier_name:
                    continue
                service_info = self.notification_helper.get_service(name=notifier_name)
                if service_info and service_info.instance and hasattr(service_info.instance, 'send'):
                    notifiers_to_use_instances.append(service_info.instance)
                elif service_info:
                     logger.warning(f"站点未读消息: 找到通知服务 {notifier_name} 但其实例无法发送消息或未正确加载。")
                else:
                    logger.warning(f"站点未读消息: 未找到名为 {notifier_name} 的通知服务。")


        if not notifiers_to_use_instances:
            logger.warning(f"站点未读消息：未找到可用的通知服务实例来发送类型 {mtype.value} 的消息。")
            # V1 self.systemmessage.put(...) might be relevant here for admin feedback
            return

        for notifier_instance in notifiers_to_use_instances:
            try:
                # The actual send method might vary based on the notifier's interface.
                # Assuming a common 'send' method signature: send(title, message, image_url=None, **kwargs)
                # Or more simply send(title, message)
                # Or some plugins might use message_to(..., title=title, body=text)
                # We'll try a generic one. If MoviePilot has a unified notifier call, that's better.
                # If _PluginBase.post_message is V2-aware, it's simpler:
                # super().post_message(mtype=mtype, title=title, text=text)
                # For direct use as per guide, we call instance.send:
                notifier_instance.send(title=title, message=text) # Adjust 'message' to 'text' or 'body' if needed
                logger.debug(f"站点未读消息：通过 {type(notifier_instance).__name__} 发送消息 - {title}")
            except Exception as e:
                logger.error(f"站点未读消息：通过 {type(notifier_instance).__name__} 发送消息失败 ({title}): {e}")

    def get_state(self) -> bool:
        return self._enabled

    @staticmethod
    def get_command() -> List[Dict[str, Any]]:
        pass

    def get_api(self) -> List[Dict[str, Any]]:
        pass

    def get_form(self) -> Tuple[List[dict], Dict[str, Any]]:
        customSites = self.__custom_sites()
        site_options = ([{"title": site.name, "value": site.id}
                         for site in self.siteoper.list_order_by_pri()]
                        + [{"title": site.get("name"), "value": site.get("id")}
                           for site in customSites])
        return [
            {
                'component': 'VForm',
                'content': [
                    {
                        'component': 'VRow',
                        'content': [
                            {
                                'component': 'VCol', 'props': {'cols': 12, 'md': 4},
                                'content': [{'component': 'VSwitch', 'props': {'model': 'enabled', 'label': '启用插件'}}],
                            },
                            {
                                'component': 'VCol', 'props': {'cols': 12, 'md': 4},
                                'content': [{'component': 'VSwitch', 'props': {'model': 'notify', 'label': '发送通知'}}],
                            },
                            {
                                'component': 'VCol', 'props': {'cols': 12, 'md': 4},
                                'content': [{'component': 'VSwitch', 'props': {'model': 'onlyonce', 'label': '立即运行一次'}}],
                            },
                        ],
                    },
                    {
                        'component': 'VRow',
                        'content': [
                            {
                                'component': 'VCol', 'props': {'cols': 12, 'md': 4},
                                'content': [{'component': 'VTextField', 'props': {'model': 'cron', 'label': '执行周期', 'placeholder': '5位cron表达式，留空不执行周期任务'}}],
                            },
                            {
                                'component': 'VCol', 'props': {'cols': 12, 'md': 4},
                                'content': [{'component': 'VTextField', 'props': {'model': 'queue_cnt', 'label': '队列数量', 'type': 'number'}}],
                            },
                            {
                                'component': 'VCol', 'props': {'cols': 12, 'md': 4},
                                'content': [{'component': 'VTextField', 'props': {'model': 'history_days', 'label': '保留历史天数', 'type': 'number'}}],
                            },
                        ],
                    },
                    {
                        'component': 'VRow',
                        'content': [
                            {
                                'component': 'VCol',
                                'content': [{'component': 'VSelect', 'props': {'chips': True, 'multiple': True, 'model': 'unread_sites', 'label': '未读消息站点', 'items': site_options}}],
                            },
                        ],
                    },
                    {
                        'component': 'VRow',
                        'content': [
                            {
                                'component': 'VCol', 'props': {'cols': 12},
                                'content': [{'component': 'VAlert', 'props': {'type': 'info', 'variant': 'tonal', 'text': '依赖于[站点数据统计]插件，解析邮件失败请去[站点数据统计]插件仓库提交issue。'}}],
                            },
                        ],
                    },
                ],
            }
        ], {
            "enabled": self._enabled, # Use current values from self
            "onlyonce": self._onlyonce,
            "notify": self._notify,
            "cron": self._cron,
            "queue_cnt": self._queue_cnt,
            "history_days": self._history_days,
            "unread_sites": self._unread_sites
        }

    def get_page(self) -> List[dict]:
        unread_data = self.get_data("history")
        if not unread_data:
            return [{'component': 'div', 'text': '暂无数据', 'props': {'class': 'text-center'}}]

        unread_data = sorted(unread_data, key=lambda item: item.get('time') or '', reverse=True)
        
        unread_msgs_content = []
        for data in unread_data:
            # Ensure all keys exist to prevent errors, provide defaults
            site_name = data.get("site", "N/A")
            head_text = data.get("head", "N/A")
            content_text = data.get("content", "N/A")
            time_text = data.get("time", "N/A")

            row_content = [
                {'component': 'td', 'props': {'class': 'whitespace-nowrap break-keep text-high-emphasis'}, 'text': site_name},
                {'component': 'td', 'text': head_text},
                {'component': 'td', 'text': content_text},
                {'component': 'td', 'text': time_text}
            ]
            unread_msgs_content.append({'component': 'tr', 'props': {'class': 'text-sm'}, 'content': row_content})


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
                                        {'component': 'tr', 'content': [ # Added <tr> for headers
                                            {'component': 'th', 'props': {'class': 'text-left ps-4'}, 'text': '站点'},
                                            {'component': 'th', 'props': {'class': 'text-left ps-4'}, 'text': '标题'},
                                            {'component': 'th', 'props': {'class': 'text-left ps-4'}, 'text': '内容'},
                                            {'component': 'th', 'props': {'class': 'text-left ps-4'}, 'text': '时间'},
                                        ]}
                                    ]},
                                    {'component': 'tbody', 'content': unread_msgs_content}
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
                    self._scheduler.shutdown(wait=False) # V2 might prefer wait=False
                self._scheduler = None
        except Exception as e:
            logger.error(f"{self.plugin_name}: 退出插件失败：{str(e)}")


    def __build_class(self, html_text: str) -> Any:
        if not self._site_schema: # Guard against uninitialized or empty _site_schema
            logger.warning(f"{self.plugin_name}: _site_schema is not available for __build_class.")
            return None
        for site_schema_cls in self._site_schema:
            try:
                # site_schema_cls is the class itself, not an instance yet
                # The match method should ideally be a staticmethod or classmethod on ISiteUserInfo implementers
                # or the class itself needs to be instantiated first if match is an instance method.
                # Assuming ISiteUserInfo.match is designed to be called on the class or is static.
                if site_schema_cls.match(html_text): # Or site_schema_cls().match(html_text) if match is instance method
                    return site_schema_cls 
            except Exception as e:
                logger.error(f"{self.plugin_name}: 站点匹配失败 for schema {site_schema_cls.__name__ if site_schema_cls else 'UnknownSchema'} - {e}")
        return None

    def build(self, site_info: CommentedMap) -> Optional[ISiteUserInfo]:
        site_cookie = site_info.get("cookie")
        if not site_cookie:
            logger.debug(f"{self.plugin_name}: 站点 {site_info.get('name')} 无 Cookie，跳过。")
            return None
            
        site_name = site_info.get("name")
        apikey = site_info.get("apikey")
        token = site_info.get("token")
        url = site_info.get("url")
        proxy_enabled = site_info.get("proxy", False) # Ensure boolean
        ua = site_info.get("ua")
        html_text = None # Initialize html_text

        # V2 Note: settings.PROXY and settings.PROXY_SERVER usage might change.
        # For now, assuming they are still valid global proxy settings.
        proxies = settings.PROXY if proxy_enabled and settings.PROXY else None
        proxy_server_playwright = settings.PROXY_SERVER if proxy_enabled and settings.PROXY_SERVER else None
        render = site_info.get("render")

        logger.debug(f"{self.plugin_name}: 准备处理站点 {site_name}, URL: {url}, Render: {render}")

        # Session should be created per request or managed carefully.
        # The original code creates it here, implying it's for a single build operation.
        with requests.Session() as session:
            if render:
                try:
                    html_text = PlaywrightHelper().get_page_source(
                        url=url,
                        cookies=site_cookie,
                        ua=ua,
                        proxies=proxy_server_playwright # Playwright uses proxy_server format
                    )
                except Exception as e:
                    logger.error(f"{self.plugin_name}: Playwright 获取站点 {site_name} 页面失败: {e}")
                    return None
            else:
                try:
                    # RequestUtils handles its own session if one is not passed, or uses the passed one.
                    # Let's ensure RequestUtils is instantiated correctly.
                    # The original passed session, ua, proxies.
                    req_utils = RequestUtils(cookies=site_cookie, session=session, ua=ua, proxies=proxies)
                    res = req_utils.get_res(url=url)
                    
                    if res and res.status_code == 200:
                        # Encoding detection
                        if re.search(r"charset=[\"']?utf-8[\"']?", res.text, re.IGNORECASE):
                            res.encoding = "utf-8"
                        else:
                            res.encoding = res.apparent_encoding
                        html_text = res.text

                        # Anti-bot redirection for first login
                        if "<title>" not in html_text.lower() and "window.location" in html_text:
                            match = re.search(r"window\.location\s*=\s*['\"]([^'\"]+)['\"]", html_text)
                            if match:
                                # Construct absolute URL if relative
                                from urllib.parse import urljoin
                                redirect_path = match.group(1).replace("+", "").replace(" ", "")
                                tmp_url = urljoin(url, redirect_path)
                                logger.debug(f"{self.plugin_name}: 检测到跳转，尝试新URL: {tmp_url} for {site_name}")
                                res = req_utils.get_res(url=tmp_url)
                                if res and res.status_code == 200:
                                    if re.search(r"charset=[\"']?utf-8[\"']?", res.text, re.IGNORECASE):
                                        res.encoding = "utf-8"
                                    else:
                                        res.encoding = res.apparent_encoding
                                    html_text = res.text
                                else:
                                    logger.error(f"{self.plugin_name}: 站点 {site_name} 跳转后请求失败: {tmp_url}, Status: {res.status_code if res else 'No response'}")
                                    html_text = None # Reset html_text
                        
                        # Fallback for sites using /index.php and not having search/csrf on base URL
                        if html_text and ('"search"' not in html_text and '"csrf-token"' not in html_text):
                            logger.debug(f"{self.plugin_name}: 站点 {site_name} 首页可能不完整，尝试 /index.php")
                            from urllib.parse import urljoin
                            index_php_url = urljoin(url, "index.php")
                            res = req_utils.get_res(url=index_php_url)
                            if res and res.status_code == 200:
                                if re.search(r"charset=[\"']?utf-8[\"']?", res.text, re.IGNORECASE):
                                    res.encoding = "utf-8"
                                else:
                                    res.encoding = res.apparent_encoding
                                html_text_index = res.text
                                # Only use index.php if it looks more complete
                                if '"search"' in html_text_index or '"csrf-token"' in html_text_index:
                                    html_text = html_text_index
                                else:
                                    logger.debug(f"{self.plugin_name}: /index.php for {site_name} 也不包含关键标识，使用原始页面。")
                            elif res:
                                logger.warning(f"{self.plugin_name}: 站点 {site_name} 访问 /index.php 失败, Status: {res.status_code}")
                            # else no response, keep original html_text if any

                    elif res:
                        logger.error(f"{self.plugin_name}: 站点 {site_name} 连接失败: {url}, Status: {res.status_code}")
                        return None
                    else:
                        logger.error(f"{self.plugin_name}: 站点 {site_name} 无法访问 (无响应): {url}")
                        return None

                except requests.exceptions.RequestException as e:
                    logger.error(f"{self.plugin_name}: 请求站点 {site_name} 发生错误: {e}")
                    return None
                except Exception as e: # Catch other unexpected errors during request/processing
                    logger.error(f"{self.plugin_name}: 处理站点 {site_name} HTTP请求时发生未知错误: {e}")
                    return None

        if html_text:
            site_schema_cls = self.__build_class(html_text)
            if not site_schema_cls:
                logger.error(f"{self.plugin_name}: 站点 {site_name} ({url}) 无法识别站点类型或无匹配解析器。")
                # You could save the html_text to a debug file here for later analysis
                # with open(f"debug_{site_name.replace('/', '_')}.html", "w", encoding="utf-8") as f:
                # f.write(html_text)
                return None
            try:
                # Pass the active session to the site_schema instance if it needs to make further requests
                return site_schema_cls(
                    site_name=site_name,
                    url=url,
                    site_cookie=site_cookie,
                    apikey=apikey,
                    token=token,
                    index_html=html_text,
                    session=session, # Pass the session from RequestUtils
                    ua=ua,
                    proxy=proxy_enabled # Pass the boolean proxy_enabled status
                )
            except Exception as e:
                logger.error(f"{self.plugin_name}: 实例化站点解析器 {site_schema_cls.__name__ if site_schema_cls else 'Unknown'} 失败 for {site_name}: {e}")
                return None
        else:
            logger.warning(f"{self.plugin_name}: 未能获取站点 {site_name} 的 HTML 内容。")
            return None

    def __refresh_site_data(self, site_info: CommentedMap):
        site_name = site_info.get('name')
        site_url = site_info.get('url')
        if not site_url:
            logger.warning(f"{self.plugin_name}: 站点 {site_name} 无 URL，跳过。")
            return None # Changed from return to return None for clarity

        if not self._site_schema: # Ensure schemas are loaded
            logger.error(f"{self.plugin_name}: 站点用户信息解析模块未加载，无法刷新 {site_name} 数据。")
            return None

        try:
            site_user_info: Optional[ISiteUserInfo] = self.build(site_info=site_info)
            if site_user_info:
                logger.debug(f"{self.plugin_name}: 站点 {site_name} 开始以 {site_user_info.site_schema()} 模型解析")
                site_user_info.parse()
                logger.debug(f"{self.plugin_name}: 站点 {site_name} 解析完成")

                if site_user_info.err_msg and site_user_info.message_unread <= 0:
                    logger.error(f"{self.plugin_name}: 站点 {site_name} 解析失败：{site_user_info.err_msg}, 未读消息: {site_user_info.message_unread}")
                    # Don't return None here if we still want to process notifications for generic messages
                    # return None 
                
                # Send notifications even if specific content parsing failed but unread > 0
                self.__notify_unread_msg(site_name, site_user_info)
            else:
                logger.warning(f"{self.plugin_name}: 构建站点 {site_name} 解析器失败或无内容返回。")

        except Exception as e:
            logger.error(f"{self.plugin_name}: 站点 {site_name} 获取或解析数据失败：{e}", exc_info=True)


    def __notify_unread_msg(self, site_name: str, site_user_info: ISiteUserInfo):
        if not self._notify: # Check plugin's own notify setting first
            return

        if site_user_info.message_unread <= 0:
            logger.debug(f"{self.plugin_name}: 站点 {site_name} 没有新消息")
            return

        if site_user_info.message_unread_contents: # Check if list is not empty
            logger.debug(f"{self.plugin_name}: 开始处理站点 {site_name} 的 {len(site_user_info.message_unread_contents)} 条未读消息内容。")
            for head, date, content in site_user_info.message_unread_contents:
                msg_title = f"【站点 {site_user_info.site_name} 消息】"
                # Truncate content if too long for a notification
                max_content_len = 500 
                truncated_content = content[:max_content_len] + ('...' if len(content) > max_content_len else '')
                msg_text = f"时间：{date}\n标题：{head}\n内容：\n{truncated_content}"
                
                # Prevent duplicate notifications within the current run / short history
                # Key should be unique enough. Consider hashing for shorter key.
                key_elements = [
                    str(site_user_info.site_name),
                    str(date if date else ""), # Handle None date
                    str(head if head else "")    # Handle None head
                ]
                # For content, use a hash or a fixed part to avoid overly long keys
                # and issues with slight content variations if messages are "updated"
                content_key_part = content[:50] if content else "" # First 50 chars of content
                key_elements.append(content_key_part)
                key = "_".join(key_elements)


                if key not in self._exits_key:
                    self._exits_key.append(key)
                    # Use the new V2 notification method
                    self._send_notification_v2(title=msg_title, text=msg_text, mtype=NotificationType.SiteMessage)
                    
                    self._history.append({
                        "site": site_name,
                        "head": head,
                        "content": content, # Store full content in history
                        "time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"), # Use current time for history entry
                        "date": date, # Original message date
                    })
                else:
                    logger.debug(f"{self.plugin_name}: 消息 {key} 已发送过，跳过。")
        else:
            # Generic notification if no specific content but unread count > 0
            msg_title = f"站点 {site_user_info.site_name} 新消息提醒"
            msg_text = f"您有 {site_user_info.message_unread} 条新消息，请登录 {site_user_info.site_name} 查看。"
            # Use a generic key for this type of notification to avoid spamming if count fluctuates
            generic_key = f"{site_user_info.site_name}_generic_unread_{site_user_info.message_unread}"
            if generic_key not in self._exits_key:
                self._exits_key.append(generic_key)
                self._send_notification_v2(title=msg_title, text=msg_text, mtype=NotificationType.SiteMessage)
                self._history.append({
                    "site": site_name,
                    "head": "未读消息提醒",
                    "content": f"{site_user_info.message_unread} 条新消息",
                    "time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    "date": datetime.now().strftime("%Y-%m-%d"), # Approximate date
                })


    def refresh_all_site_unread_msg(self):
        if not self._site_schema:
            logger.error(f"{self.plugin_name}: 站点用户信息解析模块未加载，无法执行刷新。")
            # Optionally send an admin notification about this critical failure
            self._send_notification_v2(
                title=f"【{self.plugin_name}】严重错误",
                text="站点用户信息解析模块未加载，无法执行未读消息刷新。",
                mtype=NotificationType.System, # Or a more appropriate error type
                force_send=True
            )
            return

        if not self.sites or not self.sites.get_indexers():
            logger.info(f"{self.plugin_name}: 未配置任何站点索引器，跳过刷新。")
            return

        logger.info(f"{self.plugin_name}: 开始刷新站点未读消息 ...")

        with lock: # Ensure thread safety for operations on shared lists like _history, _exits_key
            all_sites_raw = self.sites.get_indexers()
            all_sites = [site for site in all_sites_raw if not site.get("public")] + self.__custom_sites()
            
            refresh_sites = []
            if not self._unread_sites: # If _unread_sites is empty, process all non-public + custom sites
                refresh_sites = all_sites
            else:
                # Filter sites based on plugin configuration _unread_sites
                site_id_set = set(self._unread_sites) # For efficient lookup
                refresh_sites = [site for site in all_sites if site.get("id") in site_id_set]

            if not refresh_sites:
                logger.info(f"{self.plugin_name}: 未选择任何站点进行未读消息检查或所选站点不可用。")
                return

            self._history = self.get_data("history") or []
            self._exits_key = [] # Reset for current run to avoid carry-over from previous runs if logic changes

            # Load recent history to populate _exits_key for deduplication across recent runs (optional)
            # This makes _exits_key more robust than just current run.
            # For example, load keys from messages sent in the last hour or so.
            # For simplicity, current code resets _exits_key per run.

            effective_queue_cnt = min(len(refresh_sites), int(self._queue_cnt or 5))
            if effective_queue_cnt <=0: # Should not happen if refresh_sites is populated
                logger.warning(f"{self.plugin_name}: 队列数量计算结果为零或负数 ({self._queue_cnt})，使用单线程。")
                effective_queue_cnt = 1

            logger.info(f"{self.plugin_name}: 将使用 {effective_queue_cnt} 个线程处理 {len(refresh_sites)} 个站点。")

            with ThreadPool(effective_queue_cnt) as p:
                p.map(self.__refresh_site_data, refresh_sites)

            if self._history:
                try:
                    # Ensure history_days is a valid integer
                    history_days_int = int(self._history_days)
                    if history_days_int <= 0:
                         logger.warning(f"{self.plugin_name}: 保留历史天数配置错误 ({self._history_days})，将不清理历史记录。")
                    else:
                        cutoff_timestamp = time.time() - (history_days_int * 24 * 60 * 60)
                        
                        # Filter history: ensure 'time' key exists and is valid format
                        filtered_history = []
                        for record in self._history:
                            record_time_str = record.get("time")
                            if record_time_str:
                                try:
                                    record_dt = datetime.strptime(record_time_str, '%Y-%m-%d %H:%M:%S')
                                    if record_dt.timestamp() >= cutoff_timestamp:
                                        filtered_history.append(record)
                                except ValueError:
                                    logger.warning(f"{self.plugin_name}: 历史记录中发现无效时间格式: {record_time_str} - {record}")
                                    # Optionally keep malformed records or discard
                                    # Keeping them for now if they don't break things, but ideally, they are fixed or discarded.
                                    # If it's old data, it might naturally get filtered out if we assume it's older than cutoff.
                            else: # if no time, maybe keep it or discard it
                                logger.warning(f"{self.plugin_name}: 历史记录中发现无时间戳的记录: {record}")

                        self._history = filtered_history
                        self.save_data("history", self._history)
                except ValueError:
                    logger.error(f"{self.plugin_name}: history_days 配置 '{self._history_days}' 不是有效整数。历史记录未清理。")
                except Exception as e:
                    logger.error(f"{self.plugin_name}: 清理历史记录时发生错误: {e}", exc_info=True)


            logger.info(f"{self.plugin_name}: 站点未读消息刷新完成")

    def __custom_sites(self) -> List[Any]:
        custom_sites_list = []
        try:
            # Assuming get_config from _PluginBase correctly fetches other plugins' configs
            custom_sites_plugin_config = self.get_config("CustomSites") # This implies CustomSites is plugin key/ID
            if custom_sites_plugin_config and custom_sites_plugin_config.get("enabled"):
                sites_data = custom_sites_plugin_config.get("sites")
                if isinstance(sites_data, list):
                    custom_sites_list = sites_data
                else:
                    logger.warning(f"{self.plugin_name}: CustomSites插件中的sites数据不是列表格式。")
        except Exception as e:
            logger.error(f"{self.plugin_name}: 获取自定义站点配置失败: {e}")
        return custom_sites_list


    def __update_config(self):
        # Ensure data types are correct for config, especially for numbers from text fields
        try:
            queue_cnt_val = int(self._queue_cnt)
        except ValueError:
            logger.warning(f"{self.plugin_name}: queue_cnt '{self._queue_cnt}' 不是有效数字，将使用默认值5。")
            queue_cnt_val = 5 # Fallback to a default
        
        try:
            history_days_val = int(self._history_days)
        except ValueError:
            logger.warning(f"{self.plugin_name}: history_days '{self._history_days}' 不是有效数字，将使用默认值30。")
            history_days_val = 30 # Fallback to a default

        self.update_config({
            "enabled": self._enabled,
            "onlyonce": self._onlyonce,
            "cron": self._cron,
            "notify": self._notify,
            "queue_cnt": queue_cnt_val,
            "history_days": history_days_val,
            "unread_sites": self._unread_sites,
        })

    @eventmanager.register(EventType.SiteDeleted)
    def site_deleted(self, event):
        if not event or not event.event_data:
            return
            
        site_id_to_delete = event.event_data.get("site_id")
        # current_config = self.get_config() # This gets this plugin's config
                                        # self._unread_sites should already be loaded

        if self._unread_sites: # Check if it's loaded and not None
            # Ensure site_id_to_delete is comparable (e.g. string or int consistently)
            # self._unread_sites stores IDs, which might be strings or ints from JSON/DB.
            # SiteOper might return int IDs. JSON from forms might be strings.
            # Standardize to string for comparison if site_id_to_delete can be int or str.
            # Or convert elements in self._unread_sites to the type of site_id_to_delete.
            
            if site_id_to_delete is not None: # Deleting a specific site
                try:
                    # Attempt to make types consistent for comparison
                    site_id_to_delete_str = str(site_id_to_delete)
                    self._unread_sites = [s_id for s_id in self._unread_sites if str(s_id) != site_id_to_delete_str]
                except Exception as e:
                    logger.error(f"{self.plugin_name}: 比较站点ID进行删除时出错: {e}. Site ID: {site_id_to_delete}, Current selection: {self._unread_sites}")

            else: # site_id is None, typically means clear all. Check event contract.
                  # Assuming None means clear (as per original code's else block)
                logger.info(f"{self.plugin_name}: site_id_to_delete is None in SiteDeleted event, clearing all unread_sites selections.")
                self._unread_sites = []
            
            # If no sites are selected, and the plugin was enabled, consider behavior.
            # Original code disables the plugin. This might be too aggressive.
            # Maybe just log or let it run (it will find no sites to process).
            if not self._unread_sites and self._enabled:
                logger.info(f"{self.plugin_name}: 所有选中的站点均已删除或被清除，但插件保持启用状态。如需禁用，请手动操作。")
                # self._enabled = False # Original behavior, uncomment if this is desired

            self.__update_config()
            # No need to call self.init_plugin() here unless the scheduler needs full re-init
            # If only config changed, and scheduler is running, it will pick up changes on next run
            # or if init_plugin is designed to re-eval jobs. For now, just updating config.
