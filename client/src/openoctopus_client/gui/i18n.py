"""Chinese/English interface text selected from the system language."""

from __future__ import annotations

from typing import Literal

Catalog = dict[str, str]

_EN: Catalog = {
    "app.name": "OpenOctopus Client",
    "menu.status.none": "Status: Not configured",
    "menu.status.connecting": "Status: Connecting",
    "menu.status.online": "Status: Online ({device})",
    "menu.status.reconnecting": "Status: Reconnecting (attempt {attempt})",
    "menu.status.stopped": "Status: Stopped",
    "menu.status.attention": "Status: Needs attention",
    "menu.open_web": "Open web page",
    "menu.settings": "Connection settings",
    "menu.stop": "Stop client",
    "menu.start": "Start client",
    "menu.autostart": "Launch on login",
    "menu.quit": "Quit",
    "settings.title": "OpenOctopus Client settings",
    "settings.server": "Server address",
    "settings.token": "Device token",
    "settings.token_saved": "Saved (leave blank to keep)",
    "settings.token_placeholder": "openoctopus_dev_...",
    "settings.save": "Save and connect",
    "settings.saving": "Saving…",
    "settings.no_tray": "The system tray is unavailable. Background connections are stopped. "
    "Retry detection later or quit.",
    "settings.retry_tray": "Retry tray detection",
    "settings.close_hint": "Closing this window only hides it; the client keeps running "
    "with the saved configuration.",
    "error.server_invalid": "Server address must be an http(s) origin without path, query, "
    "or credentials.",
    "error.token_required_for_new_server": "Changing the Server address requires entering "
    "the new device token.",
    "error.token_required": "Device token is required.",
    "error.credential_store_unavailable": "The system credential store is unavailable or "
    "locked. Restore it and press Retry; nothing was changed.",
    "error.credential_store_denied": "Access to the system credential store was denied.",
    "error.save_failed": "Could not save the configuration. The current configuration is "
    "unchanged: {detail}",
    "error.stop_failed": "The running client could not be stopped cleanly. Old settings "
    "are kept and the real runtime status is shown.",
    "error.core_crash": "The execution core exited unexpectedly. See details in the "
    "connection settings.",
    "error.auth_rejected": "The Server rejected the device token. Fix the configuration or "
    "press Start to retry.",
    "error.connection_replaced": "Another connection replaced this device. Start the client "
    "manually only if that was you.",
    "error.config_rejected": "The Server rejected the device configuration. Check the "
    "device settings in the web page.",
    "error.startup_config_invalid": "The saved configuration was rejected by the core. "
    "Re-check the Server address and token.",
    "details.none": "No recent error.",
    "details.label": "Details:",
    "details.core_log": "Core diagnostics:",
    "details.server_unreachable": "The Server address is temporarily unreachable; retrying.",
}

_ZH: Catalog = {
    "app.name": "OpenOctopus 客户端",
    "menu.status.none": "状态：未配置",
    "menu.status.connecting": "状态：连接中",
    "menu.status.online": "状态：在线（{device}）",
    "menu.status.reconnecting": "状态：重连中（第 {attempt} 次）",
    "menu.status.stopped": "状态：已停止",
    "menu.status.attention": "状态：需要处理",
    "menu.open_web": "打开网页",
    "menu.settings": "连接设置",
    "menu.stop": "停止客户端",
    "menu.start": "启动客户端",
    "menu.autostart": "登录后自动启动",
    "menu.quit": "退出",
    "settings.title": "OpenOctopus 客户端设置",
    "settings.server": "服务器地址",
    "settings.token": "设备令牌",
    "settings.token_saved": "已保存（留空表示保留）",
    "settings.token_placeholder": "openoctopus_dev_...",
    "settings.save": "保存并连接",
    "settings.saving": "正在保存…",
    "settings.no_tray": "系统托盘不可用，后台连接已停止。可以稍后重新检测，或退出程序。",
    "settings.retry_tray": "重新检测托盘",
    "settings.close_hint": "关闭此窗口只是隐藏窗口；客户端会按已保存的配置继续运行。",
    "error.server_invalid": "服务器地址必须是不带路径、查询或凭据的 http(s) origin。",
    "error.token_required_for_new_server": "更换服务器地址必须重新输入设备令牌。",
    "error.token_required": "需要输入设备令牌。",
    "error.credential_store_unavailable": "系统凭据库不可用或被锁定。请恢复后点击重试；"
    "本次没有做任何修改。",
    "error.credential_store_denied": "系统凭据库拒绝了访问。",
    "error.save_failed": "配置保存失败，当前配置保持不变：{detail}",
    "error.stop_failed": "运行中的客户端未能干净停止。已保留旧配置并显示实际运行状态。",
    "error.core_crash": "执行核心意外退出。详细信息请在连接设置中查看。",
    "error.auth_rejected": "服务器拒绝了设备令牌。请修改配置，或点击“启动客户端”重试。",
    "error.connection_replaced": "该设备已被另一个连接替换。只有确认是自己时才手动启动。",
    "error.config_rejected": "服务器拒绝了设备配置。请在网页中检查设备设置。",
    "error.startup_config_invalid": "执行核心拒绝了保存的配置。请重新检查服务器地址和令牌。",
    "details.none": "最近没有错误。",
    "details.label": "详情：",
    "details.core_log": "核心诊断：",
    "details.server_unreachable": "服务器地址暂时不可达，正在重试。",
}


def detect_catalog() -> Catalog:
    from PySide6.QtCore import QLocale

    if QLocale.system().language() == QLocale.Language.Chinese:
        return _ZH
    return _EN


class Translator:
    def __init__(self, catalog: Catalog | None = None) -> None:
        self._catalog: Catalog = catalog if catalog is not None else _EN

    def tr(self, key: str, **format_args: object) -> str:
        text = self._catalog.get(key, _EN.get(key, key))
        if format_args:
            return text.format(**format_args)
        return text


StateKey = Literal[
    "none",
    "connecting",
    "online",
    "reconnecting",
    "stopped",
    "attention",
]
