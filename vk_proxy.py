"""Прокси для запросов к VK — общая логика для bot.py и downloader.py.

Прокси применяется ТОЛЬКО к запросам в сторону VK: VK API, upload-серверы VK,
скачивание/встраивание VK-видео. TikTok, YouTube, Instagram и Likee прокси
не используют — их код этот модуль не трогает и не импортирует.

Адрес прокси задаётся один раз в .env (VK_PROXY, не меняется на лету).
Включён он или выключен — переключается из админ-панели бота и хранится в БД,
поэтому переживает перезапуск.
"""

import os
import socket
import time
from urllib.parse import urlsplit

import requests

import db

_SETTING_KEY = "vk_proxy_enabled"
_CHECK_URL = "https://api.vk.com/method/utils.getServerTime?v=5.199"
_CONNECT_TIMEOUT = float(os.getenv("VK_CONNECT_TIMEOUT", "4"))


def configured_url() -> str | None:
    """Адрес прокси из .env (VK_PROXY), например socks5h://user:pass@host:1080.

    None, если прокси не задан — тогда включать/проверять нечего.
    """
    return os.getenv("VK_PROXY", "").strip() or None


def is_configured() -> bool:
    return configured_url() is not None


def is_enabled() -> bool:
    """Использовать ли прокси прямо сейчас (переключатель в админ-панели).

    По умолчанию включён, если вообще задан в .env. Если прокси не задан —
    всегда «выключен», переключатель в этом случае ни на что не влияет.
    """
    if not is_configured():
        return False
    return db.get_setting(_SETTING_KEY, "1") != "0"


def set_enabled(enabled: bool) -> None:
    db.set_setting(_SETTING_KEY, "1" if enabled else "0")


def active_url() -> str | None:
    """URL прокси, который нужно реально использовать сейчас — или None (напрямую)."""
    return configured_url() if is_enabled() else None


def requests_proxies() -> dict | None:
    """Словарь для параметра proxies= в requests, или None — без прокси."""
    url = active_url()
    return {"http": url, "https": url} if url else None


# ─── Диагностика (для админ-панели: /proxy) ────────────────────────────────────

def _measure(fn) -> tuple[bool, str]:
    t0 = time.monotonic()
    try:
        fn()
    except Exception as exc:
        return False, str(exc).splitlines()[0][:200]
    return True, f"{(time.monotonic() - t0) * 1000:.0f} мс"


def check_vk_direct() -> tuple[bool, str]:
    """Доступен ли VK напрямую, БЕЗ прокси, с этого сервера."""
    def probe():
        requests.get(_CHECK_URL, timeout=(_CONNECT_TIMEOUT, 8)).raise_for_status()
    return _measure(probe)


def check_proxy_server() -> tuple[bool, str]:
    """Доступен ли сам сервер прокси (TCP-подключение к host:port из VK_PROXY)."""
    url = configured_url()
    if not url:
        return False, "прокси не задан в .env (VK_PROXY)"
    parts = urlsplit(url)
    if not parts.hostname or not parts.port:
        return False, "не удалось разобрать адрес прокси"

    def probe():
        socket.create_connection((parts.hostname, parts.port), timeout=_CONNECT_TIMEOUT).close()
    return _measure(probe)


def check_vk_via_proxy() -> tuple[bool, str]:
    """Доступен ли VK через прокси (независимо от того, включён ли он сейчас)."""
    url = configured_url()
    if not url:
        return False, "прокси не задан в .env (VK_PROXY)"

    def probe():
        requests.get(
            _CHECK_URL, proxies={"http": url, "https": url}, timeout=(_CONNECT_TIMEOUT, 8),
        ).raise_for_status()
    return _measure(probe)
