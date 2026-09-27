"""Тесты модуля vk_proxy: конфигурация, переключатель, диагностика."""

from unittest.mock import MagicMock, patch

import pytest
import requests

import vk_proxy


@pytest.fixture
def tmp_db(tmp_path, monkeypatch):
    import db
    monkeypatch.setattr(db, "DB_PATH", str(tmp_path / "t.db"))
    db.init_db()
    return db


@pytest.fixture
def no_proxy(monkeypatch):
    monkeypatch.delenv("VK_PROXY", raising=False)


@pytest.fixture
def with_proxy(monkeypatch):
    monkeypatch.setenv("VK_PROXY", "socks5h://user:pass@1.2.3.4:1080")


# ─── Конфигурация и переключатель ──────────────────────────────────────────────

def test_not_configured_when_env_missing(no_proxy):
    assert vk_proxy.configured_url() is None
    assert vk_proxy.is_configured() is False
    assert vk_proxy.is_enabled() is False
    assert vk_proxy.active_url() is None
    assert vk_proxy.requests_proxies() is None


def test_enabled_by_default_when_configured(tmp_db, with_proxy):
    assert vk_proxy.is_configured() is True
    assert vk_proxy.is_enabled() is True
    assert vk_proxy.active_url() == "socks5h://user:pass@1.2.3.4:1080"
    assert vk_proxy.requests_proxies() == {
        "http": "socks5h://user:pass@1.2.3.4:1080",
        "https": "socks5h://user:pass@1.2.3.4:1080",
    }


def test_toggle_off_disables_active_url(tmp_db, with_proxy):
    vk_proxy.set_enabled(False)
    assert vk_proxy.is_enabled() is False
    assert vk_proxy.active_url() is None
    assert vk_proxy.requests_proxies() is None
    # Сам факт настройки не меняется — прокси просто временно не используется.
    assert vk_proxy.is_configured() is True


def test_toggle_persists_and_can_be_reenabled(tmp_db, with_proxy):
    vk_proxy.set_enabled(False)
    vk_proxy.set_enabled(True)
    assert vk_proxy.is_enabled() is True
    assert vk_proxy.active_url() == "socks5h://user:pass@1.2.3.4:1080"


def test_toggle_without_proxy_configured_has_no_effect(tmp_db, no_proxy):
    # Нечего включать — is_enabled всегда False, пока VK_PROXY не задан.
    vk_proxy.set_enabled(True)
    assert vk_proxy.is_enabled() is False
    assert vk_proxy.active_url() is None


def test_enabled_survives_across_module_state(tmp_db, with_proxy):
    """Настройка читается из БД каждый раз — не кешируется между вызовами."""
    assert vk_proxy.is_enabled() is True
    vk_proxy.set_enabled(False)
    assert vk_proxy.is_enabled() is False


# ─── Диагностика ────────────────────────────────────────────────────────────────

def test_check_vk_direct_ok():
    with patch.object(vk_proxy.requests, "get", return_value=MagicMock(raise_for_status=lambda: None)):
        ok, info = vk_proxy.check_vk_direct()
    assert ok is True
    assert "мс" in info


def test_check_vk_direct_failure():
    with patch.object(vk_proxy.requests, "get", side_effect=requests.exceptions.ConnectTimeout("timed out")):
        ok, info = vk_proxy.check_vk_direct()
    assert ok is False
    assert "timed out" in info.lower() or info


def test_check_proxy_server_not_configured(no_proxy):
    ok, info = vk_proxy.check_proxy_server()
    assert ok is False
    assert "не задан" in info


def test_check_proxy_server_reaches_socket(with_proxy):
    with patch.object(vk_proxy.socket, "create_connection") as conn:
        conn.return_value = MagicMock()
        ok, info = vk_proxy.check_proxy_server()
    assert ok is True
    conn.assert_called_once_with(("1.2.3.4", 1080), timeout=vk_proxy._CONNECT_TIMEOUT)


def test_check_proxy_server_unreachable(with_proxy):
    with patch.object(vk_proxy.socket, "create_connection", side_effect=OSError("refused")):
        ok, info = vk_proxy.check_proxy_server()
    assert ok is False


def test_check_vk_via_proxy_not_configured(no_proxy):
    ok, info = vk_proxy.check_vk_via_proxy()
    assert ok is False
    assert "не задан" in info


def test_check_vk_via_proxy_uses_proxies_kwarg(with_proxy):
    with patch.object(vk_proxy.requests, "get", return_value=MagicMock(raise_for_status=lambda: None)) as get:
        ok, info = vk_proxy.check_vk_via_proxy()
    assert ok is True
    assert get.call_args.kwargs["proxies"] == {
        "http": "socks5h://user:pass@1.2.3.4:1080",
        "https": "socks5h://user:pass@1.2.3.4:1080",
    }


def test_check_vk_via_proxy_failure_message_short():
    with patch.dict("os.environ", {"VK_PROXY": "socks5h://u:p@1.2.3.4:1080"}):
        with patch.object(vk_proxy.requests, "get", side_effect=Exception("x" * 500)):
            ok, info = vk_proxy.check_vk_via_proxy()
    assert ok is False
    assert len(info) <= 200
