"""Тесты чистых функций из bot.py — без реального Telegram-соединения."""

import asyncio
import os
from contextlib import contextmanager
from datetime import date, timedelta
from unittest.mock import MagicMock, patch, AsyncMock, call

import pytest
import requests as _requests
from vk_api.exceptions import ApiError

import bot


# ─── _mask_token ──────────────────────────────────────────────────────────────

def test_mask_token_long():
    token = "vk1.a.SomeVeryLongSecretToken1234567"
    result = bot._mask_token(token)
    assert result.startswith("vk1.a")
    assert "SomeVeryLongSecretToken" not in result
    assert "…" in result


def test_mask_token_shows_first_and_last():
    token = "vk1.a.ABCDEFGHIJKLMNOPQRSTUVWXYZ"
    result = bot._mask_token(token)
    assert result[:6] == token[:6]
    assert result[-4:] == token[-4:]


def test_mask_token_short():
    result = bot._mask_token("abc")
    assert result == "•••"
    assert "a" not in result


def test_mask_token_exactly_12_chars():
    token = "123456789012"
    result = bot._mask_token(token)
    assert result == "•" * 12


def test_mask_token_13_chars_uses_ellipsis():
    token = "1234567890123"
    result = bot._mask_token(token)
    assert "…" in result


# ─── _PlatformUrlFilter ───────────────────────────────────────────────────────

def _msg(text):
    m = MagicMock()
    m.text = text
    return m


@pytest.mark.parametrize("url", [
    "https://vm.tiktok.com/ZGd9V3VWS/",
    "https://www.tiktok.com/@user/video/123",
    "https://likee.video/v/AbCdEf",
    "https://youtube.com/shorts/abc",
    "https://youtu.be/dQw4w9WgXcQ",
    "https://vk.com/video-123_456",
    "https://vk.com/clip-100_200",
    "https://instagram.com/reel/abc",
])
def test_url_filter_accepts_platform_urls(url):
    assert bot._URL_FILTER.filter(_msg(url)) is True


@pytest.mark.parametrize("text", [
    "123123123",
    "просто текст",
    "https://google.com",
    "",
    "   ",
])
def test_url_filter_rejects_non_platform(text):
    assert not bot._URL_FILTER.filter(_msg(text))


def test_url_filter_rejects_none_text():
    assert not bot._URL_FILTER.filter(_msg(None))


# ─── build_time_keyboard ──────────────────────────────────────────────────────

def _all_callbacks(markup):
    return [btn.callback_data for row in markup.inline_keyboard for btn in row]


def test_time_keyboard_has_now_button():
    markup = bot.build_time_keyboard()
    assert "now" in _all_callbacks(markup)


def test_time_keyboard_has_custom_button():
    markup = bot.build_time_keyboard()
    assert "custom" in _all_callbacks(markup)


def test_time_keyboard_has_tomorrow_slots():
    markup = bot.build_time_keyboard()
    tomorrow = (date.today() + timedelta(days=1)).isoformat()
    slots = [c for c in _all_callbacks(markup) if c and c.startswith(f"slot_{tomorrow}")]
    assert len(slots) == len(bot.TIME_SLOTS)


def test_time_keyboard_tomorrow_slots_match_time_slots():
    markup = bot.build_time_keyboard()
    tomorrow = (date.today() + timedelta(days=1)).isoformat()
    hours = sorted(
        int(c.rsplit("_", 1)[1])
        for c in _all_callbacks(markup)
        if c and c.startswith(f"slot_{tomorrow}")
    )
    assert hours == sorted(bot.TIME_SLOTS)


def test_time_keyboard_rows_not_too_wide():
    markup = bot.build_time_keyboard()
    for row in markup.inline_keyboard:
        assert len(row) <= bot.TIME_SLOTS_ROW_SIZE + 1  # +1 для nav-строк


def test_time_keyboard_slot_callback_format():
    markup = bot.build_time_keyboard()
    tomorrow = (date.today() + timedelta(days=1)).isoformat()
    slot = f"slot_{tomorrow}_{bot.TIME_SLOTS[0]}"
    assert slot in _all_callbacks(markup)


# ─── build_groups_select_keyboard ────────────────────────────────────────────

def test_groups_select_keyboard_empty(tmp_path, monkeypatch):
    import db
    monkeypatch.setattr(db, "DB_PATH", str(tmp_path / "t.db"))
    db.init_db()
    db.ensure_user(1)
    markup = bot.build_groups_select_keyboard(1)
    assert len(markup.inline_keyboard) == 0


def test_groups_select_keyboard_buttons(tmp_path, monkeypatch):
    import db
    monkeypatch.setattr(db, "DB_PATH", str(tmp_path / "t.db"))
    db.init_db()
    db.ensure_user(1)
    db.add_group(1, 100, "Alpha")
    db.add_group(1, 200, "Beta")
    markup = bot.build_groups_select_keyboard(1)
    labels = [btn.text for row in markup.inline_keyboard for btn in row]
    assert "Alpha" in labels
    assert "Beta" in labels


def test_groups_select_keyboard_callback_format(tmp_path, monkeypatch):
    import db
    monkeypatch.setattr(db, "DB_PATH", str(tmp_path / "t.db"))
    db.init_db()
    db.ensure_user(1)
    db.add_group(1, 100, "G")
    markup = bot.build_groups_select_keyboard(1)
    callbacks = _all_callbacks(markup)
    assert any(c.startswith("upgroup_") for c in callbacks)


# ─── _download_video (семафор + таймаут) ─────────────────────────────────────

@pytest.mark.asyncio
async def test_download_video_calls_correct_downloader():
    bot._download_semaphore = asyncio.Semaphore(5)
    with patch("bot.download_tiktok", new_callable=AsyncMock, return_value=("/tmp/f.mp4", "Title")) as mock_dl:
        path, title = await bot._download_video("tiktok", "https://vm.tiktok.com/x")
    mock_dl.assert_called_once()
    assert path == "/tmp/f.mp4"
    assert title == "Title"


@pytest.mark.asyncio
async def test_download_video_unknown_platform_raises():
    bot._download_semaphore = asyncio.Semaphore(5)
    with pytest.raises(ValueError, match="Неизвестная платформа"):
        await bot._download_video("unknown", "https://example.com")


@pytest.mark.asyncio
async def test_download_video_timeout_raises():
    bot._download_semaphore = asyncio.Semaphore(5)
    original_timeout = bot.DOWNLOAD_TIMEOUT

    async def slow_download(*args, **kwargs):
        await asyncio.sleep(10)
        return "/tmp/f.mp4", "Title"

    with patch.object(bot, "DOWNLOAD_TIMEOUT", 1):
        with patch("bot.download_tiktok", side_effect=slow_download):
            with pytest.raises(asyncio.TimeoutError):
                await bot._download_video("tiktok", "https://vm.tiktok.com/x")


@pytest.mark.asyncio
async def test_download_semaphore_limits_concurrency():
    """Семафор не пропускает больше N одновременных загрузок."""
    bot._download_semaphore = asyncio.Semaphore(2)
    active = []
    max_active = []

    async def slow_download(*args, **kwargs):
        active.append(1)
        max_active.append(len(active))
        await asyncio.sleep(0.05)
        active.pop()
        return "/tmp/f.mp4", "Title"

    with patch("bot.download_tiktok", side_effect=slow_download):
        with patch.object(bot, "DOWNLOAD_TIMEOUT", 0):
            tasks = [bot._download_video("tiktok", "https://vm.tiktok.com/x") for _ in range(6)]
            await asyncio.gather(*tasks)

    assert max(max_active) <= 2


# ─── MAX_GROUPS_PER_USER константа ───────────────────────────────────────────

def test_max_groups_constant_defined():
    assert isinstance(bot.MAX_GROUPS_PER_USER, int)
    assert bot.MAX_GROUPS_PER_USER > 0


def test_max_groups_is_50():
    assert bot.MAX_GROUPS_PER_USER == 50


# ─── Пагинация: вспомогательные фикстуры ──────────────────────────────────────

def _setup_db(tmp_path, monkeypatch):
    import db
    monkeypatch.setattr(db, "DB_PATH", str(tmp_path / "t.db"))
    db.init_db()
    db.ensure_user(1)
    return db


def _nav_callbacks(markup):
    """Возвращает все callback_data стрелок навигации из клавиатуры."""
    return [
        btn.callback_data
        for row in markup.inline_keyboard
        for btn in row
        if btn.callback_data and ("_pg_" in btn.callback_data)
    ]


def _nav_labels(markup):
    return [
        btn.text
        for row in markup.inline_keyboard
        for btn in row
        if btn.callback_data and "_pg_" in btn.callback_data
    ]


# ─── build_groups_select_keyboard: пагинация ──────────────────────────────────

class TestGroupsSelectPagination:

    def test_no_nav_when_at_or_below_page_size(self, tmp_path, monkeypatch):
        db = _setup_db(tmp_path, monkeypatch)
        for i in range(bot.KEYBOARD_PAGE_SIZE):
            db.add_group(1, 100 + i, f"Group{i}")
        markup = bot.build_groups_select_keyboard(1)
        assert _nav_callbacks(markup) == []

    def test_right_arrow_on_first_page_when_overflow(self, tmp_path, monkeypatch):
        db = _setup_db(tmp_path, monkeypatch)
        for i in range(bot.KEYBOARD_PAGE_SIZE + 1):
            db.add_group(1, 100 + i, f"G{i}")
        markup = bot.build_groups_select_keyboard(1, page=0)
        nav = _nav_callbacks(markup)
        assert any(c == "upgroup_pg_1" for c in nav)
        assert not any("upgroup_pg_-" in c for c in nav)

    def test_no_left_arrow_on_first_page(self, tmp_path, monkeypatch):
        db = _setup_db(tmp_path, monkeypatch)
        for i in range(bot.KEYBOARD_PAGE_SIZE + 1):
            db.add_group(1, 100 + i, f"G{i}")
        markup = bot.build_groups_select_keyboard(1, page=0)
        assert "⬅️" not in _nav_labels(markup)

    def test_left_arrow_on_second_page(self, tmp_path, monkeypatch):
        db = _setup_db(tmp_path, monkeypatch)
        for i in range(bot.KEYBOARD_PAGE_SIZE + 1):
            db.add_group(1, 100 + i, f"G{i}")
        markup = bot.build_groups_select_keyboard(1, page=1)
        nav = _nav_callbacks(markup)
        assert any(c == "upgroup_pg_0" for c in nav)

    def test_no_right_arrow_on_last_page(self, tmp_path, monkeypatch):
        db = _setup_db(tmp_path, monkeypatch)
        for i in range(bot.KEYBOARD_PAGE_SIZE + 1):
            db.add_group(1, 100 + i, f"G{i}")
        markup = bot.build_groups_select_keyboard(1, page=1)
        assert "➡️" not in _nav_labels(markup)

    def test_both_arrows_on_middle_page(self, tmp_path, monkeypatch):
        db = _setup_db(tmp_path, monkeypatch)
        for i in range(bot.KEYBOARD_PAGE_SIZE * 3):
            db.add_group(1, 100 + i, f"G{i}")
        markup = bot.build_groups_select_keyboard(1, page=1)
        labels = _nav_labels(markup)
        assert "⬅️" in labels
        assert "➡️" in labels

    def test_first_page_shows_page_size_items(self, tmp_path, monkeypatch):
        db = _setup_db(tmp_path, monkeypatch)
        for i in range(bot.KEYBOARD_PAGE_SIZE + 5):
            db.add_group(1, 100 + i, f"G{i}")
        markup = bot.build_groups_select_keyboard(1, page=0)
        group_rows = [
            row for row in markup.inline_keyboard
            if any(btn.callback_data and btn.callback_data.startswith("upgroup_") and "_pg_" not in btn.callback_data for btn in row)
        ]
        assert len(group_rows) == bot.KEYBOARD_PAGE_SIZE

    def test_second_page_shows_remaining_items(self, tmp_path, monkeypatch):
        db = _setup_db(tmp_path, monkeypatch)
        extra = 3
        for i in range(bot.KEYBOARD_PAGE_SIZE + extra):
            db.add_group(1, 100 + i, f"G{i}")
        markup = bot.build_groups_select_keyboard(1, page=1)
        group_rows = [
            row for row in markup.inline_keyboard
            if any(btn.callback_data and btn.callback_data.startswith("upgroup_") and "_pg_" not in btn.callback_data for btn in row)
        ]
        assert len(group_rows) == extra

    def test_page_zero_default(self, tmp_path, monkeypatch):
        db = _setup_db(tmp_path, monkeypatch)
        for i in range(bot.KEYBOARD_PAGE_SIZE + 1):
            db.add_group(1, 100 + i, f"G{i}")
        markup_default = bot.build_groups_select_keyboard(1)
        markup_explicit = bot.build_groups_select_keyboard(1, page=0)
        assert len(markup_default.inline_keyboard) == len(markup_explicit.inline_keyboard)


# ─── build_desc_keyboard: пагинация ───────────────────────────────────────────

class TestDescKeyboardPagination:

    def test_fixed_buttons_always_present(self, tmp_path, monkeypatch):
        _setup_db(tmp_path, monkeypatch)
        markup = bot.build_desc_keyboard(1)
        callbacks = _all_callbacks(markup)
        assert "updesc_custom" in callbacks
        assert "updesc_none" in callbacks

    def test_fixed_buttons_present_on_page_two(self, tmp_path, monkeypatch):
        db = _setup_db(tmp_path, monkeypatch)
        for i in range(bot.KEYBOARD_PAGE_SIZE + 1):
            db.add_template(1, f"T{i}", f"body{i}")
        markup = bot.build_desc_keyboard(1, page=1)
        callbacks = _all_callbacks(markup)
        assert "updesc_custom" in callbacks
        assert "updesc_none" in callbacks

    def test_no_nav_when_at_or_below_page_size(self, tmp_path, monkeypatch):
        db = _setup_db(tmp_path, monkeypatch)
        for i in range(bot.KEYBOARD_PAGE_SIZE):
            db.add_template(1, f"T{i}", f"body{i}")
        markup = bot.build_desc_keyboard(1)
        assert _nav_callbacks(markup) == []

    def test_right_arrow_on_first_page_when_overflow(self, tmp_path, monkeypatch):
        db = _setup_db(tmp_path, monkeypatch)
        for i in range(bot.KEYBOARD_PAGE_SIZE + 1):
            db.add_template(1, f"T{i}", f"body{i}")
        markup = bot.build_desc_keyboard(1, page=0)
        nav = _nav_callbacks(markup)
        assert any(c == "updesc_pg_1" for c in nav)

    def test_left_arrow_on_second_page(self, tmp_path, monkeypatch):
        db = _setup_db(tmp_path, monkeypatch)
        for i in range(bot.KEYBOARD_PAGE_SIZE + 1):
            db.add_template(1, f"T{i}", f"body{i}")
        markup = bot.build_desc_keyboard(1, page=1)
        nav = _nav_callbacks(markup)
        assert any(c == "updesc_pg_0" for c in nav)

    def test_no_right_arrow_on_last_page(self, tmp_path, monkeypatch):
        db = _setup_db(tmp_path, monkeypatch)
        for i in range(bot.KEYBOARD_PAGE_SIZE + 1):
            db.add_template(1, f"T{i}", f"body{i}")
        markup = bot.build_desc_keyboard(1, page=1)
        assert "➡️" not in _nav_labels(markup)

    def test_both_arrows_on_middle_page(self, tmp_path, monkeypatch):
        db = _setup_db(tmp_path, monkeypatch)
        for i in range(bot.KEYBOARD_PAGE_SIZE * 3):
            db.add_template(1, f"T{i}", f"body{i}")
        markup = bot.build_desc_keyboard(1, page=1)
        labels = _nav_labels(markup)
        assert "⬅️" in labels
        assert "➡️" in labels

    def test_template_callbacks_on_page(self, tmp_path, monkeypatch):
        db = _setup_db(tmp_path, monkeypatch)
        for i in range(bot.KEYBOARD_PAGE_SIZE + 1):
            db.add_template(1, f"T{i}", f"body{i}")
        markup = bot.build_desc_keyboard(1, page=0)
        tpl_callbacks = [c for c in _all_callbacks(markup) if c and c.startswith("updesc_tpl_")]
        assert len(tpl_callbacks) == bot.KEYBOARD_PAGE_SIZE


# ─── build_groups_manage_keyboard: пагинация ──────────────────────────────────

class TestGroupsManagePagination:

    def test_add_button_always_present(self, tmp_path, monkeypatch):
        _setup_db(tmp_path, monkeypatch)
        markup = bot.build_groups_manage_keyboard(1)
        assert "g_add" in _all_callbacks(markup)

    def test_add_button_present_on_page_two(self, tmp_path, monkeypatch):
        db = _setup_db(tmp_path, monkeypatch)
        for i in range(bot.KEYBOARD_PAGE_SIZE + 1):
            db.add_group(1, 100 + i, f"G{i}")
        markup = bot.build_groups_manage_keyboard(1, page=1)
        assert "g_add" in _all_callbacks(markup)

    def test_no_nav_when_at_or_below_page_size(self, tmp_path, monkeypatch):
        db = _setup_db(tmp_path, monkeypatch)
        for i in range(bot.KEYBOARD_PAGE_SIZE):
            db.add_group(1, 100 + i, f"G{i}")
        markup = bot.build_groups_manage_keyboard(1)
        assert _nav_callbacks(markup) == []

    def test_right_arrow_on_first_page_when_overflow(self, tmp_path, monkeypatch):
        db = _setup_db(tmp_path, monkeypatch)
        for i in range(bot.KEYBOARD_PAGE_SIZE + 1):
            db.add_group(1, 100 + i, f"G{i}")
        markup = bot.build_groups_manage_keyboard(1, page=0)
        nav = _nav_callbacks(markup)
        assert any(c == "g_pg_1" for c in nav)

    def test_left_arrow_on_second_page(self, tmp_path, monkeypatch):
        db = _setup_db(tmp_path, monkeypatch)
        for i in range(bot.KEYBOARD_PAGE_SIZE + 1):
            db.add_group(1, 100 + i, f"G{i}")
        markup = bot.build_groups_manage_keyboard(1, page=1)
        nav = _nav_callbacks(markup)
        assert any(c == "g_pg_0" for c in nav)

    def test_no_right_arrow_on_last_page(self, tmp_path, monkeypatch):
        db = _setup_db(tmp_path, monkeypatch)
        for i in range(bot.KEYBOARD_PAGE_SIZE + 1):
            db.add_group(1, 100 + i, f"G{i}")
        markup = bot.build_groups_manage_keyboard(1, page=1)
        assert "➡️" not in _nav_labels(markup)

    def test_both_arrows_on_middle_page(self, tmp_path, monkeypatch):
        db = _setup_db(tmp_path, monkeypatch)
        for i in range(bot.KEYBOARD_PAGE_SIZE * 3):
            db.add_group(1, 100 + i, f"G{i}")
        markup = bot.build_groups_manage_keyboard(1, page=1)
        labels = _nav_labels(markup)
        assert "⬅️" in labels
        assert "➡️" in labels

    def test_each_group_has_rename_and_delete(self, tmp_path, monkeypatch):
        db = _setup_db(tmp_path, monkeypatch)
        db.add_group(1, 100, "TestGroup")
        markup = bot.build_groups_manage_keyboard(1)
        callbacks = _all_callbacks(markup)
        assert any(c and c.startswith("g_rename_") for c in callbacks)
        assert any(c and c.startswith("g_del_") for c in callbacks)

    def test_first_page_shows_page_size_groups(self, tmp_path, monkeypatch):
        db = _setup_db(tmp_path, monkeypatch)
        for i in range(bot.KEYBOARD_PAGE_SIZE + 5):
            db.add_group(1, 100 + i, f"G{i}")
        markup = bot.build_groups_manage_keyboard(1, page=0)
        rename_callbacks = [c for c in _all_callbacks(markup) if c and c.startswith("g_rename_")]
        assert len(rename_callbacks) == bot.KEYBOARD_PAGE_SIZE


# ─── build_templates_manage_keyboard: пагинация ───────────────────────────────

class TestTemplatesManagePagination:

    def test_add_button_always_present(self, tmp_path, monkeypatch):
        _setup_db(tmp_path, monkeypatch)
        markup = bot.build_templates_manage_keyboard(1)
        assert "t_add" in _all_callbacks(markup)

    def test_add_button_present_on_page_two(self, tmp_path, monkeypatch):
        db = _setup_db(tmp_path, monkeypatch)
        for i in range(bot.KEYBOARD_PAGE_SIZE + 1):
            db.add_template(1, f"T{i}", f"body{i}")
        markup = bot.build_templates_manage_keyboard(1, page=1)
        assert "t_add" in _all_callbacks(markup)

    def test_no_nav_when_at_or_below_page_size(self, tmp_path, monkeypatch):
        db = _setup_db(tmp_path, monkeypatch)
        for i in range(bot.KEYBOARD_PAGE_SIZE):
            db.add_template(1, f"T{i}", f"body{i}")
        markup = bot.build_templates_manage_keyboard(1)
        assert _nav_callbacks(markup) == []

    def test_right_arrow_on_first_page_when_overflow(self, tmp_path, monkeypatch):
        db = _setup_db(tmp_path, monkeypatch)
        for i in range(bot.KEYBOARD_PAGE_SIZE + 1):
            db.add_template(1, f"T{i}", f"body{i}")
        markup = bot.build_templates_manage_keyboard(1, page=0)
        nav = _nav_callbacks(markup)
        assert any(c == "t_pg_1" for c in nav)

    def test_left_arrow_on_second_page(self, tmp_path, monkeypatch):
        db = _setup_db(tmp_path, monkeypatch)
        for i in range(bot.KEYBOARD_PAGE_SIZE + 1):
            db.add_template(1, f"T{i}", f"body{i}")
        markup = bot.build_templates_manage_keyboard(1, page=1)
        nav = _nav_callbacks(markup)
        assert any(c == "t_pg_0" for c in nav)

    def test_no_right_arrow_on_last_page(self, tmp_path, monkeypatch):
        db = _setup_db(tmp_path, monkeypatch)
        for i in range(bot.KEYBOARD_PAGE_SIZE + 1):
            db.add_template(1, f"T{i}", f"body{i}")
        markup = bot.build_templates_manage_keyboard(1, page=1)
        assert "➡️" not in _nav_labels(markup)

    def test_both_arrows_on_middle_page(self, tmp_path, monkeypatch):
        db = _setup_db(tmp_path, monkeypatch)
        for i in range(bot.KEYBOARD_PAGE_SIZE * 3):
            db.add_template(1, f"T{i}", f"body{i}")
        markup = bot.build_templates_manage_keyboard(1, page=1)
        labels = _nav_labels(markup)
        assert "⬅️" in labels
        assert "➡️" in labels

    def test_each_template_has_edit_and_delete(self, tmp_path, monkeypatch):
        db = _setup_db(tmp_path, monkeypatch)
        db.add_template(1, "MyTitle", "MyBody")
        markup = bot.build_templates_manage_keyboard(1)
        callbacks = _all_callbacks(markup)
        assert any(c and c.startswith("t_edit_") for c in callbacks)
        assert any(c and c.startswith("t_del_") for c in callbacks)

    def test_first_page_shows_page_size_templates(self, tmp_path, monkeypatch):
        db = _setup_db(tmp_path, monkeypatch)
        for i in range(bot.KEYBOARD_PAGE_SIZE + 5):
            db.add_template(1, f"T{i}", f"body{i}")
        markup = bot.build_templates_manage_keyboard(1, page=0)
        edit_callbacks = [c for c in _all_callbacks(markup) if c and c.startswith("t_edit_")]
        assert len(edit_callbacks) == bot.KEYBOARD_PAGE_SIZE

    def test_second_page_shows_remaining_templates(self, tmp_path, monkeypatch):
        db = _setup_db(tmp_path, monkeypatch)
        extra = 4
        for i in range(bot.KEYBOARD_PAGE_SIZE + extra):
            db.add_template(1, f"T{i}", f"body{i}")
        markup = bot.build_templates_manage_keyboard(1, page=1)
        edit_callbacks = [c for c in _all_callbacks(markup) if c and c.startswith("t_edit_")]
        assert len(edit_callbacks) == extra


# ─── KEYBOARD_PAGE_SIZE константа ────────────────────────────────────────────

def test_keyboard_page_size_is_15():
    assert bot.KEYBOARD_PAGE_SIZE == 15


# ─── Helpers: upload tests ────────────────────────────────────────────────────

def _mock_resp(data, *, text=None, raise_for_status=None):
    """Мок объекта requests.Response."""
    m = MagicMock()
    m.json.return_value = data
    m.text = text or str(data)[:500]
    if raise_for_status:
        m.raise_for_status.side_effect = raise_for_status
    else:
        m.raise_for_status.return_value = None
    return m


# Типовые ответы VK API
_CREATE_OK  = {"response": {"upload_url": "https://upload.vk.com/x", "video_id": 42, "owner_id": -100}}
_UPLOAD_OK  = {"video_id": 42, "owner_id": -100}
_EDIT_OK    = {"response": 1}
_PUBLISH_OK = {"response": {"video": {"wall_post_id": 999}}}
_SAVE_OK    = {"response": {"video_id": 55, "owner_id": -100, "upload_url": "https://upload.vk.com/y"}}
_WALL_OK    = {"response": {"post_id": 77}}
_ERR_5      = {"error": {"error_code": 5,  "error_msg": "User authorization failed"}}
_ERR_15     = {"error": {"error_code": 15, "error_msg": "Access denied"}}
_ERR_3001   = {"error": {"error_code": 3001, "error_msg": "Video not ready"}}


@pytest.fixture
def tmp_video(tmp_path):
    """Временный файл-заглушка видео."""
    f = tmp_path / "video.mp4"
    f.write_bytes(b"fake_video_data" * 100)
    return str(f)


def _called_urls(mock_post):
    return [c.args[0] for c in mock_post.call_args_list]


# ─── _upload_short_video ──────────────────────────────────────────────────────

# Ответы vk_api уже без обёртки {"response": ...} — библиотека разворачивает сама.
_CREATE_RESP  = {"upload_url": "https://upload.vk.com/x", "video_id": 42, "owner_id": -100}
_PUBLISH_RESP = {"video": {"wall_post_id": 999}}


def _api_err(code: int, msg: str = "Error") -> ApiError:
    """Создаёт ApiError с нужным кодом, минуя сложный конструктор vk_api."""
    err = ApiError.__new__(ApiError)
    err.code = code
    err.error = {"error_code": code, "error_msg": msg}
    err.args = (f"[{code}] {msg}",)
    return err


@contextmanager
def _vk_mock(create_rv=None, create_se=None,
             edit_rv=1,    edit_se=None,
             publish_rv=None, publish_se=None):
    """Мокирует vk_api.VkApi и возвращает mock-объект vk API."""
    mock_vk = MagicMock()

    if create_se is not None:
        mock_vk.shortVideo.create.side_effect = create_se
    else:
        mock_vk.shortVideo.create.return_value = create_rv

    if edit_se is not None:
        mock_vk.shortVideo.edit.side_effect = edit_se
    else:
        mock_vk.shortVideo.edit.return_value = edit_rv

    if publish_se is not None:
        mock_vk.shortVideo.publish.side_effect = publish_se
    else:
        mock_vk.shortVideo.publish.return_value = publish_rv

    with patch("vk_api.VkApi") as mock_cls:
        mock_cls.return_value.get_api.return_value = mock_vk
        yield mock_vk


class TestUploadShortVideo:

    def test_happy_path_no_description(self, tmp_video):
        """Все этапы успешны, описание пустое — shortVideo.edit не вызывается."""
        with _vk_mock(create_rv=_CREATE_RESP, publish_rv=_PUBLISH_RESP) as mock_vk, \
             patch("requests.post", return_value=_mock_resp(_UPLOAD_OK)):
            bot._upload_short_video("token", 100, tmp_video, "")

        mock_vk.shortVideo.create.assert_called_once()
        mock_vk.shortVideo.edit.assert_not_called()
        mock_vk.shortVideo.publish.assert_called_once()

    def test_happy_path_with_description_calls_edit(self, tmp_video):
        """С непустым описанием shortVideo.edit вызывается между upload и publish."""
        with _vk_mock(create_rv=_CREATE_RESP, publish_rv=_PUBLISH_RESP) as mock_vk, \
             patch("requests.post", return_value=_mock_resp(_UPLOAD_OK)):
            bot._upload_short_video("token", 100, tmp_video, "Описание #тег")

        mock_vk.shortVideo.edit.assert_called_once()
        mock_vk.shortVideo.publish.assert_called_once()

    def test_create_api_error_raises_vkerror(self, tmp_video):
        with _vk_mock(create_se=_api_err(5, "Auth failed")):
            with pytest.raises(bot.VKError) as exc_info:
                bot._upload_short_video("token", 100, tmp_video, "")
        assert exc_info.value.code == 5
        assert exc_info.value.stage == "VK shortVideo.create"

    def test_create_network_error_raises_vkerror(self, tmp_video):
        with _vk_mock(create_se=Exception("connection timeout")):
            with pytest.raises(bot.VKError) as exc_info:
                bot._upload_short_video("token", 100, tmp_video, "")
        assert exc_info.value.network is True
        assert exc_info.value.stage == "VK shortVideo.create"

    def test_upload_network_error_raises_vkerror(self, tmp_video):
        with _vk_mock(create_rv=_CREATE_RESP), \
             patch("requests.post", side_effect=_requests.exceptions.ConnectionError("upload failed")):
            with pytest.raises(bot.VKError) as exc_info:
                bot._upload_short_video("token", 100, tmp_video, "")
        assert exc_info.value.network is True
        assert exc_info.value.stage == "загрузка файла shortVideo"

    def test_edit_api_error_raises_vkerror(self, tmp_video):
        with _vk_mock(create_rv=_CREATE_RESP, edit_se=_api_err(15, "Access denied")), \
             patch("requests.post", return_value=_mock_resp(_UPLOAD_OK)):
            with pytest.raises(bot.VKError) as exc_info:
                bot._upload_short_video("token", 100, tmp_video, "Описание")
        assert exc_info.value.code == 15
        assert exc_info.value.stage == "VK shortVideo.edit"

    def test_publish_retries_on_3001_then_succeeds(self, tmp_video, monkeypatch):
        """Первые две попытки publish возвращают 3001, третья — успех."""
        monkeypatch.setattr(bot, "VK_SHORT_VIDEO_POLL_INTERVAL", 0)
        publish_effects = [
            _api_err(3001, "Video not ready"),
            _api_err(3001, "Video not ready"),
            _PUBLISH_RESP,  # третья попытка — успех (return_value через side_effect)
        ]
        # side_effect: исключение бросается, словарь — возвращается
        with _vk_mock(create_rv=_CREATE_RESP, publish_se=publish_effects) as mock_vk, \
             patch("requests.post", return_value=_mock_resp(_UPLOAD_OK)):
            bot._upload_short_video("token", 100, tmp_video, "")

        assert mock_vk.shortVideo.publish.call_count == 3

    def test_publish_3001_exhausted_raises_vkerror(self, tmp_video, monkeypatch):
        """Все попытки вернули 3001 — VKError(3001) после исчерпания лимита."""
        monkeypatch.setattr(bot, "VK_SHORT_VIDEO_POLL_ATTEMPTS", 3)
        monkeypatch.setattr(bot, "VK_SHORT_VIDEO_POLL_INTERVAL", 0)
        with _vk_mock(create_rv=_CREATE_RESP,
                      publish_se=[_api_err(3001)] * 3), \
             patch("requests.post", return_value=_mock_resp(_UPLOAD_OK)):
            with pytest.raises(bot.VKError) as exc_info:
                bot._upload_short_video("token", 100, tmp_video, "")
        assert exc_info.value.code == 3001
        assert exc_info.value.stage == "VK shortVideo.publish"

    def test_publish_non_3001_error_raises_immediately(self, tmp_video, monkeypatch):
        """Ошибка с кодом != 3001 — VKError бросается сразу, polling не продолжается."""
        monkeypatch.setattr(bot, "VK_SHORT_VIDEO_POLL_INTERVAL", 0)
        with _vk_mock(create_rv=_CREATE_RESP,
                      publish_se=_api_err(15, "Access denied")) as mock_vk, \
             patch("requests.post", return_value=_mock_resp(_UPLOAD_OK)):
            with pytest.raises(bot.VKError) as exc_info:
                bot._upload_short_video("token", 100, tmp_video, "")
        assert exc_info.value.code == 15
        assert mock_vk.shortVideo.publish.call_count == 1  # только одна попытка

    def test_file_size_passed_to_create(self, tmp_video):
        """os.path.getsize(file_path) передаётся как file_size в shortVideo.create."""
        with _vk_mock(create_rv=_CREATE_RESP, publish_rv=_PUBLISH_RESP) as mock_vk, \
             patch("requests.post", return_value=_mock_resp(_UPLOAD_OK)):
            bot._upload_short_video("token", 100, tmp_video, "")

        _, kwargs = mock_vk.shortVideo.create.call_args
        assert kwargs["file_size"] == os.path.getsize(tmp_video) // 1024  # в КБ

    def test_upload_uses_file_field_not_video_file(self, tmp_video):
        """Поле при загрузке называется 'file', а не 'video_file'."""
        with _vk_mock(create_rv=_CREATE_RESP, publish_rv=_PUBLISH_RESP), \
             patch("requests.post", return_value=_mock_resp(_UPLOAD_OK)) as mock_post:
            bot._upload_short_video("token", 100, tmp_video, "")

        upload_files = mock_post.call_args.kwargs["files"]
        assert "file" in upload_files
        assert "video_file" not in upload_files

    def test_description_passed_to_edit(self, tmp_video):
        """Текст описания корректно передаётся в shortVideo.edit."""
        with _vk_mock(create_rv=_CREATE_RESP, publish_rv=_PUBLISH_RESP) as mock_vk, \
             patch("requests.post", return_value=_mock_resp(_UPLOAD_OK)):
            bot._upload_short_video("token", 100, tmp_video, "Мой текст")

        _, kwargs = mock_vk.shortVideo.edit.call_args
        assert kwargs["description"] == "Мой текст"

    def test_publish_stops_polling_on_success(self, tmp_video, monkeypatch):
        """После первого успешного publish повторных вызовов нет."""
        monkeypatch.setattr(bot, "VK_SHORT_VIDEO_POLL_INTERVAL", 0)
        with _vk_mock(create_rv=_CREATE_RESP, publish_rv=_PUBLISH_RESP) as mock_vk, \
             patch("requests.post", return_value=_mock_resp(_UPLOAD_OK)):
            bot._upload_short_video("token", 100, tmp_video, "")

        assert mock_vk.shortVideo.publish.call_count == 1


# ─── _upload_video_legacy ─────────────────────────────────────────────────────

class TestUploadVideoLegacy:

    def test_happy_path(self, tmp_video):
        with patch("requests.post", side_effect=[
            _mock_resp(_SAVE_OK),
            _mock_resp({}),
            _mock_resp(_WALL_OK),
        ]):
            bot._upload_video_legacy("token", 100, tmp_video, "Title", "Desc")

    def test_save_api_error_raises_vkerror(self, tmp_video):
        with patch("requests.post", return_value=_mock_resp(_ERR_5)):
            with pytest.raises(bot.VKError) as exc_info:
                bot._upload_video_legacy("token", 100, tmp_video, "T", "D")
        assert exc_info.value.code == 5
        assert exc_info.value.stage == "VK video.save"

    def test_save_network_error_raises_vkerror(self, tmp_video):
        with patch("requests.post", side_effect=_requests.exceptions.ConnectionError("net")):
            with pytest.raises(bot.VKError) as exc_info:
                bot._upload_video_legacy("token", 100, tmp_video, "T", "")
        assert exc_info.value.network is True
        assert exc_info.value.stage == "VK video.save"

    def test_upload_network_error_raises_vkerror(self, tmp_video):
        with patch("requests.post", side_effect=[
            _mock_resp(_SAVE_OK),
            _requests.exceptions.ConnectionError("upload failed"),
        ]):
            with pytest.raises(bot.VKError) as exc_info:
                bot._upload_video_legacy("token", 100, tmp_video, "T", "")
        assert exc_info.value.network is True
        assert exc_info.value.stage == "загрузка файла в VK"

    def test_wall_post_error_attempts_video_delete(self, tmp_video):
        """При ошибке wall.post бот пробует откатить видео через video.delete."""
        with patch("requests.post", side_effect=[
            _mock_resp(_SAVE_OK),
            _mock_resp({}),
            _mock_resp(_ERR_15),
            _mock_resp({"response": 1}),  # video.delete
        ]) as mock_post:
            with pytest.raises(bot.VKError) as exc_info:
                bot._upload_video_legacy("token", 100, tmp_video, "T", "D")
        assert exc_info.value.code == 15
        assert exc_info.value.stage == "VK wall.post"
        assert any("video.delete" in u for u in _called_urls(mock_post))

    def test_wall_post_network_error_raises_vkerror(self, tmp_video):
        with patch("requests.post", side_effect=[
            _mock_resp(_SAVE_OK),
            _mock_resp({}),
            _requests.exceptions.ConnectionError("wall.post failed"),
        ]):
            with pytest.raises(bot.VKError) as exc_info:
                bot._upload_video_legacy("token", 100, tmp_video, "T", "D")
        assert exc_info.value.network is True
        assert exc_info.value.stage == "VK wall.post"

    def test_upload_uses_video_file_field(self, tmp_video):
        """Поле при загрузке называется 'video_file' (legacy-формат)."""
        with patch("requests.post", side_effect=[
            _mock_resp(_SAVE_OK),
            _mock_resp({}),
            _mock_resp(_WALL_OK),
        ]) as mock_post:
            bot._upload_video_legacy("token", 100, tmp_video, "T", "")

        upload_files = mock_post.call_args_list[1].kwargs["files"]
        assert "video_file" in upload_files
        assert "file" not in upload_files

    def test_description_passed_to_save(self, tmp_video):
        """Описание передаётся в video.save как параметр description."""
        with patch("requests.post", side_effect=[
            _mock_resp(_SAVE_OK),
            _mock_resp({}),
            _mock_resp(_WALL_OK),
        ]) as mock_post:
            bot._upload_video_legacy("token", 100, tmp_video, "Title", "Текст описания")

        save_data = mock_post.call_args_list[0].kwargs["data"]
        assert save_data["description"] == "Текст описания"

    def test_no_description_key_when_empty(self, tmp_video):
        """Если описание пустое — ключ description не передаётся в video.save."""
        with patch("requests.post", side_effect=[
            _mock_resp(_SAVE_OK),
            _mock_resp({}),
            _mock_resp(_WALL_OK),
        ]) as mock_post:
            bot._upload_video_legacy("token", 100, tmp_video, "Title", "")

        save_data = mock_post.call_args_list[0].kwargs["data"]
        assert "description" not in save_data

    def test_wall_post_attachment_format(self, tmp_video):
        """attachments для wall.post формируется как 'video{owner_id}_{video_id}'."""
        with patch("requests.post", side_effect=[
            _mock_resp(_SAVE_OK),   # owner_id=-100, video_id=55
            _mock_resp({}),
            _mock_resp(_WALL_OK),
        ]) as mock_post:
            bot._upload_video_legacy("token", 100, tmp_video, "T", "")

        wall_data = mock_post.call_args_list[2].kwargs["data"]
        assert wall_data["attachments"] == "video-100_55"


# ─── upload_to_vk ─────────────────────────────────────────────────────────────

class TestUploadToVk:

    def test_calls_short_video_first(self, tmp_video):
        """При успехе shortVideo legacy-метод не вызывается."""
        with patch("bot._upload_short_video") as mock_short, \
             patch("bot._upload_video_legacy") as mock_legacy:
            bot.upload_to_vk("tok", 100, tmp_video, "T", "D")
        mock_short.assert_called_once()
        mock_legacy.assert_not_called()

    def test_falls_back_to_legacy_on_vkerror(self, tmp_video):
        """Если shortVideo бросает VKError — вызывается legacy."""
        with patch("bot._upload_short_video", side_effect=bot.VKError(15, "err", stage="x")), \
             patch("bot._upload_video_legacy") as mock_legacy:
            bot.upload_to_vk("tok", 100, tmp_video, "T", "D")
        mock_legacy.assert_called_once()

    def test_legacy_receives_correct_args(self, tmp_video):
        """legacy вызывается с теми же аргументами, что upload_to_vk."""
        with patch("bot._upload_short_video", side_effect=bot.VKError(1, "err")), \
             patch("bot._upload_video_legacy") as mock_legacy:
            bot.upload_to_vk("mytoken", 999, tmp_video, "MyTitle", "MyDesc")
        mock_legacy.assert_called_once_with("mytoken", 999, tmp_video, "MyTitle", "MyDesc", cancel_event=None)

    def test_negative_group_id_normalized(self, tmp_video):
        """Отрицательный vk_group_id нормализуется до abs() перед передачей."""
        with patch("bot._upload_short_video") as mock_short:
            bot.upload_to_vk("tok", -100, tmp_video, "T", "D")
        called_group_id = mock_short.call_args.args[1]
        assert called_group_id == 100

    def test_legacy_error_propagates(self, tmp_video):
        """Если оба метода падают — ошибка legacy пробрасывается наружу."""
        with patch("bot._upload_short_video", side_effect=bot.VKError(1, "short failed")), \
             patch("bot._upload_video_legacy", side_effect=bot.VKError(5, "legacy failed")):
            with pytest.raises(bot.VKError) as exc_info:
                bot.upload_to_vk("tok", 100, tmp_video, "T", "D")
        assert exc_info.value.code == 5

    def test_short_video_args_match(self, tmp_video):
        """_upload_short_video вызывается с правильными аргументами."""
        with patch("bot._upload_short_video") as mock_short:
            bot.upload_to_vk("mytoken", 42, tmp_video, "Title", "Desc")
        mock_short.assert_called_once_with("mytoken", 42, tmp_video, "Desc", cancel_event=None)


# ─── Распознавание ссылок на сообщества VK ────────────────────────────────────

@pytest.mark.parametrize("text,expected", [
    ("https://vk.com/club123456", "club123456"),
    ("vk.com/public123", "public123"),
    ("https://vk.ru/club123", "club123"),
    ("https://m.vk.ru/public55", "public55"),
    ("https://vk.ru/mygroup", "mygroup"),
    ("https://m.vk.com/mygroup?from=groups", "mygroup"),
    ("https://new.vk.com/mygroup", "mygroup"),
    ("https://vk.com/mygroup/", "mygroup"),
    ("https://vk.com/mygroup.", "mygroup"),
    ("https://vk.com/mygroup?w=wall-123_456", "mygroup"),
    ("Вот группа https://vk.com/mygroup, добавь", "mygroup"),
    ("https://vk.com/mygroup\nещё текст", "mygroup"),
    ("https://vk.com/clips/mygroup", "mygroup"),
    ("https://vkvideo.ru/@mygroup/all", "mygroup"),
    ("https://vk.me/mygroup", "mygroup"),
    ("@mygroup", "mygroup"),
    ("club123", "club123"),
    ("https://vk.com/wall-123_456", "wall-123_456"),
])
def test_extract_screen_name(text, expected):
    assert bot._extract_screen_name(text) == expected


@pytest.mark.parametrize("text,gid", [
    ("https://vk.com/club123", 123),
    ("https://vk.ru/public123", 123),
    ("vk.com/Club123", 123),
    ("https://vk.com/event77", 77),
    ("https://vk.com/board55", 55),
    ("https://vk.com/wall-123_456", 123),
    ("https://vk.com/video-123_456", 123),
    ("https://vk.com/videos-123", 123),
    ("https://vk.com/clips-123", 123),
    ("https://vk.com/album-123_0", 123),
    ("https://vk.com/topic-123_1", 123),
    ("https://vk.com/market-123", 123),
    ("-123", 123),
])
def test_resolve_numeric_links_without_token(text, gid):
    group_id, name, error, _ = bot.resolve_vk_group(None, text)
    assert (group_id, name, error) == (gid, None, None)


def test_resolve_short_name_without_token_asks_for_token():
    group_id, _, error, _ = bot.resolve_vk_group(None, "https://vk.com/mygroup")
    assert group_id is None
    assert "токен" in error


def test_resolve_short_name_via_groups_getbyid():
    with patch.object(bot, "_vk_call", return_value=({"groups": [{"id": 42, "name": "Моя группа"}]}, None)) as call_:
        assert bot.resolve_vk_group("tok", "https://vk.ru/mygroup") == (42, "Моя группа", None, None)
    call_.assert_called_once_with("groups.getById", "tok", group_id="mygroup")


def test_resolve_short_name_falls_back_to_resolve_screen_name():
    responses = iter([
        (None, {"error_code": 100, "error_msg": "invalid group_id"}),  # groups.getById
        ({"type": "event", "object_id": 7}, None),                      # resolveScreenName
        ({"groups": [{"id": 7, "name": "Встреча"}]}, None),             # имя
    ])
    with patch.object(bot, "_vk_call", side_effect=lambda *a, **k: next(responses)):
        assert bot.resolve_vk_group("tok", "vk.com/myevent") == (7, "Встреча", None, None)


def test_resolve_user_page_rejected():
    responses = iter([
        (None, {"error_code": 100, "error_msg": "invalid group_id"}),
        ({"type": "user", "object_id": 1}, None),
    ])
    with patch.object(bot, "_vk_call", side_effect=lambda *a, **k: next(responses)):
        group_id, _, error, _ = bot.resolve_vk_group("tok", "vk.com/durov")
    assert group_id is None
    assert "пользователя" in error


def test_resolve_invalid_token_reports_token_problem():
    with patch.object(bot, "_vk_call", return_value=(None, {"error_code": 5, "error_msg": "auth failed"})):
        group_id, _, error, _ = bot.resolve_vk_group("tok", "vk.com/mygroup")
    assert group_id is None
    assert "токен" in error


def test_resolve_rate_limit_not_reported_as_not_found():
    with patch.object(bot, "_vk_call", return_value=(None, {"error_code": 6, "error_msg": "Too many"})):
        group_id, _, error, _ = bot.resolve_vk_group("tok", "vk.com/mygroup")
    assert group_id is None
    assert "частоту" in error


def test_vk_call_retries_on_rate_limit(monkeypatch):
    monkeypatch.setattr(bot.time, "sleep", lambda s: None)
    with patch.object(bot.requests, "post", side_effect=[
        _mock_resp({"error": {"error_code": 6, "error_msg": "Too many"}}),
        _mock_resp({"response": [{"id": 1, "name": "G"}]}),
    ]) as post:
        response, err = bot._vk_call("groups.getById", "tok", group_id=1)
    assert err is None
    assert response == [{"id": 1, "name": "G"}]
    assert post.call_count == 2


def test_vk_call_does_not_retry_fatal_error(monkeypatch):
    monkeypatch.setattr(bot.time, "sleep", lambda s: None)
    with patch.object(bot.requests, "post", return_value=_mock_resp(
        {"error": {"error_code": 5, "error_msg": "auth"}}
    )) as post:
        response, err = bot._vk_call("groups.getById", "tok", group_id=1)
    assert response is None and err["error_code"] == 5
    assert post.call_count == 1


def test_vk_call_network_error(monkeypatch):
    monkeypatch.setattr(bot.time, "sleep", lambda s: None)
    with patch.object(bot.requests, "post", side_effect=_requests.exceptions.ConnectionError("boom")):
        response, err = bot._vk_call("groups.getById", "tok", group_id=1)
    assert response is None and err["error_code"] is None


# ─── Фильтры ссылок в сообщениях ──────────────────────────────────────────────

def test_url_filter_finds_link_inside_text():
    assert bot._URL_FILTER.filter(_msg("Смотри: https://youtu.be/abc !")) is True


def test_extract_platform_url_strips_trailing_punct():
    assert bot.extract_platform_url("вот https://vk.com/clip-1_2.") == ("https://vk.com/clip-1_2", "vk")


def test_url_filter_uses_hidden_text_links():
    m = _msg("видео тут")
    ent = MagicMock()
    ent.url = "https://www.tiktok.com/@u/video/1"
    m.entities = [ent]
    assert bot._URL_FILTER.filter(m) is True


@pytest.mark.parametrize("text", [
    "https://vk.com/mygroup",
    "https://vk.ru/club123",
    "добавь https://m.vk.com/public1",
])
def test_community_filter_accepts(text):
    assert bot._VK_COMMUNITY_FILTER.filter(_msg(text)) is True


@pytest.mark.parametrize("text", [
    "https://vk.com/video-1_2",
    "https://www.tiktok.com/@u/video/1",
    "https://notvk.com/mygroup",
    "https://oauth.vk.com/blank.html#access_token=vk1.a.xxx&expires_in=0",
    "просто текст",
])
def test_community_filter_rejects(text):
    assert not bot._VK_COMMUNITY_FILTER.filter(_msg(text))


# ─── «Сервер VK не отвечает» — сообщения пользователю ─────────────────────────

def _connect_timeout():
    return _requests.exceptions.ConnectTimeout(
        "HTTPSConnectionPool(host='api.vk.com', port=443): Max retries exceeded "
        "(Caused by ConnectTimeoutError('Connection to api.vk.com timed out.'))"
    )


def _wrapped_network_vkerror():
    try:
        try:
            raise _connect_timeout()
        except _requests.exceptions.RequestException as exc:
            raise bot.VKError(None, f"сетевая ошибка: {exc}", stage="VK video.save", network=True) from exc
    except bot.VKError as err:
        return err


@pytest.mark.parametrize("exc", [
    _requests.exceptions.ConnectTimeout("x"),
    _requests.exceptions.ReadTimeout("x"),
    _requests.exceptions.ConnectionError("x"),
    TimeoutError("x"),
    ConnectionResetError("x"),
    RuntimeError("ERROR: [vk] 1_2: Unable to download webpage: The read operation timed out"),
])
def test_is_network_error_true(exc):
    assert bot._is_network_error(exc) is True


def test_is_network_error_follows_cause_chain():
    assert bot._is_network_error(_wrapped_network_vkerror()) is True


@pytest.mark.parametrize("exc", [
    bot.VKError(15, "VK 15: Access denied", stage="VK wall.post"),
    bot.VKError(None, "сетевая ошибка: Expecting value", network=True),  # не-JSON ответ
    ValueError("Видео слишком длинное — 5:01. Максимум 3 минуты."),
    None,
])
def test_is_network_error_false(exc):
    assert bot._is_network_error(exc) is False


def test_format_error_vk_unreachable_is_friendly():
    text = bot._format_error(_wrapped_network_vkerror(), "VK video.save", "Моя группа", "tiktok")
    assert "Сервер VK не отвечает" in text
    assert "Моя группа" in text
    assert "HTTPSConnectionPool" not in text  # без технической простыни


def test_format_error_download_names_source_platform():
    exc = RuntimeError("Unable to download webpage: connection refused")
    text = bot._format_error(exc, "скачивание", "G", "tiktok")
    assert "Сервер TikTok не отвечает" in text


def test_format_error_regular_error_unchanged():
    exc = bot.VKError(100, "VK 100: One of the parameters is invalid", stage="VK wall.post")
    text = bot._format_error(exc, "VK wall.post", "G")
    assert "VK 100: One of the parameters is invalid" in text and "не отвечает" not in text


def test_vk_error_text_network():
    assert "Сервер VK не отвечает" in bot._vk_error_text({"error_code": None, "error_msg": "x"})


def test_resolve_numeric_id_reports_vk_unreachable():
    with patch.object(bot, "_vk_call", return_value=(None, {"error_code": None, "error_msg": "timeout"})):
        group_id, name, error, note = bot.resolve_vk_group("tok", "https://vk.com/club240977878")
    assert (group_id, name, error) == (240977878, None, None)
    assert "Сервер VK не отвечает" in note


def test_no_name_text_includes_note():
    text = bot._no_name_text(5, "🌐 Сервер VK не отвечает")
    assert "id 5" in text and "не отвечает" in text and "Введи название" in text


def test_upload_to_vk_skips_legacy_when_vk_unreachable(tmp_video):
    with patch.object(bot, "_upload_short_video", side_effect=_wrapped_network_vkerror()), \
         patch.object(bot, "_upload_video_legacy") as legacy:
        with pytest.raises(bot.VKError):
            bot.upload_to_vk("tok", 1, tmp_video, "t", "")
    legacy.assert_not_called()


async def test_publish_to_vk_notifies_user_on_retry(monkeypatch):
    monkeypatch.setattr(bot, "VK_PUBLISH_RETRIES", 2)
    monkeypatch.setattr(bot, "VK_RETRY_BASE_DELAY", 0)
    monkeypatch.setattr(bot.random, "uniform", lambda a, b: 0)
    calls = iter([_wrapped_network_vkerror(), None])

    def fake_upload(*a, **k):
        exc = next(calls)
        if exc:
            raise exc

    on_retry = AsyncMock()
    with patch.object(bot, "upload_to_vk", side_effect=fake_upload):
        await bot._publish_to_vk(999001, "tok", 1, "f", "t", "", on_retry=on_retry)
    on_retry.assert_awaited_once()
    assert "Сервер VK не отвечает" in on_retry.await_args.args[0]
    assert "2 из 2" in on_retry.await_args.args[0]


async def test_on_error_notifies_user(monkeypatch):
    monkeypatch.setattr(bot, "_record_error", lambda *a, **k: None)
    update = MagicMock(spec=bot.Update)
    update.effective_chat.id = 42
    update.effective_user.id = 7
    update.callback_query = None
    context = MagicMock()
    context.error = _connect_timeout()
    context.bot.send_message = AsyncMock()
    await bot._on_error(update, context)
    chat_id, text = context.bot.send_message.await_args.args
    assert chat_id == 42 and "Сервер VK не отвечает" in text


async def test_on_error_ignores_telegram_network_errors():
    from telegram.error import TimedOut
    context = MagicMock()
    context.error = TimedOut()
    context.bot.send_message = AsyncMock()
    await bot._on_error(MagicMock(spec=bot.Update), context)
    context.bot.send_message.assert_not_awaited()


# ─── Надёжность публикации: отмена, дубли, черновики ──────────────────────────

import threading as _threading
from telegram.error import BadRequest as _BadRequest, TimedOut as _TimedOut, Forbidden as _Forbidden


@pytest.fixture
def tmp_db(tmp_path, monkeypatch):
    import db
    monkeypatch.setattr(db, "DB_PATH", str(tmp_path / "t.db"))
    db.init_db()
    return db


def test_check_cancel_raises_when_set():
    ev = _threading.Event()
    bot._check_cancel(ev)  # не взведён — ок
    bot._check_cancel(None)
    ev.set()
    with pytest.raises(bot.UploadCancelled):
        bot._check_cancel(ev)


def test_short_video_cancel_before_publish_deletes_draft(tmp_video):
    ev = _threading.Event()

    def upload(*a, **k):
        ev.set()  # пользователь нажал «Отменить» во время загрузки файла
        return _mock_resp(_UPLOAD_OK)

    with _vk_mock(create_rv=_CREATE_RESP) as vk, \
         patch("bot.requests.post", side_effect=upload), \
         patch.object(bot, "_delete_vk_video") as delete:
        with pytest.raises(bot.UploadCancelled):
            bot._upload_short_video("tok", 100, tmp_video, "", cancel_event=ev)
    vk.shortVideo.publish.assert_not_called()
    delete.assert_called_once_with("tok", -100, 42)


def test_short_video_publish_network_error_is_ambiguous_and_keeps_video(tmp_video):
    with _vk_mock(create_rv=_CREATE_RESP, publish_se=_requests.exceptions.ReadTimeout("timed out")), \
         patch("bot.requests.post", return_value=_mock_resp(_UPLOAD_OK)), \
         patch.object(bot, "_delete_vk_video") as delete:
        with pytest.raises(bot.VKError) as ei:
            bot._upload_short_video("tok", 100, tmp_video, "")
    assert ei.value.ambiguous is True
    delete.assert_not_called()  # клип мог уже выйти — не трогаем


def test_short_video_edit_error_deletes_draft(tmp_video):
    with _vk_mock(create_rv=_CREATE_RESP, edit_se=_api_err(15)), \
         patch("bot.requests.post", return_value=_mock_resp(_UPLOAD_OK)), \
         patch.object(bot, "_delete_vk_video") as delete:
        with pytest.raises(bot.VKError):
            bot._upload_short_video("tok", 100, tmp_video, "desc")
    delete.assert_called_once()


def test_short_video_unexpected_create_response(tmp_video):
    with _vk_mock(create_rv={"something": "else"}):
        with pytest.raises(bot.VKError) as ei:
            bot._upload_short_video("tok", 100, tmp_video, "")
    assert "неожиданный ответ" in str(ei.value)
    assert ei.value.network is False


def test_legacy_unexpected_save_response(tmp_video):
    with patch("bot.requests.post", return_value=_mock_resp({"response": {}})):
        with pytest.raises(bot.VKError) as ei:
            bot._upload_video_legacy("tok", 100, tmp_video, "t", "")
    assert "неожиданный ответ" in str(ei.value)


def test_legacy_wall_post_network_error_is_ambiguous(tmp_video):
    with patch("bot.requests.post", side_effect=[
        _mock_resp(_SAVE_OK), _mock_resp({}), _requests.exceptions.ReadTimeout("timed out"),
    ]), patch.object(bot, "_delete_vk_video") as delete:
        with pytest.raises(bot.VKError) as ei:
            bot._upload_video_legacy("tok", 100, tmp_video, "t", "")
    assert ei.value.ambiguous is True
    delete.assert_not_called()


def test_upload_to_vk_no_fallback_when_ambiguous(tmp_video):
    err = bot.VKError(None, "x", stage="VK shortVideo.publish", network=True, ambiguous=True)
    with patch.object(bot, "_upload_short_video", side_effect=err), \
         patch.object(bot, "_upload_video_legacy") as legacy:
        with pytest.raises(bot.VKError):
            bot.upload_to_vk("tok", 1, tmp_video, "t", "")
    legacy.assert_not_called()


async def test_publish_to_vk_does_not_retry_ambiguous(monkeypatch):
    monkeypatch.setattr(bot, "VK_PUBLISH_RETRIES", 3)
    err = bot.VKError(None, "x", network=True, ambiguous=True)
    with patch.object(bot, "upload_to_vk", side_effect=err) as up:
        with pytest.raises(bot.VKError):
            await bot._publish_to_vk(999002, "tok", 1, "f", "t", "")
    assert up.call_count == 1


def test_format_error_ambiguous():
    err = bot.VKError(None, "x", network=True, ambiguous=True)
    text = bot._format_error(err, "VK shortVideo.publish", "Группа")
    assert "проверь группу" in text


@pytest.mark.parametrize("code,needle", [(5, "токен"), (15, "администратор"), (14, "капч")])
def test_format_error_known_vk_codes(code, needle):
    text = bot._format_error(bot.VKError(code, f"VK {code}: x"), "VK wall.post", "G")
    assert needle in text


# ─── Статусы и уведомления не обрывают публикацию ─────────────────────────────

async def test_do_upload_survives_status_edit_failures(monkeypatch, tmp_path):
    f = tmp_path / "v.mp4"
    f.write_bytes(b"x")
    monkeypatch.setattr(bot, "_download_video", AsyncMock(return_value=(str(f), "t")))
    publish = AsyncMock()
    monkeypatch.setattr(bot, "_publish_to_vk", publish)
    status = MagicMock()
    status.edit_text = AsyncMock(side_effect=_TimedOut())
    context = MagicMock()
    context.bot.send_message = AsyncMock(side_effect=_TimedOut())
    job = {"url": "u", "platform": "tiktok", "vk_token": "t", "vk_group_id": 1, "vk_group_name": "G"}
    await bot.do_upload(1, context, job, status_message=status)
    publish.assert_awaited_once()
    assert not f.exists()


async def test_scheduled_job_publishes_even_if_user_blocked_bot(monkeypatch, tmp_path):
    f = tmp_path / "v.mp4"
    f.write_bytes(b"x")
    monkeypatch.setattr(bot.db, "get_vk_token", lambda tid: "tok")
    deleted = []
    monkeypatch.setattr(bot.db, "delete_scheduled_post", deleted.append)
    monkeypatch.setattr(bot, "_download_video", AsyncMock(return_value=(str(f), "t")))
    publish = AsyncMock()
    monkeypatch.setattr(bot, "_publish_to_vk", publish)
    context = MagicMock()
    context.bot.send_message = AsyncMock(side_effect=_Forbidden("bot was blocked by the user"))
    context.job.data = {"chat_id": 1, "url": "u", "platform": "tiktok",
                        "vk_group_id": 5, "vk_group_name": "G", "scheduled_post_id": 77}
    await bot._scheduled_upload_job(context)
    publish.assert_awaited_once()
    assert deleted == [77]


async def test_scheduled_job_keeps_row_if_stopped_while_downloading(monkeypatch):
    monkeypatch.setattr(bot.db, "get_vk_token", lambda tid: "tok")
    deleted = []
    monkeypatch.setattr(bot.db, "delete_scheduled_post", deleted.append)
    monkeypatch.setattr(bot, "_download_video", AsyncMock(side_effect=asyncio.CancelledError()))
    context = MagicMock()
    context.bot.send_message = AsyncMock()
    context.job.data = {"chat_id": 1, "url": "u", "platform": "tiktok",
                        "vk_group_id": 5, "vk_group_name": "G", "scheduled_post_id": 78}
    with pytest.raises(asyncio.CancelledError):
        await bot._scheduled_upload_job(context)
    assert deleted == []  # восстановится после рестарта


async def test_scheduled_job_drops_row_if_stopped_while_publishing(monkeypatch, tmp_path):
    f = tmp_path / "v.mp4"
    f.write_bytes(b"x")
    monkeypatch.setattr(bot.db, "get_vk_token", lambda tid: "tok")
    deleted = []
    monkeypatch.setattr(bot.db, "delete_scheduled_post", deleted.append)
    monkeypatch.setattr(bot, "_download_video", AsyncMock(return_value=(str(f), "t")))
    monkeypatch.setattr(bot, "_publish_to_vk", AsyncMock(side_effect=asyncio.CancelledError()))
    context = MagicMock()
    context.bot.send_message = AsyncMock()
    context.job.data = {"chat_id": 1, "url": "u", "platform": "tiktok",
                        "vk_group_id": 5, "vk_group_name": "G", "scheduled_post_id": 79}
    with pytest.raises(asyncio.CancelledError):
        await bot._scheduled_upload_job(context)
    assert deleted == [79]  # ролик мог выйти — повтор после рестарта дал бы дубль


# ─── Безобидные ошибки Telegram, устаревшие кнопки ────────────────────────────

@pytest.mark.parametrize("msg,expected", [
    ("Message is not modified: specified new message content ...", True),
    ("Query is too old and response timeout expired or query id is invalid", True),
    ("Text must be non-empty", False),
])
def test_is_harmless_telegram_error(msg, expected):
    assert bot._is_harmless_telegram_error(_BadRequest(msg)) is expected


async def test_safe_answer_swallows_old_query():
    q = MagicMock()
    q.answer = AsyncMock(side_effect=_BadRequest("Query is too old"))
    await bot._safe_answer(q)  # не бросает


async def test_on_error_ignores_not_modified():
    context = MagicMock()
    context.error = _BadRequest("Message is not modified")
    context.bot.send_message = AsyncMock()
    await bot._on_error(MagicMock(spec=bot.Update), context)
    context.bot.send_message.assert_not_awaited()


async def test_on_error_reports_real_bad_request(monkeypatch):
    monkeypatch.setattr(bot, "_record_error", lambda *a, **k: None)
    update = MagicMock(spec=bot.Update)
    update.effective_chat.id = 3
    update.callback_query = None
    context = MagicMock()
    context.error = _BadRequest("Text must be non-empty")
    context.bot.send_message = AsyncMock()
    await bot._on_error(update, context)
    context.bot.send_message.assert_awaited_once()


async def test_stale_callback_is_answered():
    update = MagicMock()
    update.callback_query.answer = AsyncMock()
    await bot.handle_stale_callback(update, MagicMock())
    assert "устарела" in update.callback_query.answer.await_args.args[0]


# ─── Ввод: названия, владельцы, токен ─────────────────────────────────────────

@pytest.mark.parametrize("text,ok", [
    ("Моя группа", True), ("  много   пробелов  ", True), ("   ", False), ("", False), (None, False),
    ("x" * 61, False), ("x" * 60, True),
])
def test_clean_name(text, ok):
    name, error = bot._clean_name(text)
    assert (name is not None) is ok and (error is None) is ok


def test_clean_name_collapses_spaces():
    assert bot._clean_name("  много   пробелов  ")[0] == "много пробелов"


def test_group_label_fallback_for_empty_name(tmp_db):
    tmp_db.ensure_user(1)
    tmp_db.add_group(1, 555, "   ")
    labels = [b.text for row in bot.build_groups_select_keyboard(1).inline_keyboard for b in row]
    assert labels == ["Группа 555"]


def test_own_group_checks_owner(tmp_db):
    tmp_db.ensure_user(1)
    tmp_db.ensure_user(2)
    tmp_db.add_group(1, 100, "A")
    row_id = tmp_db.get_groups(1)[0]["id"]
    assert bot._own_group(row_id, 1) is not None
    assert bot._own_group(row_id, 2) is None


async def test_foreign_group_delete_is_ignored(tmp_db):
    tmp_db.ensure_user(1)
    tmp_db.ensure_user(2)
    tmp_db.add_group(1, 100, "A")
    row_id = tmp_db.get_groups(1)[0]["id"]
    update = MagicMock()
    update.effective_user.id = 2  # чужой пользователь
    update.callback_query.data = f"g_del_{row_id}"
    update.callback_query.answer = AsyncMock()
    update.callback_query.edit_message_text = AsyncMock()
    await bot.groups_button(update, MagicMock())
    assert len(tmp_db.get_groups(1)) == 1


async def test_desc_choice_deleted_template_reprompts(tmp_db):
    tmp_db.ensure_user(1)
    update = MagicMock()
    update.effective_user.id = 1
    update.callback_query.data = "updesc_tpl_12345"
    update.callback_query.answer = AsyncMock()
    update.callback_query.edit_message_text = AsyncMock()
    context = MagicMock()
    context.user_data = {}
    assert await bot.handle_desc_choice(update, context) == bot.UP_DESC
    assert "description" not in context.user_data


def _token_update(text):
    u = MagicMock()
    u.effective_user.id = 1
    u.message.text = text
    u.message.entities = []
    u.message.reply_text = AsyncMock()
    return u


async def test_handle_token_rejects_invalid_token(tmp_db):
    tmp_db.ensure_user(1)
    u = _token_update("vk1.a." + "A" * 60)
    with patch.object(bot, "_vk_call", return_value=(None, {"error_code": 5, "error_msg": "auth"})):
        state = await bot.handle_token(u, MagicMock())
    assert state == bot.TOKEN_WAIT
    assert tmp_db.get_vk_token(1) is None


async def test_handle_token_saves_when_vk_unreachable(tmp_db):
    tmp_db.ensure_user(1)
    tok = "vk1.a." + "B" * 60
    u = _token_update(tok)
    with patch.object(bot, "_vk_call", return_value=(None, {"error_code": None, "error_msg": "timeout"})):
        await bot.handle_token(u, MagicMock())
    assert tmp_db.get_vk_token(1) == tok
    assert "не отвечает" in u.message.reply_text.await_args.args[0]


async def test_handle_token_valid(tmp_db):
    tmp_db.ensure_user(1)
    u = _token_update("vk1.a." + "C" * 60)
    with patch.object(bot, "_vk_call", return_value=([{"first_name": "Иван", "last_name": "Петров"}], None)):
        await bot.handle_token(u, MagicMock())
    assert "Иван Петров" in u.message.reply_text.await_args.args[0]


async def test_rename_rejects_empty_name(tmp_db):
    tmp_db.ensure_user(1)
    tmp_db.add_group(1, 100, "A")
    u = _token_update("   ")
    context = MagicMock()
    context.user_data = {"rename_group_id": tmp_db.get_groups(1)[0]["id"]}
    assert await bot.groups_rename(u, context) == bot.G_RENAME
    assert tmp_db.get_groups(1)[0]["name"] == "A"


# ─── Таймауты подключения к VK и общий предел поиска ──────────────────────────

import time as _time_mod


def test_timeout_session_sets_default_timeout():
    with patch("requests.Session.request", return_value="ok") as req:
        bot._TimeoutSession().post("https://api.vk.com/method/x", data={})
    assert req.call_args.kwargs["timeout"] == (bot.VK_CONNECT_TIMEOUT, 60)


def test_timeout_session_keeps_explicit_timeout():
    with patch("requests.Session.request", return_value="ok") as req:
        bot._TimeoutSession().get("https://x", timeout=5)
    assert req.call_args.kwargs["timeout"] == 5


def test_short_video_uses_session_with_timeout(tmp_video):
    with patch("vk_api.VkApi") as cls:
        cls.return_value.get_api.return_value.shortVideo.create.side_effect = _api_err(15)
        with pytest.raises(bot.VKError):
            bot._upload_short_video("tok", 1, tmp_video, "")
    assert isinstance(cls.call_args.kwargs["session"], bot._TimeoutSession)


def test_vk_call_uses_short_connect_timeout():
    with patch.object(bot.requests, "post", return_value=_mock_resp({"response": 1})) as post:
        bot._vk_call("users.get", "tok")
    assert post.call_args.kwargs["timeout"][0] == bot.VK_CONNECT_TIMEOUT


async def test_lookup_group_answers_within_deadline(monkeypatch):
    monkeypatch.setattr(bot, "VK_LOOKUP_DEADLINE", 0.2)
    monkeypatch.setattr(bot.db, "get_vk_token", lambda uid: "tok")
    monkeypatch.setattr(bot, "resolve_vk_group", lambda *a: _time_mod.sleep(1.5))
    u = MagicMock()
    u.effective_user.id = 1
    u.message.text = "https://vk.ru/imfather"
    u.message.entities = []
    u.message.reply_text = AsyncMock()
    started = _time_mod.monotonic()
    group_id, _, error, _ = await bot._lookup_group(u, MagicMock())
    assert _time_mod.monotonic() - started < 1.0
    assert group_id is None and "не отвечает" in error
    assert "Ищу" in u.message.reply_text.await_args_list[0].args[0]
