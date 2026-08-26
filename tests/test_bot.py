"""Тесты чистых функций из bot.py — без реального Telegram-соединения."""

import asyncio
from datetime import date, timedelta
from unittest.mock import MagicMock, patch, AsyncMock

import pytest

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
