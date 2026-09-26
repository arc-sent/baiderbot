"""Тесты downloader.py — чистые функции без сетевых запросов."""

import pytest
from downloader import (
    detect_platform,
    _check_duration,
    _find_mp4,
    _find_duration,
    _extract_vk_ids,
    _to_vkcom,
    _og_title,
)


# ─── detect_platform ──────────────────────────────────────────────────────────

@pytest.mark.parametrize("url,expected", [
    # TikTok
    ("https://www.tiktok.com/@user/video/123456789", "tiktok"),
    ("https://vm.tiktok.com/ZGd9V3VWS/", "tiktok"),
    ("https://vt.tiktok.com/ZSYabcdef/", "tiktok"),
    ("http://tiktok.com/t/abc", "tiktok"),
    # Likee
    ("https://likee.video/v/AbCdEf", "likee"),
    ("https://l.likee.video/v/AbCdEf", "likee"),
    # YouTube
    ("https://www.youtube.com/shorts/dQw4w9WgXcQ", "youtube"),
    ("https://youtube.com/shorts/abc", "youtube"),
    ("https://youtu.be/dQw4w9WgXcQ", "youtube"),
    ("https://www.youtube.com/watch?v=abc", "youtube"),
    ("https://m.youtube.com/watch?v=abc", "youtube"),
    ("https://www.youtube.com/embed/abc", "youtube"),
    ("https://www.youtube.com/v/abc", "youtube"),
    ("https://www.youtube.com/live/abc", "youtube"),
    # VK
    ("https://vk.com/video-123456_789012", "vk"),
    ("https://vk.com/clip-100_200", "vk"),
    ("https://vk.com/clips-100_200", "vk"),
    ("https://www.vk.com/video123_456", "vk"),
    ("https://m.vk.com/video-123_456", "vk"),
    ("https://vk.ru/video-123_456", "vk"),
    ("https://vkvideo.ru/video-123_456", "vk"),
    ("https://vkvideo.ru/clip-123_456", "vk"),
    ("https://vk.com/clips/mygroup?z=clip-123_456", "vk"),
    ("https://vk.com/feed?z=video-123_456%2Fabc", "vk"),
    # Instagram
    ("https://instagram.com/reel/abc", "instagram"),
    ("https://www.instagram.com/reels/abc/", "instagram"),
    # Нераспознанные
    ("https://google.com", None),
    ("https://instagram.com/someuser", None),
    ("https://vk.com/mygroup", None),
    ("https://vk.com/clubsomething", None),
    ("123123123", None),
    ("просто текст", None),
    ("", None),
    ("https://notvk.com/video123", None),
])
def test_detect_platform(url, expected):
    assert detect_platform(url) == expected


# ─── _check_duration ──────────────────────────────────────────────────────────

def test_check_duration_within_limit():
    _check_duration(120)  # 2 минуты — ок


def test_check_duration_exact_limit():
    _check_duration(180)  # ровно 3 минуты — ок


def test_check_duration_none():
    _check_duration(None)  # неизвестная длина — ок


def test_check_duration_zero():
    _check_duration(0)  # 0 — не превышает лимит


def test_check_duration_too_long():
    with pytest.raises(ValueError, match="слишком длинное"):
        _check_duration(181)


def test_check_duration_very_long():
    with pytest.raises(ValueError, match="слишком длинное"):
        _check_duration(3600)


def test_check_duration_error_shows_time():
    with pytest.raises(ValueError, match="5:01"):
        _check_duration(301)


# ─── _find_mp4 ────────────────────────────────────────────────────────────────

def test_find_mp4_picks_highest_quality():
    text = '"mp4_480":"https://cdn/480.mp4","mp4_1080":"https://cdn/1080.mp4","mp4_360":"https://cdn/360.mp4"'
    url, quality = _find_mp4(text)
    assert quality == 1080
    assert "1080.mp4" in url


def test_find_mp4_fallback_to_lower():
    text = '"mp4_360":"https://cdn/360.mp4"'
    url, quality = _find_mp4(text)
    assert quality == 360


def test_find_mp4_url_format_unescaped():
    text = '"mp4_720":"https:\\/\\/cdn.vk.com\\/video720.mp4"'
    url, quality = _find_mp4(text)
    assert quality == 720
    assert "https://cdn.vk.com/video720.mp4" == url


def test_find_mp4_not_found():
    assert _find_mp4("no video data here") is None


def test_find_mp4_empty():
    assert _find_mp4("") is None


# ─── _find_duration ───────────────────────────────────────────────────────────

def test_find_duration_found():
    assert _find_duration('"duration":42') == 42


def test_find_duration_with_spaces():
    assert _find_duration('"duration" : 120') == 120


def test_find_duration_not_found():
    assert _find_duration("no duration field here") is None


def test_find_duration_empty():
    assert _find_duration("") is None


# ─── _extract_vk_ids ──────────────────────────────────────────────────────────

@pytest.mark.parametrize("url,expected", [
    ("https://vk.com/video-123456_789012", ("-123456", "789012")),
    ("https://vk.com/clip-100_200", ("-100", "200")),
    ("https://vk.com/video123_456", ("123", "456")),
    ("https://vk.com/profile", None),
    ("https://vk.com/wall-123_456", None),
])
def test_extract_vk_ids(url, expected):
    assert _extract_vk_ids(url) == expected


# ─── _to_vkcom ────────────────────────────────────────────────────────────────

def test_to_vkcom_vkru():
    assert _to_vkcom("https://vk.ru/video123") == "https://vk.com/video123"


def test_to_vkcom_mobile():
    assert _to_vkcom("https://m.vk.com/video123") == "https://vk.com/video123"


def test_to_vkcom_vkvideo():
    assert _to_vkcom("https://vkvideo.ru/video-1_2") == "https://vk.com/video-1_2"


def test_to_vkcom_mobile_vkru():
    assert _to_vkcom("https://m.vk.ru/video-1_2") == "https://vk.com/video-1_2"


def test_to_vkcom_desktop_unchanged():
    url = "https://vk.com/video-123_456"
    assert _to_vkcom(url) == url


def test_to_vkcom_www_unchanged():
    url = "https://www.vk.com/video123"
    assert _to_vkcom(url) == url


# ─── _og_title ────────────────────────────────────────────────────────────────

def test_og_title_property_first():
    html = '<meta property="og:title" content="My Video Title">'
    assert _og_title(html) == "My Video Title"


def test_og_title_content_first():
    html = '<meta content="Another Title" property="og:title">'
    assert _og_title(html) == "Another Title"


def test_og_title_not_found():
    assert _og_title("<html><body>no meta</body></html>") == ""


def test_og_title_strips_whitespace():
    html = '<meta property="og:title" content="  Trimmed Title  ">'
    assert _og_title(html) == "Trimmed Title"
