"""Тесты downloader.py — чистые функции без сетевых запросов."""

import asyncio
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


# ─── Понятные ошибки yt-dlp, проверка потока, очистка временных файлов ────────

import os as _os
import time as _time
from unittest.mock import MagicMock as _MagicMock

import downloader as _dl


@pytest.mark.parametrize("msg,needle", [
    ("ERROR: [youtube] abc: Private video. Sign in if you've been granted access", "приватное"),
    ("ERROR: [youtube] abc: Sign in to confirm you’re not a bot", "не бот"),
    ("ERROR: [youtube] abc: Sign in to confirm your age", "18+"),
    ("ERROR: [Instagram] abc: Requested content is not available, rate-limit reached or login required", "вход"),
    ("ERROR: [TikTok] 123: HTTP Error 429: Too Many Requests", "ограничил"),
    ("ERROR: [youtube] abc: Video unavailable", "не найдено"),
])
def test_friendly_ytdlp_error(msg, needle):
    err = _dl._friendly_ytdlp_error(Exception(msg), "YouTube")
    assert err is not None and needle in str(err)


@pytest.mark.parametrize("msg", [
    "ERROR: Unable to download webpage: The read operation timed out",
    "ERROR: [tiktok] x: Unable to download: <urlopen error [Errno 111] Connection refused>",
    "ERROR: something completely unknown",
])
def test_friendly_ytdlp_error_keeps_network_and_unknown(msg):
    assert _dl._friendly_ytdlp_error(Exception(msg), "TikTok") is None


def _stream_resp(chunks, ctype="video/mp4", length=None):
    r = _MagicMock()
    r.headers = {"Content-Type": ctype}
    if length is not None:
        r.headers["Content-Length"] = str(length)
    r.iter_content.return_value = iter(chunks)
    return r


def test_save_stream_ok(tmp_path):
    out = tmp_path / "v.mp4"
    _dl._save_stream(_stream_resp([b"ab", b"cd"]), str(out), "VK")
    assert out.read_bytes() == b"abcd"


def test_save_stream_rejects_html(tmp_path):
    out = tmp_path / "v.mp4"
    with pytest.raises(ValueError, match="веб-страница"):
        _dl._save_stream(_stream_resp([b"<html>"], ctype="text/html; charset=utf-8"), str(out), "VK")
    assert not out.exists()


def test_save_stream_rejects_too_big_by_header(tmp_path, monkeypatch):
    monkeypatch.setattr(_dl, "MAX_VIDEO_BYTES", 10)
    with pytest.raises(ValueError, match="слишком большой"):
        _dl._save_stream(_stream_resp([b"x"], length=11), str(tmp_path / "v.mp4"), "VK")


def test_save_stream_rejects_too_big_while_streaming(tmp_path, monkeypatch):
    monkeypatch.setattr(_dl, "MAX_VIDEO_BYTES", 10)
    out = tmp_path / "v.mp4"
    with pytest.raises(ValueError, match="слишком большой"):
        _dl._save_stream(_stream_resp([b"x" * 6, b"x" * 6]), str(out), "VK")
    assert not out.exists()


def test_save_stream_rejects_empty(tmp_path):
    out = tmp_path / "v.mp4"
    with pytest.raises(ValueError, match="пустой"):
        _dl._save_stream(_stream_resp([]), str(out), "Likee")
    assert not out.exists()


def test_cleanup_tmp_files(tmp_path, monkeypatch):
    monkeypatch.setattr(_dl, "_TMP_ROOT", str(tmp_path))
    old_file = tmp_path / "old.mp4"
    old_file.write_bytes(b"x")
    old_dir = tmp_path / "tiktok_dir_old"
    old_dir.mkdir()
    (old_dir / "a.part").write_bytes(b"x")
    fresh = tmp_path / "fresh.mp4"
    fresh.write_bytes(b"x")
    long_ago = _time.time() - 10 * 3600
    for p in (old_file, old_dir, old_dir / "a.part"):
        _os.utime(p, (long_ago, long_ago))

    assert _dl.cleanup_tmp_files(6 * 3600) == 2
    assert not old_file.exists() and not old_dir.exists()
    assert fresh.exists()


def test_cleanup_tmp_files_missing_dir(tmp_path, monkeypatch):
    monkeypatch.setattr(_dl, "_TMP_ROOT", str(tmp_path / "nope"))
    assert _dl.cleanup_tmp_files(1) == 0


# ─── TT_PROXY: запасной прокси для TikTok — только при ошибке ─────────────────

def test_download_tiktok_no_retry_when_direct_succeeds(monkeypatch):
    monkeypatch.setattr(_dl, "TT_PROXY", "socks5h://u:p@1.2.3.4:1080")
    calls = []

    def fake_sync(url, save_path, prefix, default_title, proxy=None):
        calls.append(proxy)
        return "/tmp/x.mp4", "T"

    monkeypatch.setattr(_dl, "_download_ytdlp_sync", fake_sync)
    result = asyncio.run(_dl.download_tiktok("https://tiktok.com/@u/video/1"))
    assert result == ("/tmp/x.mp4", "T")
    assert calls == [None]  # ни разу не понадобился прокси


def test_download_tiktok_no_retry_without_tt_proxy(monkeypatch):
    monkeypatch.setattr(_dl, "TT_PROXY", None)
    calls = []

    def fake_sync(url, save_path, prefix, default_title, proxy=None):
        calls.append(proxy)
        raise RuntimeError("boom")

    monkeypatch.setattr(_dl, "_download_ytdlp_sync", fake_sync)
    with pytest.raises(RuntimeError, match="boom"):
        asyncio.run(_dl.download_tiktok("https://tiktok.com/@u/video/1"))
    assert calls == [None]  # без TT_PROXY повтора нет


def test_download_tiktok_retries_via_proxy_on_error(monkeypatch):
    monkeypatch.setattr(_dl, "TT_PROXY", "socks5h://u:p@1.2.3.4:1080")
    calls = []

    def fake_sync(url, save_path, prefix, default_title, proxy=None):
        calls.append(proxy)
        if proxy is None:
            raise RuntimeError("прямое скачивание упало")
        return "/tmp/via_proxy.mp4", "T"

    monkeypatch.setattr(_dl, "_download_ytdlp_sync", fake_sync)
    result = asyncio.run(_dl.download_tiktok("https://tiktok.com/@u/video/1"))
    assert result == ("/tmp/via_proxy.mp4", "T")
    assert calls == [None, "socks5h://u:p@1.2.3.4:1080"]


def test_download_tiktok_raises_if_proxy_retry_also_fails(monkeypatch):
    monkeypatch.setattr(_dl, "TT_PROXY", "socks5h://u:p@1.2.3.4:1080")

    def fake_sync(url, save_path, prefix, default_title, proxy=None):
        raise RuntimeError(f"упало (proxy={proxy})")

    monkeypatch.setattr(_dl, "_download_ytdlp_sync", fake_sync)
    with pytest.raises(RuntimeError, match=r"proxy=socks5h"):
        asyncio.run(_dl.download_tiktok("https://tiktok.com/@u/video/1"))


def test_other_platforms_never_use_tt_proxy(monkeypatch):
    """youtube/instagram не должны знать о TT_PROXY вообще."""
    monkeypatch.setattr(_dl, "TT_PROXY", "socks5h://u:p@1.2.3.4:1080")
    calls = []

    def fake_sync(url, save_path, prefix, default_title, proxy=None):
        calls.append((prefix, proxy))
        raise RuntimeError("boom")

    monkeypatch.setattr(_dl, "_download_ytdlp_sync", fake_sync)
    with pytest.raises(RuntimeError):
        asyncio.run(_dl.download_youtube("https://youtube.com/shorts/1"))
    assert calls == [("youtube", None)]  # один вызов, без повтора и без прокси
