"""Тесты слоя db.py — работа с SQLite без внешних зависимостей."""

import time
import pytest
import db


@pytest.fixture(autouse=True)
def isolated_db(tmp_path, monkeypatch):
    """Каждый тест получает чистую временную БД."""
    monkeypatch.setattr(db, "DB_PATH", str(tmp_path / "test.db"))
    db.init_db()


# ─── Пользователи ─────────────────────────────────────────────────────────────

def test_ensure_user_creates_row():
    db.ensure_user(1)
    assert db.get_vk_token(1) is None


def test_ensure_user_idempotent():
    db.ensure_user(1)
    db.ensure_user(1)
    assert db.get_vk_token(1) is None


def test_set_get_token():
    db.ensure_user(1)
    db.set_vk_token(1, "vk1.a.testtoken")
    assert db.get_vk_token(1) == "vk1.a.testtoken"


def test_clear_token():
    db.ensure_user(1)
    db.set_vk_token(1, "vk1.a.testtoken")
    db.clear_vk_token(1)
    assert db.get_vk_token(1) is None


def test_token_update_on_conflict():
    db.ensure_user(1)
    db.set_vk_token(1, "vk1.a.first")
    db.set_vk_token(1, "vk1.a.second")
    assert db.get_vk_token(1) == "vk1.a.second"


def test_tokens_isolated_between_users():
    db.ensure_user(1)
    db.ensure_user(2)
    db.set_vk_token(1, "vk1.a.aaa")
    assert db.get_vk_token(2) is None


# ─── Группы ───────────────────────────────────────────────────────────────────

def test_add_and_get_groups():
    db.ensure_user(1)
    db.add_group(1, 100, "Alpha")
    db.add_group(1, 200, "Beta")
    groups = db.get_groups(1)
    assert len(groups) == 2
    names = {g["name"] for g in groups}
    assert names == {"Alpha", "Beta"}


def test_add_group_upserts_name():
    db.ensure_user(1)
    db.add_group(1, 100, "Old")
    db.add_group(1, 100, "New")
    groups = db.get_groups(1)
    assert len(groups) == 1
    assert groups[0]["name"] == "New"


def test_count_groups_empty():
    db.ensure_user(1)
    assert db.count_groups(1) == 0


def test_count_groups():
    db.ensure_user(1)
    db.add_group(1, 1, "A")
    db.add_group(1, 2, "B")
    db.add_group(1, 3, "C")
    assert db.count_groups(1) == 3


def test_count_groups_isolated_by_user():
    db.ensure_user(1)
    db.ensure_user(2)
    db.add_group(1, 1, "A")
    db.add_group(2, 2, "B")
    db.add_group(2, 3, "C")
    assert db.count_groups(1) == 1
    assert db.count_groups(2) == 2


def test_get_group_by_id():
    db.ensure_user(1)
    db.add_group(1, 100, "MyGroup")
    g = db.get_groups(1)[0]
    fetched = db.get_group(g["id"])
    assert fetched["name"] == "MyGroup"
    assert fetched["vk_group_id"] == 100


def test_get_group_missing_returns_none():
    assert db.get_group(99999) is None


def test_rename_group():
    db.ensure_user(1)
    db.add_group(1, 100, "Old")
    g = db.get_groups(1)[0]
    db.rename_group(g["id"], "New")
    assert db.get_group(g["id"])["name"] == "New"


def test_delete_group():
    db.ensure_user(1)
    db.add_group(1, 100, "G")
    g = db.get_groups(1)[0]
    db.delete_group(g["id"])
    assert db.get_groups(1) == []


def test_groups_not_shared_between_users():
    db.ensure_user(1)
    db.ensure_user(2)
    db.add_group(1, 100, "G1")
    assert db.get_groups(2) == []


# ─── Заготовки описаний ───────────────────────────────────────────────────────

def test_add_and_get_template():
    db.ensure_user(1)
    db.add_template(1, "Шаблон", "Текст шаблона")
    templates = db.get_templates(1)
    assert len(templates) == 1
    assert templates[0]["title"] == "Шаблон"
    assert templates[0]["body"] == "Текст шаблона"


def test_get_template_by_id():
    db.ensure_user(1)
    db.add_template(1, "T", "B")
    t = db.get_templates(1)[0]
    fetched = db.get_template(t["id"])
    assert fetched["body"] == "B"


def test_update_template():
    db.ensure_user(1)
    db.add_template(1, "T", "B")
    t = db.get_templates(1)[0]
    db.update_template(t["id"], "T2", "B2")
    updated = db.get_template(t["id"])
    assert updated["title"] == "T2"
    assert updated["body"] == "B2"


def test_delete_template():
    db.ensure_user(1)
    db.add_template(1, "T", "B")
    t = db.get_templates(1)[0]
    db.delete_template(t["id"])
    assert db.get_templates(1) == []


def test_templates_isolated_by_user():
    db.ensure_user(1)
    db.ensure_user(2)
    db.add_template(1, "T", "B")
    assert db.get_templates(2) == []


# ─── Отложенные публикации ────────────────────────────────────────────────────

def _make_post(**kwargs):
    defaults = dict(
        telegram_id=1,
        chat_id=10,
        url="https://vm.tiktok.com/test",
        platform="tiktok",
        description="",
        vk_group_id=100,
        vk_group_name="TestGroup",
        publish_at=int(time.time()) + 3600,
    )
    defaults.update(kwargs)
    return defaults


def test_add_and_get_scheduled_post():
    db.ensure_user(1)
    post_id = db.add_scheduled_post(**_make_post())
    assert isinstance(post_id, int)
    posts = db.get_scheduled_posts()
    assert len(posts) == 1
    assert posts[0]["url"] == "https://vm.tiktok.com/test"
    assert posts[0]["platform"] == "tiktok"
    assert posts[0]["vk_group_name"] == "TestGroup"


def test_scheduled_posts_ordered_by_publish_at():
    db.ensure_user(1)
    now = int(time.time())
    db.add_scheduled_post(**_make_post(publish_at=now + 7200))
    db.add_scheduled_post(**_make_post(publish_at=now + 3600))
    db.add_scheduled_post(**_make_post(publish_at=now + 1800))
    posts = db.get_scheduled_posts()
    times = [p["publish_at"] for p in posts]
    assert times == sorted(times)


def test_delete_scheduled_post():
    db.ensure_user(1)
    post_id = db.add_scheduled_post(**_make_post())
    db.delete_scheduled_post(post_id)
    assert db.get_scheduled_posts() == []


def test_scheduled_post_description_stored():
    db.ensure_user(1)
    db.add_scheduled_post(**_make_post(description="Моё описание"))
    posts = db.get_scheduled_posts()
    assert posts[0]["description"] == "Моё описание"


# ─── Логи ошибок ──────────────────────────────────────────────────────────────

def test_log_and_get_errors():
    db.ensure_user(1)
    db.log_error(1, stage="upload", message="что-то пошло не так")
    errors = db.get_errors(1)
    assert len(errors) == 1
    assert errors[0]["message"] == "что-то пошло не так"
    assert errors[0]["stage"] == "upload"


def test_count_errors():
    db.ensure_user(1)
    assert db.count_errors(1) == 0
    db.log_error(1, message="err1")
    db.log_error(1, message="err2")
    assert db.count_errors(1) == 2


def test_errors_isolated_by_user():
    db.ensure_user(1)
    db.ensure_user(2)
    db.log_error(1, message="err")
    assert db.count_errors(2) == 0


def test_errors_ordered_newest_first():
    db.ensure_user(1)
    db.log_error(1, message="first")
    db.log_error(1, message="second")
    errors = db.get_errors(1)
    assert errors[0]["message"] == "second"


def test_get_error_by_id():
    db.ensure_user(1)
    db.log_error(1, message="specific")
    e = db.get_errors(1)[0]
    fetched = db.get_error(e["id"])
    assert fetched["message"] == "specific"


def test_cleanup_old_errors_removes_stale():
    db.ensure_user(1)
    with db._connect() as conn:
        conn.execute(
            "INSERT INTO error_logs (telegram_id, created_at, message) VALUES (?, ?, ?)",
            (1, int(time.time()) - 10 * 86400, "old error"),
        )
    db.log_error(1, message="fresh error")
    deleted = db.cleanup_old_errors(days=5)
    assert deleted == 1
    assert db.count_errors(1) == 1
    assert db.get_errors(1)[0]["message"] == "fresh error"


def test_cleanup_old_errors_keeps_recent():
    db.ensure_user(1)
    db.log_error(1, message="recent")
    deleted = db.cleanup_old_errors(days=5)
    assert deleted == 0
    assert db.count_errors(1) == 1


def test_log_error_masks_vk1_token():
    db.ensure_user(1)
    db.log_error(1, message="got vk1.a.SECRETTOKEN123 in response")
    e = db.get_errors(1)[0]
    assert "SECRETTOKEN123" not in e["message"]
    assert "vk1.a.***" in e["message"]


def test_log_error_masks_access_token_in_url():
    db.ensure_user(1)
    db.log_error(1, url="https://api.vk.com/method?access_token=mysecret&v=5")
    e = db.get_errors(1)[0]
    assert "mysecret" not in e["url"]
    assert "access_token=***" in e["url"]


def test_log_error_masks_token_in_traceback():
    db.ensure_user(1)
    db.log_error(1, traceback="line 5: access_token=hidden123 raised")
    e = db.get_errors(1)[0]
    assert "hidden123" not in e["traceback"]


def test_log_error_best_effort_no_raise(monkeypatch):
    """log_error не должен падать даже при сбое БД."""
    monkeypatch.setattr(db, "DB_PATH", "/nonexistent/path/db.sqlite")
    db.log_error(999, message="test")  # не должно бросить исключение


# ─── _sanitize ────────────────────────────────────────────────────────────────

def test_sanitize_none_input():
    assert db._sanitize(None) is None


def test_sanitize_empty_string():
    assert db._sanitize("") == ""


def test_sanitize_no_tokens():
    text = "обычное сообщение без токенов"
    assert db._sanitize(text) == text


def test_sanitize_vk1_token():
    result = db._sanitize("vk1.a.AbCdEfGhIjKlMnOpQrStUvWxYz")
    assert "AbCdEfGhIjKlMnOpQrStUvWxYz" not in result
    assert "vk1.a.***" in result


def test_sanitize_access_token_in_query():
    result = db._sanitize("access_token=mytoken123&other=val")
    assert "mytoken123" not in result
    assert "access_token=***" in result


# ─── Миграция scheduled_posts ─────────────────────────────────────────────────

def test_migration_from_old_schema(tmp_path, monkeypatch):
    """Миграция: старая таблица с file_path NOT NULL → новая без него."""
    import sqlite3

    old_db = str(tmp_path / "old.db")
    monkeypatch.setattr(db, "DB_PATH", old_db)

    # Создаём старую схему вручную
    with sqlite3.connect(old_db) as conn:
        conn.executescript("""
            CREATE TABLE users (telegram_id INTEGER PRIMARY KEY, vk_token TEXT);
            CREATE TABLE vk_groups (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                telegram_id INTEGER NOT NULL,
                vk_group_id INTEGER NOT NULL,
                name TEXT NOT NULL,
                UNIQUE(telegram_id, vk_group_id)
            );
            CREATE TABLE description_templates (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                telegram_id INTEGER NOT NULL,
                title TEXT NOT NULL,
                body TEXT NOT NULL
            );
            CREATE TABLE error_logs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                telegram_id INTEGER NOT NULL,
                created_at INTEGER NOT NULL,
                stage TEXT, platform TEXT, url TEXT,
                vk_group_id INTEGER, vk_group_name TEXT,
                error_code INTEGER, message TEXT, traceback TEXT
            );
            CREATE TABLE scheduled_posts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                telegram_id INTEGER NOT NULL,
                chat_id INTEGER NOT NULL,
                file_path TEXT NOT NULL,
                title TEXT NOT NULL,
                description TEXT NOT NULL,
                vk_group_id INTEGER NOT NULL,
                vk_group_name TEXT NOT NULL,
                platform TEXT,
                url TEXT,
                publish_at INTEGER NOT NULL
            );
            INSERT INTO scheduled_posts
                (telegram_id, chat_id, file_path, title, description,
                 vk_group_id, vk_group_name, platform, url, publish_at)
            VALUES (1, 10, '/tmp/video.mp4', 'Title', 'Desc', 100, 'G', 'tiktok',
                    'https://vm.tiktok.com/xxx', 9999999999);
        """)

    db.init_db()  # должна запустить миграцию

    posts = db.get_scheduled_posts()
    assert len(posts) == 1
    assert posts[0]["url"] == "https://vm.tiktok.com/xxx"
    assert posts[0]["platform"] == "tiktok"
