import os
import sys

# Добавляем корень проекта в sys.path, чтобы импорты db/downloader/bot работали
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

# Заглушка токена — bot.py читает его при импорте
os.environ.setdefault("TELEGRAM_TOKEN", "0:fake_token_for_tests")
