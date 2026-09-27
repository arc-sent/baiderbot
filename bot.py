import os
import re
import time
import random
import asyncio
import logging
import threading
import traceback as tb_module
from concurrent.futures import ThreadPoolExecutor
from io import BytesIO
from datetime import datetime, timedelta
from typing import Awaitable, Callable

import pytz
import requests
import vk_api
from vk_api.exceptions import ApiError
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, ReplyKeyboardMarkup
from telegram.error import BadRequest, NetworkError as TelegramNetworkError
from telegram.ext import (
    Application, CommandHandler, MessageHandler,
    CallbackQueryHandler, ConversationHandler, filters,
    ContextTypes, PicklePersistence, TypeHandler,
)
from dotenv import load_dotenv

import db
import vk_proxy
from downloader import (
    detect_platform, download_tiktok, download_likee, download_youtube,
    download_vk, download_instagram, cleanup_tmp_files,
)

load_dotenv()

TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
VK_API_VERSION = "5.199"
VK_SHORT_VIDEO_API_VERSION = "5.126"  # shortVideo методы работают только на этой версии
MOSCOW_TZ = pytz.timezone("Europe/Moscow")

# Telegram ID администраторов (через запятую в .env) — кто видит ВСЕ ошибки.
ADMIN_IDS = {
    int(x) for x in os.getenv("ADMIN_IDS", "").replace(" ", "").split(",") if x
}

# Сколько суток храним логи ошибок и как часто чистим (см. job в main()).
ERROR_RETENTION_DAYS = int(os.getenv("ERROR_RETENTION_DAYS", "5"))
ERROR_CLEANUP_INTERVAL_DAYS = int(os.getenv("ERROR_CLEANUP_INTERVAL_DAYS", "5"))

# Размер страницы в админ-панели / списке ошибок.
ERRORS_PAGE_SIZE = 8

PLATFORM_LABELS = {
    "tiktok": "TikTok",
    "likee": "Likee",
    "youtube": "YouTube Shorts",
    "vk": "VK",
    "instagram": "Instagram Reels",
}

logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)
# httpx на уровне INFO пишет каждый запрос к Telegram с полным URL — а в нём
# токен бота (/bot<TOKEN>/getUpdates). Оставляем только предупреждения.
logging.getLogger("httpx").setLevel(logging.WARNING)

# ─── Состояния разговоров ─────────────────────────────────────────────────────

# Загрузка видео
UP_GROUP, UP_DESC, UP_CUSTOM_DESC, UP_TIME, UP_CUSTOM_TIME = range(5)
# Установка токена
TOKEN_WAIT = 10
# Управление группами
G_ADD_ID, G_ADD_CONFIRM, G_ADD_NAME, G_RENAME = range(20, 24)
# Управление заготовками
T_TITLE, T_BODY = range(30, 32)

TIME_SLOTS = [7, 9, 11, 13, 15, 17, 19, 21, 23]
TIME_SLOTS_ROW_SIZE = 3  # кнопок в строке клавиатуры времени

MAX_GROUPS_PER_USER = 50  # максимум групп VK на одного пользователя
KEYBOARD_PAGE_SIZE = 15  # максимум элементов на одной странице инлайн-клавиатуры

# ─── Контроль нагрузки на VK (настраивается через .env) ───────────────────────
# Сколько публикаций ОДНОГО пользователя может уходить в VK одновременно.
# Лимиты VK (error_code 6/9) считаются по access_token, т.е. на пользователя —
# поэтому семафор отдельный на каждого юзера (см. _user_publish_semaphore).
# Так разные пользователи никогда не блокируют друг друга, а несколько роликов
# одного юзера в один слот по-прежнему выстраиваются в очередь.
# Сколько видео качается одновременно по всем пользователям.
# Ограничивает нагрузку на thread pool, диск и снижает риск бана по IP
# на TikTok/YouTube при массовых одновременных публикациях.
DOWNLOAD_CONCURRENCY = int(os.getenv("DOWNLOAD_CONCURRENCY", "20"))
DOWNLOAD_TIMEOUT = int(os.getenv("DOWNLOAD_TIMEOUT", "600"))  # секунд, 0 = без таймаута
_download_semaphore: asyncio.Semaphore | None = None  # инициализируется в main()

VK_PUBLISH_CONCURRENCY = int(os.getenv("VK_PUBLISH_CONCURRENCY", "1"))
_vk_publish_semaphores: dict[int, asyncio.Semaphore] = {}

# Отдельный пул потоков для публикаций в VK. Публикация держит поток долго
# (загрузка файла, ожидание обработки клипа до ~2 мин), и в общем пуле такие
# задачи могли занять все потоки — тогда «зависали» и короткие операции
# (поиск группы, проверка токена) у всех пользователей.
_vk_executor = ThreadPoolExecutor(
    max_workers=int(os.getenv("VK_PUBLISH_THREADS", "16")),
    thread_name_prefix="vk_publish",
)


async def _download_video(platform: str, url: str, vk_token: str | None = None) -> tuple[str, str]:
    """Скачивает видео с учётом глобального семафора и таймаута.

    Бросает asyncio.TimeoutError если скачивание не завершилось за DOWNLOAD_TIMEOUT секунд,
    освобождая слот семафора для следующей задачи в очереди.
    """
    async with _download_semaphore:
        coro: object
        if platform == "tiktok":
            coro = download_tiktok(url, None)
        elif platform == "likee":
            coro = download_likee(url)
        elif platform == "youtube":
            coro = download_youtube(url, None)
        elif platform == "vk":
            coro = download_vk(url, vk_token)
        elif platform == "instagram":
            coro = download_instagram(url, None)
        else:
            raise ValueError(f"Неизвестная платформа: {platform}")

        if DOWNLOAD_TIMEOUT > 0:
            return await asyncio.wait_for(coro, timeout=DOWNLOAD_TIMEOUT)
        return await coro


def _user_publish_semaphore(telegram_id: int) -> asyncio.Semaphore:
    """Семафор публикации для конкретного пользователя (создаётся лениво).

    В asyncio один поток выполнения, между get и присваиванием нет await —
    поэтому get-or-create атомарен, гонки нет."""
    sem = _vk_publish_semaphores.get(telegram_id)
    if sem is None:
        sem = asyncio.Semaphore(VK_PUBLISH_CONCURRENCY)
        _vk_publish_semaphores[telegram_id] = sem
    return sem

# Ретрай публикации при временных ошибках VK / сети.
VK_PUBLISH_RETRIES = int(os.getenv("VK_PUBLISH_RETRIES", "3"))       # всего попыток
VK_RETRY_BASE_DELAY = float(os.getenv("VK_RETRY_BASE_DELAY", "3"))   # секунды, растёт экспоненциально
# Коды ошибок VK, при которых имеет смысл повторить запрос.
VK_RETRYABLE_ERROR_CODES = {1, 6, 9, 10}  # неизвестная/too many/flood/internal

# Ожидание обработки клипа на серверах VK (error 3001 = видео ещё не готово).
VK_SHORT_VIDEO_POLL_ATTEMPTS = int(os.getenv("VK_SHORT_VIDEO_POLL_ATTEMPTS", "12"))
VK_SHORT_VIDEO_POLL_INTERVAL = float(os.getenv("VK_SHORT_VIDEO_POLL_INTERVAL", "10"))

# Джиттер времени публикации: чтобы ролики не выходили ровно в HH:00:00
# (для реков — «живее», когда время чуть «плавает»).
PUBLISH_JITTER_SECONDS = int(os.getenv("PUBLISH_JITTER_SECONDS", "300"))

# При старте бот восстанавливает отложенные публикации из БД. Просроченные
# (их время прошло, пока бот лежал) публикуются сразу, но с этим интервалом
# между собой — чтобы накопившиеся ролики не ушли в VK залпом и не словили
# rate-limit.
RESTORE_SPREAD_SECONDS = int(os.getenv("RESTORE_SPREAD_SECONDS", "20"))

# Активные задачи загрузки. Ключ — (chat_id, message_id) статусного сообщения:
# message_id уникален лишь ВНУТРИ чата, поэтому у разных пользователей id
# совпадают. Ключ только по message_id приводил бы к коллизии между юзерами —
# кнопка «Отмена» одного могла отменить чужую загрузку. Пара (chat_id, message_id)
# глобально уникальна.
_upload_tasks: dict[tuple[int, int], asyncio.Task] = {}


class VKError(RuntimeError):
    """Ошибка публикации в VK с кодом и этапом — чтобы отличать временные сбои
    от фатальных и показывать пользователю, на каком шаге всё упало.

    network=True помечает сетевой сбой (а не ответ VK с error_code) — такие
    ошибки тоже имеет смысл повторять.

    ambiguous=True — запрос на публикацию ушёл, но ответа нет: VK мог уже
    опубликовать ролик. Такое НЕ повторяем — иначе в группе будет дубль.
    """

    def __init__(
        self,
        code: int | None,
        message: str,
        *,
        stage: str | None = None,
        network: bool = False,
        ambiguous: bool = False,
    ):
        self.code = code
        self.stage = stage
        self.network = network
        self.ambiguous = ambiguous
        super().__init__(message)


# Признаки «сервер не отвечает» в тексте ошибки — для исключений, которые не
# сохраняют исходную причину в цепочке (например, DownloadError из yt-dlp).
_NETWORK_ERROR_MARKERS = (
    "timed out", "read timeout", "connect timeout", "max retries exceeded",
    "failed to establish a new connection", "connection refused",
    "connection reset", "connection aborted", "remotedisconnected",
    "network is unreachable", "no route to host", "name resolution",
    "name or service not known", "nodename nor servname",
)


def _is_network_error(exc: BaseException | None) -> bool:
    """True, если ошибка — «сервер не отвечает / нет соединения», а не ответ сервера.

    Проходит по цепочке причин (raise … from exc), т.к. сетевой сбой обычно
    завёрнут в VKError / RuntimeError.
    """
    seen: set[int] = set()
    while exc is not None and id(exc) not in seen:
        seen.add(id(exc))
        # VKError.network сам по себе не показатель: им помечены и «кривые»
        # ответы (не-JSON). Смотрим на исходную причину в цепочке.
        if isinstance(exc, (
            requests.exceptions.ConnectionError,
            requests.exceptions.Timeout,
            ConnectionError,
            TimeoutError,
        )):
            return True
        text = str(exc).lower()
        if any(marker in text for marker in _NETWORK_ERROR_MARKERS):
            return True
        exc = exc.__cause__ or exc.__context__
    return False


def _unreachable_text(service: str = "VK") -> str:
    return (
        f"🌐 Сервер {service} не отвечает — не удалось подключиться.\n"
        f"Скорее всего, это временный сбой на стороне {service} или сети. "
        "Попробуй ещё раз через несколько минут."
    )


CANCEL_MARKUP = InlineKeyboardMarkup([[
    InlineKeyboardButton("❌ Отменить", callback_data="cancel_upload")
]])

# ─── Постоянное меню (кнопки над клавиатурой) ─────────────────────────────────
BTN_TOKEN = "🔑 Мой токен"
BTN_GROUPS = "👥 Группы"
BTN_TEMPLATES = "📝 Описания"
MENU_BUTTON_TEXTS = [BTN_TOKEN, BTN_GROUPS, BTN_TEMPLATES]


def main_keyboard() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        [[BTN_TOKEN, BTN_GROUPS, BTN_TEMPLATES]],
        resize_keyboard=True,
    )


# ─── Helpers ────────────────────────────────────────────────────────────────

# Ссылка на VK в любом месте текста. Хосты: vk.com / vk.ru (VK переехал на него,
# приложение копирует ссылки уже с vk.ru), vkontakte.ru, vk.me, vkvideo.ru,
# плюс любые поддомены (m., www., new.). Lookbehind не даёт зацепить «notvk.com».
_VK_LINK_RE = re.compile(
    r"(?<![\w.-])(?:https?://)?(?:[a-z0-9-]+\.)*"
    r"(?:vk\.com|vk\.ru|vkontakte\.ru|vk\.me|vkvideo\.ru)/([^\s<>\"'«»]*)",
    re.IGNORECASE,
)
# Сегменты пути, которые обозначают раздел, а не само сообщество:
# vk.com/clips/mygroup, vkvideo.ru/@mygroup/all и т.п.
_VK_SECTION_SEGMENTS = {"clips", "video", "videos", "all", "playlists", "shorts"}
# Разделы, в ссылке на которые после минуса стоит id сообщества:
# wall-1_2, video-1_2, videos-1, clips-1, album-1_0, topic-1_2, market-1 …
_VK_OWNER_SECTION_RE = re.compile(
    r"(?:wall|videos?|clips?|photos?|albums?|topic|market|audios|docs|"
    r"playlist|podcasts|articles|board)-(\d+)",
    re.IGNORECASE,
)
_TRAILING_PUNCT = ".,;:!?)]}»\"'"

# Типы из utils.resolveScreenName, которые означают сообщество.
_VK_COMMUNITY_TYPES = {"group", "page", "event", "community"}
# Ошибки VK, которые не про «такого имени нет», а про токен/лимиты/сеть —
# их нужно показать пользователю как есть, а не выдавать за «не найдено».
_VK_FATAL_LOOKUP_CODES = {None, 1, 5, 6, 9, 10, 14, 17, 29}

VK_LOOKUP_RETRIES = int(os.getenv("VK_LOOKUP_RETRIES", "3"))
VK_LOOKUP_RETRY_DELAY = float(os.getenv("VK_LOOKUP_RETRY_DELAY", "1"))

# Таймаут ПОДКЛЮЧЕНИЯ к серверам VK, секунд. Важно: у api.vk.com ~9 IP-адресов,
# и requests/urllib3 при неудаче перебирает их по очереди, ожидая connect-таймаут
# на КАЖДОМ. С прежними 15 с один запрос при недоступном VK висел ~2 минуты,
# а с ретраями — до 7 минут, и пользователь не получал никакого ответа.
VK_CONNECT_TIMEOUT = float(os.getenv("VK_CONNECT_TIMEOUT", "4"))
# Общий предел ожидания поиска группы / проверки токена — после него
# пользователь получает «сервер VK не отвечает», даже если запросы ещё идут.
VK_LOOKUP_DEADLINE = float(os.getenv("VK_LOOKUP_DEADLINE", "40"))


class _TimeoutSession(requests.Session):
    """requests.Session с таймаутом по умолчанию и (если включён) прокси VK.

    vk_api делает запросы без таймаута вообще: при недоступном VK вызов мог
    висеть десятки минут (системный таймаут TCP × 9 IP-адресов). Прокси
    проверяется на каждый запрос заново — переключатель в админ-панели
    должен подхватываться сразу, без пересоздания сессии.
    """

    def request(self, *args, **kwargs):
        kwargs.setdefault("timeout", (VK_CONNECT_TIMEOUT, 60))
        kwargs.setdefault("proxies", vk_proxy.requests_proxies())
        return super().request(*args, **kwargs)


async def _run_vk_lookup(func, *args):
    """Запускает синхронный поиск в VK в потоке с общим пределом времени.

    Бросает asyncio.TimeoutError, если VK_LOOKUP_DEADLINE истёк (поток при
    этом доработает сам — прервать его нельзя, но пользователь уже получит ответ).
    """
    loop = asyncio.get_running_loop()
    return await asyncio.wait_for(
        loop.run_in_executor(None, func, *args), timeout=VK_LOOKUP_DEADLINE
    )


def _message_text(message) -> str:
    """Текст сообщения + адреса из скрытых ссылок (text_link).

    В пересланных постах ссылка часто «спрятана» под словом — тогда в
    message.text её нет, она лежит только в entity.url.
    """
    text = getattr(message, "text", None) or ""
    extra = []
    for ent in getattr(message, "entities", None) or ():
        url = getattr(ent, "url", None)
        if isinstance(url, str) and url:
            extra.append(url)
    return "\n".join([text, *extra]) if extra else text


_URL_IN_TEXT_RE = re.compile(r"https?://[^\s<>\"'«»]+", re.IGNORECASE)


def extract_platform_url(text: str | None) -> tuple[str, str] | None:
    """Находит в тексте первую ссылку на поддерживаемую платформу.

    Возвращает (url, platform) или None. Ссылка может стоять в любом месте
    сообщения («смотри https://youtu.be/…»), а не только в начале.
    """
    if not text:
        return None
    for m in _URL_IN_TEXT_RE.finditer(text):
        url = m.group(0).rstrip(_TRAILING_PUNCT)
        platform = detect_platform(url)
        if platform:
            return url, platform
    return None


def _extract_screen_name(text: str) -> str:
    """Из ссылки/ввода достаёт «короткое имя» сообщества.

    vk.com/club123                 -> club123
    https://vk.ru/durov            -> durov
    Вот группа https://vk.com/abc  -> abc
    vk.com/video-1_2?list=x        -> video-1_2
    vkvideo.ru/@mygroup/all        -> mygroup
    @mygroup / club123             -> mygroup / club123
    """
    text = text.strip()
    m = _VK_LINK_RE.search(text)
    if m:
        path = m.group(1)
    else:
        # Ссылки нет — считаем, что прислали само имя / id (одним словом).
        parts = text.split()
        path = parts[0] if parts else ""
    path = path.split("?")[0].split("#")[0]      # убираем query/fragment
    segments = [s for s in path.split("/") if s]
    name = next(
        (s for s in segments if s.lower().lstrip("@") not in _VK_SECTION_SEGMENTS),
        "",
    )
    return name.lstrip("@").rstrip(_TRAILING_PUNCT)


def _vk_call(method: str, vk_token: str, **params) -> tuple[object, dict | None]:
    """Вызов VK API с ретраями на временные ошибки (лимиты, сеть).

    Возвращает (response, error). error — dict VK {"error_code", "error_msg"};
    у сетевых сбоев error_code = None. POST, а не GET: иначе access_token попал
    бы в текст исключения requests (URL с query) и дальше в логи.
    """
    err: dict | None = None
    for attempt in range(max(1, VK_LOOKUP_RETRIES)):
        try:
            resp = requests.post(
                f"https://api.vk.com/method/{method}",
                data={**params, "access_token": vk_token, "v": VK_API_VERSION},
                proxies=vk_proxy.requests_proxies(),
                timeout=(VK_CONNECT_TIMEOUT, 15),
            ).json()
        except (requests.exceptions.RequestException, ValueError) as exc:
            err = {"error_code": None, "error_msg": f"сетевая ошибка: {exc}"}
        else:
            if "error" not in resp:
                return resp.get("response"), None
            err = resp["error"]
            if err.get("error_code") not in VK_RETRYABLE_ERROR_CODES:
                break
        logger.warning("%s: попытка %s не удалась: %s", method, attempt + 1, err)
        if attempt + 1 < VK_LOOKUP_RETRIES:
            time.sleep(VK_LOOKUP_RETRY_DELAY * (2 ** attempt))
    logger.warning("%s error: %s", method, err)
    return None, err


def _vk_error_text(err: dict) -> str:
    """Понятное пользователю объяснение ошибки VK при поиске сообщества."""
    code = err.get("error_code")
    if code is None:
        return _unreachable_text("VK")
    if code == 5:
        return (
            "VK отклонил твой токен — он недействителен или истёк.\n"
            f"Обнови его кнопкой «{BTN_TOKEN}» и пришли ссылку ещё раз."
        )
    if code in (6, 9, 29):
        return "VK временно ограничил частоту запросов. Подожди минуту и пришли ссылку ещё раз."
    if code in (14, 17):
        return (
            "VK требует подтверждения (капча / проверка) для этого токена. "
            f"Зайди в VK с браузера, затем получи новый токен кнопкой «{BTN_TOKEN}»."
        )
    return f"VK вернул ошибку {code}: {err.get('error_msg') or 'без описания'}"


def _first_group(response) -> dict | None:
    """Достаёт первую группу из ответа groups.getById (старый и новый формат)."""
    try:
        if isinstance(response, list):
            return response[0]
        if isinstance(response, dict):
            return response["groups"][0]
    except (KeyError, IndexError, TypeError):
        pass
    return None


def resolve_vk_group(
    vk_token: str | None, text: str,
) -> tuple[int | None, str | None, str | None, str | None]:
    """По ссылке/короткому имени/ID определяет группу.

    Возвращает (group_id, name, error, note). Если group_id is None — в error
    лежит текст для пользователя, объясняющий, почему не удалось. note — пояснение,
    почему не удалось получить название (группа при этом найдена по id).
    """
    raw = _extract_screen_name(text)
    if not raw:
        return None, None, "Пустая ссылка. Пришли ссылку на сообщество VK.", None

    def by_id(gid: int):
        if not vk_token:
            return gid, None, None, None
        name, err = fetch_vk_group_name(vk_token, gid)
        note = _vk_error_text(err) if err and not name else None
        return gid, name, None, note

    # wall-1_2 / video-1_2 / clips-1 / album-1_0 … — id группы это число после минуса
    m = _VK_OWNER_SECTION_RE.match(raw)
    if m:
        return by_id(int(m.group(1)))

    # club123 / public123 / event123 / board123 — числовой id прямо в имени
    m = re.fullmatch(r"(?:club|public|event|board)(\d+)", raw, re.IGNORECASE)
    if m:
        return by_id(int(m.group(1)))

    # голый id (вдруг прислали число или -число)
    if re.fullmatch(r"-?\d+", raw):
        return by_id(abs(int(raw)))

    # короткое имя сообщества — нужен токен
    if not vk_token:
        return None, None, (
            "Чтобы добавить группу по короткой ссылке, сначала задай VK токен "
            f"(кнопка «{BTN_TOKEN}»). Либо пришли ссылку вида vk.com/club123."
        ), None

    # groups.getById принимает и короткие имена: одним запросом получаем и id,
    # и название (меньше запросов — меньше шанс словить лимит VK).
    response, err = _vk_call("groups.getById", vk_token, group_id=raw)
    group = _first_group(response)
    if group and group.get("id"):
        return int(group["id"]), group.get("name"), None, None
    if err and err.get("error_code") in _VK_FATAL_LOOKUP_CODES:
        return None, None, _vk_error_text(err), None

    # Не группа (или VK не отдал её) — выясняем, что это за имя.
    obj, err = _vk_call("utils.resolveScreenName", vk_token, screen_name=raw)
    if err:
        return None, None, _vk_error_text(err), None
    if not obj:
        return None, None, (
            f"Не нашёл в VK сообщества «{raw}». Проверь ссылку — "
            "или пришли ссылку вида vk.com/club123."
        ), None
    obj_type = obj.get("type")
    if obj_type not in _VK_COMMUNITY_TYPES:
        human = {"user": "страница пользователя", "application": "приложение"}.get(obj_type, obj_type)
        return None, None, f"Это не сообщество, а {human}. Пришли ссылку именно на группу/паблик VK.", None
    return by_id(int(obj["object_id"]))


def fetch_vk_group_name(vk_token: str, group_id: int) -> tuple[str | None, dict | None]:
    """Пробует получить название группы через VK API.

    Возвращает (name, error): name=None, если не удалось; error — ошибка VK
    (error_code=None — сервер VK не ответил).
    """
    response, err = _vk_call("groups.getById", vk_token, group_id=group_id)
    group = _first_group(response)
    return (group.get("name"), None) if group else (None, err)


class _PlatformUrlFilter(filters.MessageFilter):
    """Пропускает только сообщения, содержащие распознанную ссылку на платформу."""
    def filter(self, message) -> bool:
        return extract_platform_url(_message_text(message)) is not None

_URL_FILTER = _PlatformUrlFilter()


class _VKCommunityLinkFilter(filters.MessageFilter):
    """Ссылка на VK, но не на видео/клип — т.е. скорее всего на сообщество."""
    def filter(self, message) -> bool:
        text = _message_text(message)
        if "access_token=" in text:  # это ссылка с токеном, а не с сообществом
            return False
        return bool(_VK_LINK_RE.search(text)) and extract_platform_url(text) is None

_VK_COMMUNITY_FILTER = _VKCommunityLinkFilter()


class UploadCancelled(Exception):
    """Пользователь нажал «Отменить» — публикацию нужно прервать."""


def _check_cancel(cancel_event: threading.Event | None) -> None:
    """Точка отмены внутри потока публикации.

    asyncio-отмена не останавливает код в run_in_executor: поток продолжал бы
    работу и публиковал ролик, хотя пользователь уже видит «отменено». Поэтому
    поток сам проверяет флаг между этапами.
    """
    if cancel_event is not None and cancel_event.is_set():
        raise UploadCancelled("публикация отменена пользователем")


def _delete_vk_video(vk_token: str, owner_id, video_id) -> None:
    """Best-effort удаление загруженного, но не опубликованного видео/клипа —
    чтобы после сбоя или отмены в группе не оставались «висящие» черновики."""
    try:
        requests.post(
            "https://api.vk.com/method/video.delete",
            data={
                "access_token": vk_token,
                "v": VK_API_VERSION,
                "owner_id": owner_id,
                "video_id": video_id,
            },
            proxies=vk_proxy.requests_proxies(),
            timeout=(VK_CONNECT_TIMEOUT, 30),
        )
    except Exception:
        logger.warning("Не удалось удалить черновик видео %s_%s", owner_id, video_id, exc_info=True)


def _unexpected_response(stage: str, data) -> VKError:
    """VK ответил не тем, что мы ждали (нет нужных полей)."""
    return VKError(None, f"неожиданный ответ VK: {str(data)[:300]}", stage=stage)


def _upload_short_video(
    vk_token: str,
    group_id: int,
    file_path: str,
    description: str,
    cancel_event: threading.Event | None = None,
) -> None:
    """Публикует видео как VK Клип через shortVideo API (через библиотеку vk_api).

    Последовательность: shortVideo.create → загрузка файла → shortVideo.edit
    (если есть описание) → polling shortVideo.publish до готовности видео.
    Бросает VKError при любой ошибке API или превышении попыток ожидания,
    UploadCancelled — если пользователь отменил публикацию.
    Если сбой случился после создания клипа — черновик удаляется.
    """
    _check_cancel(cancel_event)
    try:
        vk = vk_api.VkApi(
            token=vk_token,
            api_version=VK_SHORT_VIDEO_API_VERSION,
            session=_TimeoutSession(),
        ).get_api()
    except Exception as exc:
        raise VKError(None, f"ошибка инициализации vk_api: {exc}", stage="vk_api init", network=True) from exc

    # ── Этап 1: shortVideo.create ─────────────────────────────────────────
    stage = "VK shortVideo.create"
    try:
        upload_data = vk.shortVideo.create(
            group_id=group_id,
            file_size=os.path.getsize(file_path) // 1024,  # в килобайтах
        )
    except ApiError as exc:
        raise VKError(exc.code, str(exc), stage=stage) from exc
    except Exception as exc:
        raise VKError(None, f"сетевая ошибка: {exc}", stage=stage, network=True) from exc
    logger.info("shortVideo.create response: %s", upload_data)

    try:
        upload_url = upload_data["upload_url"]
        video_id   = upload_data["video_id"]
        owner_id   = upload_data["owner_id"]
    except (KeyError, TypeError) as exc:
        raise _unexpected_response(stage, upload_data) from exc

    published = False
    try:
        # ── Этап 2: загрузка файла ────────────────────────────────────────
        _check_cancel(cancel_event)
        stage = "загрузка файла shortVideo"
        try:
            with open(file_path, "rb") as f:
                upload_resp = requests.post(
                    upload_url, files={"file": f},
                    proxies=vk_proxy.requests_proxies(), timeout=(VK_CONNECT_TIMEOUT, 300),
                )
                upload_resp.raise_for_status()
                logger.info("shortVideo upload response: %s", upload_resp.text[:500])
                upload_info = upload_resp.json()
                video_id = upload_info.get("video_id", video_id)
                owner_id = upload_info.get("owner_id", owner_id)
        except requests.exceptions.RequestException as exc:
            raise VKError(None, f"сетевая ошибка: {exc}", stage=stage, network=True) from exc
        except AttributeError as exc:  # json() вернул не dict
            raise _unexpected_response(stage, upload_resp.text[:300]) from exc

        # ── Этап 3: shortVideo.edit (только если есть описание) ───────────
        if description:
            _check_cancel(cancel_event)
            stage = "VK shortVideo.edit"
            try:
                vk.shortVideo.edit(
                    video_id=video_id,
                    owner_id=owner_id,
                    description=description,
                )
            except ApiError as exc:
                raise VKError(exc.code, str(exc), stage=stage) from exc
            except Exception as exc:
                raise VKError(None, f"сетевая ошибка: {exc}", stage=stage, network=True) from exc

        # ── Этап 4: polling shortVideo.publish ────────────────────────────
        stage = "VK shortVideo.publish"
        for attempt in range(VK_SHORT_VIDEO_POLL_ATTEMPTS):
            _check_cancel(cancel_event)
            try:
                vk.shortVideo.publish(
                    video_id=video_id,
                    owner_id=owner_id,
                    license_agree=1,
                    wallpost=1,
                )
                published = True
                logger.info("shortVideo.publish успешно (попытка %s)", attempt + 1)
                return
            except ApiError as exc:
                if exc.code == 3001:
                    logger.info(
                        "shortVideo: видео ещё не обработано, ожидание (попытка %s/%s)",
                        attempt + 1, VK_SHORT_VIDEO_POLL_ATTEMPTS,
                    )
                    # Ждём кусками, чтобы отмена срабатывала быстро.
                    if cancel_event is not None:
                        cancel_event.wait(VK_SHORT_VIDEO_POLL_INTERVAL)
                    else:
                        time.sleep(VK_SHORT_VIDEO_POLL_INTERVAL)
                    continue
                raise VKError(exc.code, str(exc), stage=stage) from exc
            except Exception as exc:
                # Запрос на публикацию ушёл, а ответа нет — VK мог уже
                # опубликовать клип. Повтор или запасной путь дали бы дубль.
                published = True  # черновик не удаляем: он может быть уже постом
                raise VKError(
                    None, f"сетевая ошибка: {exc}", stage=stage, network=True, ambiguous=True,
                ) from exc

        raise VKError(
            3001,
            f"Видео не обработано после {VK_SHORT_VIDEO_POLL_ATTEMPTS} попыток "
            f"({VK_SHORT_VIDEO_POLL_ATTEMPTS * VK_SHORT_VIDEO_POLL_INTERVAL:.0f} сек). "
            "Попробуй увеличить VK_SHORT_VIDEO_POLL_ATTEMPTS.",
            stage=stage,
        )
    finally:
        if not published:
            # Сбой или отмена после create — клип остался бы черновиком в группе,
            # а повтор / video.save создали бы ещё одну копию.
            _delete_vk_video(vk_token, owner_id, video_id)


def _upload_video_legacy(
    vk_token: str,
    group_id: int,
    file_path: str,
    title: str,
    description: str,
    cancel_event: threading.Event | None = None,
) -> None:
    """Загружает видео через старый API (video.save + wall.post).

    Используется как fallback, если shortVideo недоступен или вернул ошибку.
    """
    _check_cancel(cancel_event)
    # ── Этап 1: video.save ────────────────────────────────────────────────
    stage = "VK video.save"
    save_data = {
        "access_token": vk_token,
        "v": VK_API_VERSION,
        "group_id": group_id,
        "name": title,
        "wallpost": 0,
    }
    if description:
        save_data["description"] = description

    try:
        save_resp = requests.post(
            "https://api.vk.com/method/video.save",
            data=save_data,
            proxies=vk_proxy.requests_proxies(),
            timeout=(VK_CONNECT_TIMEOUT, 30),
        ).json()
    except requests.exceptions.RequestException as exc:
        raise VKError(None, f"сетевая ошибка: {exc}", stage=stage, network=True) from exc
    logger.info("video.save response: %s", save_resp)

    if "error" in save_resp:
        e = save_resp["error"]
        raise VKError(
            e.get("error_code"),
            f"VK {e.get('error_code')}: {e.get('error_msg')}",
            stage=stage,
        )

    try:
        video_id = save_resp["response"]["video_id"]
        owner_id = save_resp["response"]["owner_id"]
        upload_url = save_resp["response"]["upload_url"]
    except (KeyError, TypeError) as exc:
        raise _unexpected_response(stage, save_resp) from exc

    posted = False
    try:
        # ── Этап 2: загрузка файла на upload-сервер ───────────────────────
        _check_cancel(cancel_event)
        stage = "загрузка файла в VK"
        try:
            with open(file_path, "rb") as f:
                upload_resp = requests.post(
                    upload_url, files={"video_file": f},
                    proxies=vk_proxy.requests_proxies(), timeout=(VK_CONNECT_TIMEOUT, 300),
                )
                upload_resp.raise_for_status()
                logger.info("video upload response: %s", upload_resp.text[:500])
        except requests.exceptions.RequestException as exc:
            raise VKError(None, f"сетевая ошибка: {exc}", stage=stage, network=True) from exc

        # ── Этап 3: wall.post (публикация записи на стене) ────────────────
        _check_cancel(cancel_event)
        stage = "VK wall.post"
        wall_params = {
            "access_token": vk_token,
            "v": VK_API_VERSION,
            "owner_id": f"-{group_id}",
            "message": description,
            "attachments": f"video{owner_id}_{video_id}",
            "from_group": 1,
        }
        try:
            wall_resp = requests.post(
                "https://api.vk.com/method/wall.post", data=wall_params,
                proxies=vk_proxy.requests_proxies(), timeout=(VK_CONNECT_TIMEOUT, 30),
            ).json()
        except requests.exceptions.RequestException as exc:
            # Пост мог уже появиться — не удаляем видео и не повторяем.
            posted = True
            raise VKError(
                None, f"сетевая ошибка: {exc}", stage=stage, network=True, ambiguous=True,
            ) from exc
        logger.info("wall.post response: %s", wall_resp)

        if "error" in wall_resp:
            e = wall_resp["error"]
            raise VKError(
                e.get("error_code"),
                f"VK {e.get('error_code')}: {e.get('error_msg')}",
                stage=stage,
            )
        posted = True
    finally:
        if not posted:
            _delete_vk_video(vk_token, owner_id, video_id)


def upload_to_vk(
    vk_token: str,
    vk_group_id: int,
    file_path: str,
    title: str,
    description: str,
    cancel_event: threading.Event | None = None,
) -> None:
    """Загружает видео в VK и публикует в группе.

    Сначала пробует shortVideo API (видео попадает в Клипы).
    Если shortVideo вернул ошибку — публикует через video.save + wall.post.
    """
    group_id = abs(int(vk_group_id))
    logger.info("upload_to_vk: group_id=%s description=%r", group_id, description)

    try:
        _upload_short_video(vk_token, group_id, file_path, description, cancel_event=cancel_event)
        logger.info("upload_to_vk: опубликовано как Клип (shortVideo)")
        return
    except VKError as exc:
        if exc.ambiguous or _is_network_error(exc):
            # ambiguous — клип мог уже выйти, запасной путь дал бы дубль.
            # Сеть — video.save упрётся в тот же таймаут; _publish_to_vk
            # повторит попытку и сообщит пользователю.
            raise
        logger.warning(
            "shortVideo не удался (код %s, этап %r), переключаюсь на video.save: %s",
            exc.code, exc.stage, exc,
        )

    _upload_video_legacy(vk_token, group_id, file_path, title, description, cancel_event=cancel_event)
    logger.info("upload_to_vk: опубликовано как обычное видео (video.save fallback)")


async def _publish_to_vk(
    telegram_id: int,
    vk_token: str,
    vk_group_id: int,
    file_path: str,
    title: str,
    description: str,
    on_retry: Callable[[str], Awaitable[None]] | None = None,
    cancel_event: threading.Event | None = None,
) -> None:
    """Публикует видео в VK с ограничением одновременности и ретраями.

    on_retry(text) — вызывается перед каждой повторной попыткой с текстом для
    пользователя (например, «сервер VK не отвечает, повторю через …»).
    cancel_event — флаг отмены: при отмене корутины он взводится, и поток
    публикации останавливается на ближайшей контрольной точке.

    - семафор на пользователя: запросы одного юзера к VK не идут лавиной, даже
      если в один слот попало много его роликов — они выстраиваются в очередь.
      Разные пользователи друг друга НЕ ждут (лимит VK — по токену);
    - ретрай с экспоненциальным backoff на временные ошибки VK (rate limit /
      flood / internal) и сетевые сбои. Пауза между попытками — ВНЕ семафора,
      чтобы ожидание не блокировало публикации других роликов того же юзера.
    """
    loop = asyncio.get_running_loop()
    semaphore = _user_publish_semaphore(telegram_id)
    last_exc: Exception | None = None
    for attempt in range(1, VK_PUBLISH_RETRIES + 1):
        async with semaphore:
            try:
                await loop.run_in_executor(
                    _vk_executor,
                    lambda: upload_to_vk(
                        vk_token, vk_group_id, file_path, title, description,
                        cancel_event=cancel_event,
                    ),
                )
                return
            except asyncio.CancelledError:
                if cancel_event is not None:
                    cancel_event.set()
                raise
            except VKError as exc:
                last_exc = exc
                retryable = (
                    (exc.network or exc.code in VK_RETRYABLE_ERROR_CODES)
                    and not exc.ambiguous  # ролик мог выйти — повтор дал бы дубль
                )
                if not retryable or attempt == VK_PUBLISH_RETRIES:
                    raise

        # Пауза перед повтором — вне семафора: пропуск освобождён, другие ролики
        # этого юзера могут публиковаться, пока текущий ждёт следующей попытки.
        delay = VK_RETRY_BASE_DELAY * (2 ** (attempt - 1)) + random.uniform(0, 2)
        logger.warning(
            "Публикация в VK не удалась (попытка %s/%s): %s. Повтор через %.1f c",
            attempt, VK_PUBLISH_RETRIES, last_exc, delay,
        )
        if on_retry is not None:
            if _is_network_error(last_exc):
                reason = "🌐 Сервер VK не отвечает."
            else:
                reason = "⏳ VK временно ограничил запросы."
            try:
                await on_retry(
                    f"{reason} Повторю попытку через {delay:.0f} с "
                    f"({attempt + 1} из {VK_PUBLISH_RETRIES})…"
                )
            except Exception:
                logger.debug("on_retry: не удалось уведомить пользователя", exc_info=True)
        await asyncio.sleep(delay)


# Понятные объяснения частых кодов ошибок VK при публикации. Технический
# текст ошибки по-прежнему сохраняется в /errors.
_VK_PUBLISH_ERROR_TEXTS = {
    5: (
        "VK отклонил токен — он недействителен или истёк.\n"
        f"Получи новый токен кнопкой «{BTN_TOKEN}» и опубликуй заново."
    ),
    6: "VK ограничил частоту запросов. Подожди пару минут и попробуй снова.",
    9: "VK ограничил количество публикаций (flood control). Попробуй позже.",
    14: (
        "VK требует ввести капчу для этого аккаунта. Зайди в VK с браузера, "
        f"сделай пару действий вручную, затем получи новый токен («{BTN_TOKEN}»)."
    ),
    15: "Нет доступа к публикации в этой группе — проверь, что ты её администратор или редактор.",
    17: f"VK требует подтвердить вход. Зайди в VK с браузера и получи новый токен («{BTN_TOKEN}»).",
    27: f"Этот токен не подходит — нужен токен пользователя, а не сообщества. Задай его кнопкой «{BTN_TOKEN}».",
    203: "Нет доступа к группе — проверь, что ты её администратор и группа не заблокирована.",
    204: "Нет прав на загрузку видео в эту группу — проверь права администратора.",
    214: "Публикация на стене этой группы запрещена — проверь настройки стены.",
    3001: "VK слишком долго обрабатывает ролик. Попробуй опубликовать ещё раз чуть позже.",
}


def _format_error(
    exc: Exception,
    stage: str | None,
    group_name: str | None,
    platform: str | None = None,
) -> str:
    """Готовит человекочитаемое сообщение об ошибке для пользователя.

    Сетевые сбои («сервер не отвечает») показываем понятным текстом с указанием,
    чей сервер недоступен: при скачивании — платформы-источника, иначе — VK.
    Технические подробности остаются в /errors.
    """
    target = f" при публикации в «{group_name}»" if group_name else ""
    if isinstance(exc, VKError) and exc.ambiguous:
        return (
            f"⚠️ VK не ответил на запрос публикации{target}.\n"
            "Ролик мог успеть выйти — проверь группу, прежде чем публиковать заново "
            "(повторять автоматически не стал, чтобы не было дубля)."
        )
    if isinstance(exc, VKError) and exc.code in _VK_PUBLISH_ERROR_TEXTS:
        return f"❌ Не удалось{target or ' опубликовать'}.\n\n{_VK_PUBLISH_ERROR_TEXTS[exc.code]}"
    if _is_network_error(exc):
        if stage == "скачивание" and platform and not isinstance(exc, VKError):
            service = PLATFORM_LABELS.get(platform, platform)
        else:
            service = "VK"
        return f"❌ Не удалось{target or ' выполнить действие'}.\n\n{_unreachable_text(service)}"
    where = f" на этапе «{stage}»" if stage else ""
    return f"❌ Ошибка{where}{target}:\n{exc}"


def _record_error(
    telegram_id: int,
    exc: Exception,
    *,
    stage: str | None = None,
    platform: str | None = None,
    url: str | None = None,
    vk_group_id: int | None = None,
    vk_group_name: str | None = None,
) -> None:
    """Пишет ошибку в БД (токены маскируются внутри db.log_error)."""
    code = exc.code if isinstance(exc, VKError) else None
    eff_stage = stage or (exc.stage if isinstance(exc, VKError) else None)
    db.log_error(
        telegram_id,
        stage=eff_stage,
        platform=platform,
        url=url,
        vk_group_id=vk_group_id,
        vk_group_name=vk_group_name,
        error_code=code,
        message=str(exc),
        traceback="".join(
            tb_module.format_exception(type(exc), exc, exc.__traceback__)
        ),
    )


async def _notify(bot_, chat_id: int, text: str, **kwargs) -> None:
    """Best-effort сообщение пользователю.

    Сбой отправки в Telegram (таймаут, пользователь заблокировал бота) не должен
    обрывать публикацию в VK — раньше из-за неотправленного «Скачиваю…» ролик
    не публиковался вовсе.
    """
    try:
        await bot_.send_message(chat_id, text, **kwargs)
    except Exception:
        logger.warning("Не удалось отправить сообщение chat_id=%s", chat_id, exc_info=True)


async def _scheduled_upload_job(context: ContextTypes.DEFAULT_TYPE) -> None:
    """PTB job: вызывается в момент запланированной публикации.

    Скачивает видео по URL и публикует в VK — скачивание откладывается до этого
    момента, чтобы не хранить файлы на диске всё время ожидания.
    """
    data = context.job.data
    chat_id = data["chat_id"]
    telegram_id = data.get("telegram_id", chat_id)
    url = data["url"]
    platform = data.get("platform")
    group_name = data.get("vk_group_name") or "VK"
    vk_group_id = data.get("vk_group_id")
    description = data.get("description", "")
    file_path: str | None = None
    stage = "скачивание"
    # Удалять ли строку из scheduled_posts по завершении. Если бота остановили
    # посреди СКАЧИВАНИЯ — оставляем, после рестарта публикация восстановится.
    # Если уже шла публикация — удаляем: ролик мог выйти, повтор дал бы дубль.
    drop_row = True

    try:
        vk_token = db.get_vk_token(telegram_id)
        if not vk_token:
            await _notify(
                context.bot, chat_id,
                f"❌ Не удалось опубликовать в «{group_name}»: VK токен не найден. "
                f"Задай токен кнопкой «{BTN_TOKEN}» и запланируй публикацию заново.",
            )
            return

        await _notify(context.bot, chat_id, f"📥 Скачиваю видео для публикации в «{group_name}»…")

        try:
            file_path, title = await _download_video(platform, url, vk_token)
        except asyncio.TimeoutError:
            raise RuntimeError(
                f"Скачивание заняло больше {DOWNLOAD_TIMEOUT} с — превышен таймаут."
            )

        stage = "публикация"
        await _notify(context.bot, chat_id, f"📤 Публикую в «{group_name}» (id {vk_group_id})…")

        async def notify_retry(text: str) -> None:
            await _notify(context.bot, chat_id, text)

        await _publish_to_vk(
            telegram_id, vk_token, vk_group_id, file_path, title, description,
            on_retry=notify_retry,
            cancel_event=threading.Event(),
        )
        await _notify(context.bot, chat_id, f"✅ Видео опубликовано в «{group_name}»!")

    except asyncio.CancelledError:
        # Бота останавливают посреди работы.
        drop_row = stage != "скачивание"
        logger.warning(
            "Отложенная публикация прервана остановкой бота (этап %s, запись %s)",
            stage, "удалена" if drop_row else "сохранена для восстановления",
        )
        raise
    except Exception as exc:
        logger.exception("Ошибка отложенной публикации chat_id=%s", chat_id)
        stage = exc.stage if isinstance(exc, VKError) else stage
        _record_error(
            telegram_id, exc,
            stage=stage,
            platform=platform,
            url=url,
            vk_group_id=vk_group_id,
            vk_group_name=group_name,
        )
        await _notify(
            context.bot, chat_id,
            _format_error(exc, stage, group_name, platform) + "\n\nℹ️ Подробности — в /errors",
        )
    finally:
        post_id = data.get("scheduled_post_id")
        if post_id is not None and drop_row:
            try:
                db.delete_scheduled_post(post_id)
            except Exception:
                logger.exception("Не удалось удалить отложенную публикацию #%s из БД", post_id)
        _remove_file(file_path)


def _remove_file(path: str | None) -> None:
    if path and os.path.exists(path):
        try:
            os.remove(path)
        except OSError:
            logger.warning("Не удалось удалить временный файл %s", path, exc_info=True)


async def _restore_scheduled_posts(app: Application) -> None:
    """Восстанавливает отложенные публикации из БД после перезапуска бота.

    job_queue хранится только в памяти — без этого все запланированные ролики
    терялись бы при рестарте. Видео скачивается в момент публикации, поэтому
    файлы на диске не нужны — достаточно URL из БД.

    Просроченные (время наступило, пока бот лежал) публикуем сразу, разнося
    по RESTORE_SPREAD_SECONDS, чтобы не уйти в VK залпом.
    """
    posts = db.get_scheduled_posts()
    if not posts:
        return

    now = datetime.now(MOSCOW_TZ)
    overdue_index = 0
    restored = 0

    for post in posts:
        publish_at = datetime.fromtimestamp(post["publish_at"], tz=MOSCOW_TZ)
        if publish_at > now:
            when = publish_at
        else:
            when = timedelta(seconds=5 + overdue_index * RESTORE_SPREAD_SECONDS)
            overdue_index += 1

        app.job_queue.run_once(
            _scheduled_upload_job,
            when=when,
            data={
                "chat_id": post["chat_id"],
                "telegram_id": post["telegram_id"],
                "url": post["url"],
                "platform": post["platform"],
                "description": post["description"],
                "vk_group_id": post["vk_group_id"],
                "vk_group_name": post["vk_group_name"],
                "scheduled_post_id": post["id"],
            },
            name=f"restored_{post['id']}",
        )
        restored += 1

    if restored:
        logger.info("Восстановлено отложенных публикаций: %s (просрочено: %s)", restored, overdue_index)


# ─── Keyboards ────────────────────────────────────────────────────────────────

def _group_label(g) -> str:
    """Текст кнопки группы. Пустое название (сохранённое до валидации) давало
    кнопку без текста — Telegram отклонял всю клавиатуру."""
    return (g["name"] or "").strip() or f"Группа {g['vk_group_id']}"


def _template_label(t) -> str:
    return (t["title"] or "").strip() or f"Заготовка #{t['id']}"


def build_time_keyboard() -> InlineKeyboardMarkup:
    now = datetime.now(MOSCOW_TZ)
    today = now.date()
    tomorrow = today + timedelta(days=1)

    keyboard = [[InlineKeyboardButton("⚡ Сейчас", callback_data="now")]]

    today_btns = []
    for h in TIME_SLOTS:
        slot = MOSCOW_TZ.localize(datetime(today.year, today.month, today.day, h))
        if slot > now + timedelta(minutes=5):
            today_btns.append(
                InlineKeyboardButton(f"Сегодня {h}:00", callback_data=f"slot_{today.isoformat()}_{h}")
            )
    for i in range(0, len(today_btns), TIME_SLOTS_ROW_SIZE):
        keyboard.append(today_btns[i:i + TIME_SLOTS_ROW_SIZE])

    tomorrow_btns = [
        InlineKeyboardButton(f"Завтра {h}:00", callback_data=f"slot_{tomorrow.isoformat()}_{h}")
        for h in TIME_SLOTS
    ]
    for i in range(0, len(tomorrow_btns), TIME_SLOTS_ROW_SIZE):
        keyboard.append(tomorrow_btns[i:i + TIME_SLOTS_ROW_SIZE])
    keyboard.append([InlineKeyboardButton("✏️ Своё время", callback_data="custom")])
    return InlineKeyboardMarkup(keyboard)


def build_groups_select_keyboard(telegram_id: int, page: int = 0) -> InlineKeyboardMarkup:
    all_groups = db.get_groups(telegram_id)
    total = len(all_groups)
    start = page * KEYBOARD_PAGE_SIZE
    page_groups = all_groups[start:start + KEYBOARD_PAGE_SIZE]
    rows = [
        [InlineKeyboardButton(_group_label(g), callback_data=f"upgroup_{g['id']}")]
        for g in page_groups
    ]
    nav = []
    if page > 0:
        nav.append(InlineKeyboardButton("⬅️", callback_data=f"upgroup_pg_{page - 1}"))
    if start + KEYBOARD_PAGE_SIZE < total:
        nav.append(InlineKeyboardButton("➡️", callback_data=f"upgroup_pg_{page + 1}"))
    if nav:
        rows.append(nav)
    return InlineKeyboardMarkup(rows)


def build_desc_keyboard(telegram_id: int, page: int = 0) -> InlineKeyboardMarkup:
    all_templates = db.get_templates(telegram_id)
    total = len(all_templates)
    start = page * KEYBOARD_PAGE_SIZE
    page_templates = all_templates[start:start + KEYBOARD_PAGE_SIZE]
    rows = [
        [InlineKeyboardButton(f"📝 {_template_label(t)}", callback_data=f"updesc_tpl_{t['id']}")]
        for t in page_templates
    ]
    nav = []
    if page > 0:
        nav.append(InlineKeyboardButton("⬅️", callback_data=f"updesc_pg_{page - 1}"))
    if start + KEYBOARD_PAGE_SIZE < total:
        nav.append(InlineKeyboardButton("➡️", callback_data=f"updesc_pg_{page + 1}"))
    if nav:
        rows.append(nav)
    rows.append([InlineKeyboardButton("✏️ Написать своё", callback_data="updesc_custom")])
    rows.append([InlineKeyboardButton("➖ Без описания", callback_data="updesc_none")])
    return InlineKeyboardMarkup(rows)


def build_groups_manage_keyboard(telegram_id: int, page: int = 0) -> InlineKeyboardMarkup:
    all_groups = db.get_groups(telegram_id)
    total = len(all_groups)
    start = page * KEYBOARD_PAGE_SIZE
    page_groups = all_groups[start:start + KEYBOARD_PAGE_SIZE]
    rows = []
    for g in page_groups:
        rows.append([InlineKeyboardButton(f"{_group_label(g)} (id {g['vk_group_id']})", callback_data="noop")])
        rows.append([
            InlineKeyboardButton("✏️ Переименовать", callback_data=f"g_rename_{g['id']}"),
            InlineKeyboardButton("🗑 Удалить", callback_data=f"g_del_{g['id']}"),
        ])
    nav = []
    if page > 0:
        nav.append(InlineKeyboardButton("⬅️", callback_data=f"g_pg_{page - 1}"))
    if start + KEYBOARD_PAGE_SIZE < total:
        nav.append(InlineKeyboardButton("➡️", callback_data=f"g_pg_{page + 1}"))
    if nav:
        rows.append(nav)
    rows.append([InlineKeyboardButton("➕ Добавить группу", callback_data="g_add")])
    return InlineKeyboardMarkup(rows)


def build_token_keyboard(has_token: bool) -> InlineKeyboardMarkup:
    rows = [[
        InlineKeyboardButton(
            "✏️ Изменить токен" if has_token else "➕ Задать токен",
            callback_data="settoken_change",
        )
    ]]
    if has_token:
        rows.append([InlineKeyboardButton("🗑 Удалить токен", callback_data="settoken_delete")])
    return InlineKeyboardMarkup(rows)


def _mask_token(token: str) -> str:
    if len(token) <= 12:
        return "•" * len(token)
    return f"{token[:6]}…{token[-4:]}"


def build_templates_manage_keyboard(telegram_id: int, page: int = 0) -> InlineKeyboardMarkup:
    all_templates = db.get_templates(telegram_id)
    total = len(all_templates)
    start = page * KEYBOARD_PAGE_SIZE
    page_templates = all_templates[start:start + KEYBOARD_PAGE_SIZE]
    rows = []
    for t in page_templates:
        rows.append([InlineKeyboardButton(f"📝 {_template_label(t)}", callback_data="noop")])
        rows.append([
            InlineKeyboardButton("✏️ Изменить", callback_data=f"t_edit_{t['id']}"),
            InlineKeyboardButton("🗑 Удалить", callback_data=f"t_del_{t['id']}"),
        ])
    nav = []
    if page > 0:
        nav.append(InlineKeyboardButton("⬅️", callback_data=f"t_pg_{page - 1}"))
    if start + KEYBOARD_PAGE_SIZE < total:
        nav.append(InlineKeyboardButton("➡️", callback_data=f"t_pg_{page + 1}"))
    if nav:
        rows.append(nav)
    rows.append([InlineKeyboardButton("➕ Добавить заготовку", callback_data="t_add")])
    return InlineKeyboardMarkup(rows)


# ─── Core upload flow ────────────────────────────────────────────────────────

async def do_upload(
    chat_id: int,
    context: ContextTypes.DEFAULT_TYPE,
    job: dict,
    status_message=None,
    task_key: tuple[int, int] | None = None,
):
    """Немедленная публикация: скачать → опубликовать в VK прямо сейчас.

    Только для случая «Сейчас» — отложенные публикации идут через _scheduled_upload_job.
    job — снимок данных из _snapshot_job, не зависит от context.user_data.
    """
    url = job["url"]
    platform = job["platform"]
    description = job.get("description", "")
    vk_token = job["vk_token"]
    vk_group_id = job["vk_group_id"]
    vk_group_name = job.get("vk_group_name") or "VK"
    telegram_id = job.get("telegram_id", chat_id)
    file_path: str | None = None
    stage = "скачивание"
    cancel_event = threading.Event()

    async def set_status(text: str, final: bool = False):
        """Обновляет статус. Best-effort: сбой Telegram (таймаут, сообщение
        удалено, «not modified») не должен обрывать саму публикацию."""
        nonlocal status_message
        markup = None if final else CANCEL_MARKUP
        if status_message:
            try:
                await status_message.edit_text(text, reply_markup=markup)
                return
            except BadRequest as exc:
                if "not modified" in str(exc).lower():
                    return
                logger.info("Статусное сообщение недоступно (%s) — шлю новое", exc)
            except Exception:
                logger.warning("Не удалось обновить статус chat_id=%s", chat_id, exc_info=True)
                return
        try:
            status_message = await context.bot.send_message(chat_id, text, reply_markup=markup)
        except Exception:
            logger.warning("Не удалось отправить статус chat_id=%s", chat_id, exc_info=True)

    try:
        await set_status(f"⏳ Скачиваю видео с {PLATFORM_LABELS.get(platform, platform)}...")

        try:
            file_path, title = await _download_video(platform, url, vk_token)
        except asyncio.TimeoutError:
            raise RuntimeError(
                f"Скачивание заняло больше {DOWNLOAD_TIMEOUT} с — превышен таймаут."
            )

        size_mb = os.path.getsize(file_path) / (1024 * 1024)
        stage = "публикация"
        await set_status(
            f"📤 Публикую в «{vk_group_name}» (id {vk_group_id})…\nРазмер: {size_mb:.1f} МБ"
        )
        await _publish_to_vk(
            telegram_id, vk_token, vk_group_id, file_path, title, description,
            on_retry=set_status,
            cancel_event=cancel_event,
        )
        await set_status(f"✅ Опубликовано в «{vk_group_name}»!", final=True)

    except (asyncio.CancelledError, UploadCancelled) as exc:
        # Останавливаем поток публикации на ближайшей контрольной точке.
        cancel_event.set()
        text = "❌ Загрузка отменена"
        if stage == "публикация":
            text += (
                "\n\nЕсли отмена пришлась на самый последний шаг, VK мог успеть "
                "опубликовать ролик — проверь группу."
            )
        await set_status(text, final=True)
        if isinstance(exc, asyncio.CancelledError):
            raise
    except Exception as exc:
        logger.exception("Ошибка обработки %s", url)
        eff_stage = exc.stage if isinstance(exc, VKError) else stage
        _record_error(
            telegram_id, exc,
            stage=eff_stage,
            platform=platform,
            url=url,
            vk_group_id=vk_group_id,
            vk_group_name=vk_group_name,
        )
        await set_status(
            _format_error(exc, eff_stage, vk_group_name, platform) + "\n\nℹ️ Подробности — в /errors",
            final=True,
        )
    finally:
        if task_key is not None:
            _upload_tasks.pop(task_key, None)
        _remove_file(file_path)


def _snapshot_job(context: ContextTypes.DEFAULT_TYPE) -> dict:
    """Фиксирует данные текущего потока, чтобы фоновая загрузка не зависела от
    последующих изменений context.user_data (новый поток / параллельная загрузка)."""
    return {
        "url": context.user_data["url"],
        "platform": context.user_data["platform"],
        "description": context.user_data.get("description", ""),
        "vk_token": context.user_data["vk_token"],
        "vk_group_id": context.user_data["vk_group_id"],
        "vk_group_name": context.user_data.get("vk_group_name", ""),
        "telegram_id": context.user_data.get("telegram_id"),
    }


def _start_upload(chat_id: int, context: ContextTypes.DEFAULT_TYPE, status_msg) -> None:
    """Запускает немедленную публикацию в фоне — диспетчер бота не блокируется.

    application.create_task, а не asyncio.create_task: исключения задачи попадают
    в обработчик ошибок бота (_on_error), а не теряются в консоли с
    «Task exception was never retrieved».
    """
    job = _snapshot_job(context)
    task_key = (chat_id, status_msg.message_id)
    task = context.application.create_task(
        do_upload(chat_id, context, job, status_message=status_msg, task_key=task_key),
        name=f"upload_{chat_id}_{status_msg.message_id}",
    )
    _upload_tasks[task_key] = task


async def _schedule_post(
    chat_id: int,
    context: ContextTypes.DEFAULT_TYPE,
    publish_ts: int,
    edit_message=None,
) -> None:
    """Ставит публикацию в очередь без скачивания видео.

    Сохраняет URL в БД, регистрирует job — скачивание и публикация произойдут
    в момент publish_ts. edit_message — объект Message для edit_text (если есть).
    """
    jitter = random.randint(0, PUBLISH_JITTER_SECONDS)
    dt = datetime.fromtimestamp(publish_ts + jitter, tz=MOSCOW_TZ)

    ud = context.user_data
    telegram_id = ud.get("telegram_id", chat_id)
    vk_token = ud["vk_token"]
    vk_group_id = ud["vk_group_id"]
    vk_group_name = ud.get("vk_group_name", "VK")
    url = ud["url"]
    platform = ud["platform"]
    description = ud.get("description", "")

    post_id = db.add_scheduled_post(
        telegram_id=telegram_id,
        chat_id=chat_id,
        url=url,
        platform=platform,
        description=description,
        vk_group_id=vk_group_id,
        vk_group_name=vk_group_name,
        publish_at=int(dt.timestamp()),
    )

    context.job_queue.run_once(
        _scheduled_upload_job,
        when=dt,
        data={
            "chat_id": chat_id,
            "telegram_id": telegram_id,
            "url": url,
            "platform": platform,
            "description": description,
            "vk_token": vk_token,
            "vk_group_id": vk_group_id,
            "vk_group_name": vk_group_name,
            "scheduled_post_id": post_id,
        },
        name=f"scheduled_{post_id}",
    )

    text = (
        f"✅ Видео поставлено в очередь!\n\n"
        f"📅 Опубликую примерно {dt.strftime('%d.%m.%Y в %H:%M')} МСК "
        f"в «{vk_group_name}»."
    )
    if edit_message:
        await edit_message.edit_text(text)
    else:
        await context.bot.send_message(chat_id, text)


# ─── /start ───────────────────────────────────────────────────────────────────

async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    db.ensure_user(update.effective_user.id)
    await update.message.reply_text(
        "Привет! Я скачиваю видео из TikTok, Instagram Reels, Likee, YouTube Shorts и VK и публикую в твою группу VK.\n\n"
        "Кнопки внизу:\n"
        f"{BTN_TOKEN} — посмотреть / изменить / удалить VK токен\n"
        f"{BTN_GROUPS} — управление группами VK\n"
        f"{BTN_TEMPLATES} — заготовки описаний\n\n"
        "Чтобы опубликовать видео — просто пришли ссылку на TikTok, Instagram Reels, Likee, YouTube Shorts или VK.",
        reply_markup=main_keyboard(),
    )


# ─── Upload conversation ────────────────────────────────────────────────────

async def handle_link(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    found = extract_platform_url(_message_text(update.message))
    if not found:
        await update.message.reply_text(
            "Не распознал ссылку. Поддерживаются:\n"
            "• TikTok (tiktok.com)\n"
            "• Instagram Reels (instagram.com/reel/…)\n"
            "• Likee (likee.video)\n"
            "• YouTube Shorts (youtube.com/shorts…, youtu.be)\n"
            "• VK видео и клипы (vk.com/video…, vk.com/clip…)"
        )
        return ConversationHandler.END
    url, platform = found

    telegram_id = update.effective_user.id
    db.ensure_user(telegram_id)

    vk_token = db.get_vk_token(telegram_id)
    if not vk_token:
        await update.message.reply_text("Сначала задай VK токен командой /settoken")
        return ConversationHandler.END

    groups = db.get_groups(telegram_id)
    if not groups:
        await update.message.reply_text("Сначала добавь хотя бы одну группу командой /groups")
        return ConversationHandler.END

    context.user_data["url"] = url
    context.user_data["platform"] = platform
    context.user_data["vk_token"] = vk_token
    context.user_data["telegram_id"] = telegram_id
    await update.message.reply_text(
        f"Ссылка {PLATFORM_LABELS[platform]} принята.\nВ какую группу опубликовать?",
        reply_markup=build_groups_select_keyboard(telegram_id),
    )
    return UP_GROUP


def _own_group(group_row_id: int, telegram_id: int):
    """Группа из БД, только если она принадлежит этому пользователю.

    id строки приходит из callback_data, а её можно подделать — без проверки
    владельца можно было удалить/переименовать/выбрать чужую группу."""
    group = db.get_group(group_row_id)
    return group if group and group["telegram_id"] == telegram_id else None


def _own_template(template_id: int, telegram_id: int):
    """Заготовка из БД, только если она принадлежит этому пользователю."""
    template = db.get_template(template_id)
    return template if template and template["telegram_id"] == telegram_id else None


async def handle_group_choice(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    query = update.callback_query
    await _safe_answer(query)

    if query.data.startswith("upgroup_pg_"):
        page = int(query.data[len("upgroup_pg_"):])
        telegram_id = update.effective_user.id
        platform = context.user_data.get("platform", "")
        await query.edit_message_text(
            f"Ссылка {PLATFORM_LABELS.get(platform, platform)} принята.\nВ какую группу опубликовать?",
            reply_markup=build_groups_select_keyboard(telegram_id, page),
        )
        return UP_GROUP

    group_row_id = int(query.data.split("_")[1])
    group = _own_group(group_row_id, update.effective_user.id)
    if not group:
        await query.edit_message_text("Группа не найдена. Начни заново — пришли ссылку.")
        return ConversationHandler.END

    context.user_data["vk_group_id"] = group["vk_group_id"]
    context.user_data["vk_group_name"] = group["name"]
    telegram_id = update.effective_user.id
    await query.edit_message_text(
        f"Группа: {group['name']}\n\nВыбери описание:",
        reply_markup=build_desc_keyboard(telegram_id),
    )
    return UP_DESC


async def handle_desc_choice(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    query = update.callback_query
    await _safe_answer(query)
    data = query.data

    if data.startswith("updesc_pg_"):
        page = int(data[len("updesc_pg_"):])
        telegram_id = update.effective_user.id
        group_name = context.user_data.get("vk_group_name", "")
        await query.edit_message_text(
            f"Группа: {group_name}\n\nВыбери описание:",
            reply_markup=build_desc_keyboard(telegram_id, page),
        )
        return UP_DESC

    if data == "updesc_custom":
        await query.edit_message_text("Введи текст описания для публикации:")
        return UP_CUSTOM_DESC

    if data == "updesc_none":
        context.user_data["description"] = ""
    else:  # updesc_tpl_<id>
        template_id = int(data.rsplit("_", 1)[1])
        template = _own_template(template_id, update.effective_user.id)
        if template is None:
            # Заготовку удалили, пока выбирали — раньше молча уходило пустое описание.
            await query.edit_message_text(
                "⚠️ Эта заготовка уже удалена. Выбери другое описание:",
                reply_markup=build_desc_keyboard(update.effective_user.id),
            )
            return UP_DESC
        context.user_data["description"] = template["body"]

    await query.edit_message_text("Когда опубликовать видео?", reply_markup=build_time_keyboard())
    return UP_TIME


async def handle_custom_desc(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    context.user_data["description"] = update.message.text
    await update.message.reply_text("Когда опубликовать видео?", reply_markup=build_time_keyboard())
    return UP_TIME


async def handle_time_choice(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    query = update.callback_query
    await _safe_answer(query)
    data = query.data
    chat_id = query.message.chat_id

    if data == "now":
        status_msg = await query.edit_message_text("⏳ Начинаю...", reply_markup=CANCEL_MARKUP)
        _start_upload(chat_id, context, status_msg)
        return ConversationHandler.END

    if data == "custom":
        await query.edit_message_text(
            "Введи дату и время в формате ДД.ММ.ГГГГ ЧЧ:ММ\nНапример: 25.12.2024 18:30 (время московское)"
        )
        return UP_CUSTOM_TIME

    # slot_YYYY-MM-DD_HH
    _, date_str, hour_str = data.split("_")
    d = datetime.strptime(date_str, "%Y-%m-%d").date()
    h = int(hour_str)
    scheduled_time = MOSCOW_TZ.localize(datetime(d.year, d.month, d.day, h))
    await _schedule_post(chat_id, context, int(scheduled_time.timestamp()), edit_message=query.message)
    return ConversationHandler.END


async def handle_custom_time(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    text = update.message.text.strip()
    try:
        scheduled_time = MOSCOW_TZ.localize(datetime.strptime(text, "%d.%m.%Y %H:%M"))
    except ValueError:
        await update.message.reply_text(
            "Неверный формат. Введи так: ДД.ММ.ГГГГ ЧЧ:ММ\nНапример: 25.12.2024 18:30"
        )
        return UP_CUSTOM_TIME

    if scheduled_time <= datetime.now(MOSCOW_TZ) + timedelta(minutes=1):
        await update.message.reply_text("Это время уже прошло. Введи время в будущем:")
        return UP_CUSTOM_TIME

    await _schedule_post(update.message.chat_id, context, int(scheduled_time.timestamp()))
    return ConversationHandler.END


async def handle_cancel_upload(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    await _safe_answer(query, "Отмена...")
    # Кнопка «Отмена» висит на том же сообщении, по которому задача и ключуется.
    # Ключ — (chat_id, message_id): message_id уникален лишь внутри чата, поэтому
    # без chat_id отмена одного юзера могла бы попасть в чужую загрузку.
    task_key = (query.message.chat_id, query.message.message_id)
    task = _upload_tasks.get(task_key)
    if task and not task.done():
        task.cancel()
    else:
        await query.edit_message_text("Нечего отменять")


# ─── Меню (кнопки над клавиатурой) ────────────────────────────────────────────

async def _reply_groups(update: Update, telegram_id: int) -> None:
    await update.message.reply_text(
        "Твои группы VK:",
        reply_markup=build_groups_manage_keyboard(telegram_id),
    )


async def _reply_templates(update: Update, telegram_id: int) -> None:
    await update.message.reply_text(
        "Твои заготовки описаний:",
        reply_markup=build_templates_manage_keyboard(telegram_id),
    )


async def main_menu_button(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    text = update.message.text
    telegram_id = update.effective_user.id
    db.ensure_user(telegram_id)

    if text == BTN_GROUPS:
        await _reply_groups(update, telegram_id)
    elif text == BTN_TEMPLATES:
        await _reply_templates(update, telegram_id)
    elif text == BTN_TOKEN:
        await show_token_status(update, context)


async def show_token_status(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    token = db.get_vk_token(update.effective_user.id)
    if token:
        msg = f"🔑 Токен задан: {_mask_token(token)}\n(показан частично — в целях безопасности)"
    else:
        msg = "❌ Токен не задан."
    await update.message.reply_text(msg, reply_markup=build_token_keyboard(bool(token)))


async def handle_token_delete(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    await _safe_answer(query, "Токен удалён")
    db.clear_vk_token(update.effective_user.id)
    await query.edit_message_text("🗑 Токен удалён.", reply_markup=build_token_keyboard(False))


# ─── /settoken conversation ───────────────────────────────────────────────────

SETTOKEN_PROMPT = (
    "Пришли свой VK токен.\n\n"
    "Как получить через Kate Mobile:\n"
    "1. Открой браузер и перейди по ссылке:\n"
    "https://oauth.vk.com/authorize?client_id=2685278&scope=1073737727&redirect_uri=https://oauth.vk.com/blank.html&display=page&response_type=token\n"
    "2. Войди в VK и разреши доступ\n"
    "3. Скопируй access_token из адресной строки (между access_token= и &expires_in)"
)


async def cmd_settoken(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    await update.message.reply_text(SETTOKEN_PROMPT)
    return TOKEN_WAIT


async def settoken_from_button(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    query = update.callback_query
    await _safe_answer(query)
    await query.edit_message_text(SETTOKEN_PROMPT)
    return TOKEN_WAIT


async def handle_token(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    token = update.message.text.strip()

    # Частая ошибка: вставляют всю адресную строку
    # https://oauth.vk.com/blank.html#access_token=vk1.a...&expires_in=0&user_id=1
    # Раньше она целиком (длинная, без пробелов) сохранялась как «токен», и потом
    # все запросы к VK падали — в т.ч. поиск групп по ссылке. Вырезаем сам токен.
    m = re.search(r"access_token=([^&\s#]+)", token)
    if m:
        token = m.group(1)
    elif _VK_COMMUNITY_FILTER.filter(update.message):
        # Прислали ссылку на сообщество — видимо, хотят добавить группу.
        await update.message.reply_text("Ввод токена отменён — это ссылка на сообщество.")
        await handle_community_link(update, context)
        return ConversationHandler.END

    # Пользователь, видимо, передумал вводить токен и прислал ссылку/нажал кнопку меню —
    # не сохраняем это как токен (фикс бага, когда ссылка попадала в токен).
    if extract_platform_url(token) or token in MENU_BUTTON_TEXTS:
        await update.message.reply_text(
            "Похоже, это не VK токен — ввод токена отменён.\n"
            f"Если хотел задать токен, нажми «{BTN_TOKEN}» и пришли его."
        )
        return ConversationHandler.END

    # Новый формат VK токена: vk1.a.XXXX (минимум 20 символов после префикса)
    # Старый формат: длинная строка без пробелов (85+ символов)
    is_new = token.startswith("vk1.a.") and len(token) >= 26
    is_old = (
        len(token) >= 85
        and not any(ch.isspace() for ch in token)
        and "/" not in token  # ссылка — не токен
    )
    if not (is_new or is_old):
        await update.message.reply_text(
            "❌ Это не похоже на VK токен.\n\n"
            "VK токен выглядит так:\n"
            "<code>vk1.a.AbCdEfGhIj...</code>\n\n"
            "Пришли правильный токен или нажми /cancel.",
            parse_mode="HTML",
        )
        return TOKEN_WAIT

    # Проверяем токен сразу, а не при первой публикации (которая может быть
    # отложенной на завтра — и тогда пользователь узнает о проблеме слишком поздно).
    await update.message.reply_text("⏳ Проверяю токен в VK…")
    try:
        response, err = await _run_vk_lookup(_vk_call, "users.get", token)
    except asyncio.TimeoutError:
        response, err = None, {"error_code": None, "error_msg": "VK не ответил вовремя"}

    if err and err.get("error_code") in (5, 1116):
        await update.message.reply_text(
            "❌ VK отклонил этот токен — он недействителен или уже истёк.\n"
            "Получи новый по инструкции выше и пришли его, или нажми /cancel."
        )
        return TOKEN_WAIT

    db.set_vk_token(update.effective_user.id, token)
    if err:
        note = (
            _unreachable_text("VK") if err.get("error_code") is None
            else f"VK вернул ошибку {err.get('error_code')}: {err.get('error_msg')}"
        )
        text = f"✅ Токен сохранён, но проверить его сейчас не получилось.\n\n{note}"
    elif isinstance(response, list) and response:
        user = response[0]
        who = f"{user.get('first_name', '')} {user.get('last_name', '')}".strip()
        text = "✅ Токен сохранён и проверен" + (f" (аккаунт: {who})." if who else ".")
    else:
        text = (
            "✅ Токен сохранён, но VK не вернул данные пользователя — похоже, это токен "
            "сообщества. Для публикации клипов нужен токен пользователя (по инструкции выше)."
        )
    await update.message.reply_text(text, reply_markup=main_keyboard())
    return ConversationHandler.END


# ─── /groups conversation ─────────────────────────────────────────────────────

async def cmd_groups(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    telegram_id = update.effective_user.id
    db.ensure_user(telegram_id)
    await _reply_groups(update, telegram_id)
    return ConversationHandler.END


async def groups_button(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    query = update.callback_query
    data = query.data

    if data == "noop":
        await _safe_answer(query)
        return ConversationHandler.END

    if data.startswith("g_pg_"):
        await _safe_answer(query)
        page = int(data[len("g_pg_"):])
        await query.edit_message_text(
            "Твои группы VK:",
            reply_markup=build_groups_manage_keyboard(update.effective_user.id, page),
        )
        return ConversationHandler.END

    if data == "g_add":
        await _safe_answer(query)
        if db.count_groups(update.effective_user.id) >= MAX_GROUPS_PER_USER:
            await query.edit_message_text(
                f"❌ Достигнут лимит групп: максимум {MAX_GROUPS_PER_USER}.\n"
                "Удали ненужные группы, чтобы добавить новые.",
                reply_markup=build_groups_manage_keyboard(update.effective_user.id),
            )
            return ConversationHandler.END
        await query.edit_message_text(
            "Пришли ссылку на сообщество VK — ID определю сам.\n\n"
            "Например:\n"
            "• vk.com/club123456\n"
            "• vk.com/public123456\n"
            "• vk.com/my_group_name"
        )
        return G_ADD_ID

    if data.startswith("g_del_"):
        group = _own_group(int(data.rsplit("_", 1)[1]), update.effective_user.id)
        if group:
            db.delete_group(group["id"])
            await _safe_answer(query, "Удалено")
        else:
            await _safe_answer(query, "Группа уже удалена")
        await query.edit_message_text(
            "Твои группы VK:",
            reply_markup=build_groups_manage_keyboard(update.effective_user.id),
        )
        return ConversationHandler.END

    if data.startswith("g_rename_"):
        group = _own_group(int(data.rsplit("_", 1)[1]), update.effective_user.id)
        if not group:
            await _safe_answer(query, "Группа не найдена", show_alert=True)
            return ConversationHandler.END
        await _safe_answer(query)
        context.user_data["rename_group_id"] = group["id"]
        await query.edit_message_text("Введи новое название группы:")
        return G_RENAME

    await _safe_answer(query)
    return ConversationHandler.END


async def _lookup_group(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Определяет сообщество по тексту сообщения. Возвращает (group_id, name, error, note)."""
    vk_token = db.get_vk_token(update.effective_user.id)
    # Сразу даём знать, что ссылка принята: если VK отвечает медленно, раньше
    # пользователь минутами не видел никакой реакции и считал, что бот сломан.
    try:
        await update.message.reply_text("🔎 Ищу сообщество в VK…")
    except Exception:
        logger.warning("Не удалось отправить «Ищу сообщество»", exc_info=True)
    try:
        return await _run_vk_lookup(resolve_vk_group, vk_token, _message_text(update.message))
    except asyncio.TimeoutError:
        logger.warning("Поиск сообщества не уложился в %s с — VK не отвечает", VK_LOOKUP_DEADLINE)
        return None, None, _unreachable_text("VK"), None


def _no_name_text(group_id: int, note: str | None, prompt: str = "Введи название вручную:") -> str:
    """Сообщение «группа найдена, названия нет» — с причиной, если она известна
    (например, сервер VK не ответил)."""
    text = f"Сообщество найдено (id {group_id}), но название получить не удалось."
    if note:
        text += f"\n\n{note}"
    return f"{text}\n\n{prompt}"


async def _offer_group(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
    group_id: int,
    name: str | None,
    note: str | None = None,
) -> None:
    """Запоминает найденное сообщество и предлагает сохранить его кнопками."""
    context.user_data["pending_group_id"] = group_id
    context.user_data["pending_group_name"] = name
    if name:
        text = f"Нашёл сообщество: «{name}» (id {group_id})\nСохранить с этим именем?"
        rows = [
            [InlineKeyboardButton("✅ Сохранить", callback_data="g_confirmname")],
            [InlineKeyboardButton("✏️ Задать своё имя", callback_data="g_manualname")],
        ]
    else:
        text = _no_name_text(group_id, note, "Задай название вручную:")
        rows = [[InlineKeyboardButton("✏️ Ввести название", callback_data="g_manualname")]]
    await update.message.reply_text(text, reply_markup=InlineKeyboardMarkup(rows))


async def groups_add_id(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    found = extract_platform_url(_message_text(update.message))
    if found and found[1] != "vk":
        # Прислали ссылку на TikTok/YouTube/… — это видео, а не сообщество VK.
        await update.message.reply_text(
            f"Это ссылка на видео {PLATFORM_LABELS[found[1]]}, а не на сообщество VK — "
            "добавление группы отменено.\nЕсли хотел опубликовать видео, пришли ссылку ещё раз."
        )
        return ConversationHandler.END

    group_id, name, error, note = await _lookup_group(update, context)
    if error:
        await update.message.reply_text(error + "\n\nПопробуй ещё раз или /cancel.")
        return G_ADD_ID

    if name:
        await _offer_group(update, context, group_id, name)
        return G_ADD_CONFIRM

    context.user_data["pending_group_id"] = group_id
    context.user_data["pending_group_name"] = None
    await update.message.reply_text(_no_name_text(group_id, note))
    return G_ADD_NAME


async def handle_community_link(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Ссылка на сообщество, присланная вне диалога добавления группы.

    Раньше такая ссылка молча игнорировалась: если после «➕ Добавить группу»
    прошло больше 5 минут (conversation_timeout), бот перезапускался (состояние
    диалогов не сохраняется) или кнопку просто не нажали — бот не отвечал вовсе.
    Теперь сразу ищем сообщество и предлагаем его сохранить.
    """
    telegram_id = update.effective_user.id
    db.ensure_user(telegram_id)
    if db.count_groups(telegram_id) >= MAX_GROUPS_PER_USER:
        await update.message.reply_text(
            f"❌ Достигнут лимит групп: максимум {MAX_GROUPS_PER_USER}.\n"
            "Удали ненужные группы, чтобы добавить новые."
        )
        return
    group_id, name, error, note = await _lookup_group(update, context)
    if error:
        await update.message.reply_text(error)
        return
    await _offer_group(update, context, group_id, name, note)


async def groups_add_confirm(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    query = update.callback_query
    await _safe_answer(query)
    telegram_id = update.effective_user.id

    if context.user_data.get("pending_group_id") is None:
        # Кнопка от старого сообщения — данные уже использованы / потеряны.
        await query.edit_message_text("Это предложение устарело. Пришли ссылку на сообщество ещё раз.")
        return ConversationHandler.END

    if _group_limit_reached(telegram_id, context.user_data["pending_group_id"]):
        await query.edit_message_text(
            f"❌ Достигнут лимит групп: максимум {MAX_GROUPS_PER_USER}.\n"
            "Удали ненужные группы, чтобы добавить новые.",
            reply_markup=build_groups_manage_keyboard(telegram_id),
        )
        return ConversationHandler.END

    if query.data == "g_confirmname" and context.user_data.get("pending_group_name"):
        db.add_group(
            telegram_id,
            context.user_data.pop("pending_group_id"),
            context.user_data.pop("pending_group_name"),
        )
        await query.edit_message_text(
            "✅ Группа добавлена.\n\nТвои группы VK:",
            reply_markup=build_groups_manage_keyboard(telegram_id),
        )
        return ConversationHandler.END

    # g_manualname
    await query.edit_message_text("Введи название группы вручную:")
    return G_ADD_NAME


MAX_NAME_LENGTH = 60  # название группы/заготовки — это текст кнопки


def _clean_name(text: str | None) -> tuple[str | None, str | None]:
    """Проверяет название группы/заготовки. Возвращает (name, error).

    Пустое название (из одних пробелов) давало кнопку с пустым текстом —
    Telegram такую отклоняет, и меню «Группы»/«Описания» переставало открываться.
    """
    name = " ".join((text or "").split())
    if not name:
        return None, "Название не может быть пустым. Введи название ещё раз:"
    if len(name) > MAX_NAME_LENGTH:
        return None, (
            f"Слишком длинное название ({len(name)} симв.) — максимум {MAX_NAME_LENGTH}. "
            "Введи покороче:"
        )
    return name, None


def _group_limit_reached(telegram_id: int, vk_group_id: int) -> bool:
    """Лимит групп достигнут (повторное добавление той же группы — не новая)."""
    groups = db.get_groups(telegram_id)
    if any(g["vk_group_id"] == vk_group_id for g in groups):
        return False
    return len(groups) >= MAX_GROUPS_PER_USER


async def groups_add_name(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    telegram_id = update.effective_user.id
    name, error = _clean_name(update.message.text)
    if error:
        await update.message.reply_text(error)
        return G_ADD_NAME
    group_id = context.user_data.pop("pending_group_id", None)
    context.user_data.pop("pending_group_name", None)
    if group_id is None:
        await update.message.reply_text("Не нашёл, какую группу добавлять. Пришли ссылку на сообщество ещё раз.")
        return ConversationHandler.END
    if _group_limit_reached(telegram_id, group_id):
        await update.message.reply_text(
            f"❌ Достигнут лимит групп: максимум {MAX_GROUPS_PER_USER}.\n"
            "Удали ненужные группы, чтобы добавить новые."
        )
        return ConversationHandler.END
    db.add_group(telegram_id, group_id, name)
    await update.message.reply_text(
        "✅ Группа добавлена.\n\nТвои группы VK:",
        reply_markup=build_groups_manage_keyboard(telegram_id),
    )
    return ConversationHandler.END


async def groups_rename(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    telegram_id = update.effective_user.id
    name, error = _clean_name(update.message.text)
    if error:
        await update.message.reply_text(error)
        return G_RENAME
    group = _own_group(context.user_data.pop("rename_group_id", -1), telegram_id)
    if not group:
        await update.message.reply_text("Группа не найдена — возможно, её уже удалили.")
        return ConversationHandler.END
    db.rename_group(group["id"], name)
    await update.message.reply_text(
        "✅ Переименовано.\n\nТвои группы VK:",
        reply_markup=build_groups_manage_keyboard(telegram_id),
    )
    return ConversationHandler.END


# ─── /templates conversation ──────────────────────────────────────────────────

async def cmd_templates(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    telegram_id = update.effective_user.id
    db.ensure_user(telegram_id)
    await _reply_templates(update, telegram_id)
    return ConversationHandler.END


async def templates_button(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    query = update.callback_query
    data = query.data

    if data == "noop":
        await _safe_answer(query)
        return ConversationHandler.END

    if data.startswith("t_pg_"):
        await _safe_answer(query)
        page = int(data[len("t_pg_"):])
        await query.edit_message_text(
            "Твои заготовки описаний:",
            reply_markup=build_templates_manage_keyboard(update.effective_user.id, page),
        )
        return ConversationHandler.END

    if data == "t_add":
        await _safe_answer(query)
        context.user_data.pop("edit_template_id", None)
        await query.edit_message_text("Введи название заготовки (короткое, для себя):")
        return T_TITLE

    if data.startswith("t_del_"):
        template = _own_template(int(data.rsplit("_", 1)[1]), update.effective_user.id)
        if template:
            db.delete_template(template["id"])
            await _safe_answer(query, "Удалено")
        else:
            await _safe_answer(query, "Заготовка уже удалена")
        await query.edit_message_text(
            "Твои заготовки описаний:",
            reply_markup=build_templates_manage_keyboard(update.effective_user.id),
        )
        return ConversationHandler.END

    if data.startswith("t_edit_"):
        template = _own_template(int(data.rsplit("_", 1)[1]), update.effective_user.id)
        if not template:
            await _safe_answer(query, "Заготовка не найдена", show_alert=True)
            return ConversationHandler.END
        await _safe_answer(query)
        context.user_data["edit_template_id"] = template["id"]
        await query.edit_message_text("Введи новое название заготовки:")
        return T_TITLE

    await _safe_answer(query)
    return ConversationHandler.END


async def templates_title(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    title, error = _clean_name(update.message.text)
    if error:
        await update.message.reply_text(error)
        return T_TITLE
    context.user_data["template_title"] = title
    await update.message.reply_text("Теперь введи текст описания:")
    return T_BODY


async def templates_body(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    telegram_id = update.effective_user.id
    title = context.user_data.get("template_title")
    body = update.message.text or ""
    if not title:
        await update.message.reply_text("Потерялось название заготовки — начни заново через «📝 Описания».")
        return ConversationHandler.END
    if not body.strip():
        await update.message.reply_text("Текст описания не может быть пустым. Введи текст:")
        return T_BODY

    edit_id = context.user_data.get("edit_template_id")
    if edit_id and not _own_template(edit_id, telegram_id):
        edit_id = None  # заготовку удалили, пока редактировали — сохраним как новую
    if edit_id:
        db.update_template(edit_id, title, body)
        msg = "✅ Заготовка обновлена."
    else:
        db.add_template(telegram_id, title, body)
        msg = "✅ Заготовка добавлена."

    await update.message.reply_text(
        f"{msg}\n\nТвои заготовки описаний:",
        reply_markup=build_templates_manage_keyboard(telegram_id),
    )
    return ConversationHandler.END


# ─── Админ-панель / логи ошибок (/errors, /admin) ─────────────────────────────

def _is_admin(telegram_id: int) -> bool:
    return telegram_id in ADMIN_IDS


def _fmt_ts(epoch: int) -> str:
    return datetime.fromtimestamp(epoch, tz=MOSCOW_TZ).strftime("%d.%m.%Y %H:%M")


def _err_btn_label(e) -> str:
    ts = datetime.fromtimestamp(e["created_at"], tz=MOSCOW_TZ).strftime("%d.%m %H:%M")
    code = f"VK{e['error_code']}" if e["error_code"] is not None else (e["stage"] or "ошибка")
    plat = e["platform"] or "—"
    return f"{ts} · {plat} · {code}"[:60]


def _kb(rows) -> InlineKeyboardMarkup | None:
    # Telegram отклоняет пустую инлайн-клавиатуру — отдаём None.
    return InlineKeyboardMarkup(rows) if rows else None


def _pager_rows(page: int, total: int, prefix: str) -> list[list[InlineKeyboardButton]]:
    pages = (total + ERRORS_PAGE_SIZE - 1) // ERRORS_PAGE_SIZE
    nav = []
    if page > 0:
        nav.append(InlineKeyboardButton("⬅️", callback_data=f"{prefix}_{page - 1}"))
    if page < pages - 1:
        nav.append(InlineKeyboardButton("➡️", callback_data=f"{prefix}_{page + 1}"))
    return [nav] if nav else []


def _own_list_view(telegram_id: int, page: int):
    total = db.count_errors(telegram_id)
    errors = db.get_errors(telegram_id, ERRORS_PAGE_SIZE, page * ERRORS_PAGE_SIZE)
    rows = [[InlineKeyboardButton(_err_btn_label(e), callback_data=f"err_v_{e['id']}")] for e in errors]
    rows += _pager_rows(page, total, "err_self")
    return f"📋 Твои ошибки: {total}", _kb(rows)


def _admin_users_view(page: int):
    total = db.count_users_with_errors()
    if not total:
        return "🛠 Админ-панель ошибок\n\n✅ Ошибок пока нет.", None
    users = db.get_users_with_errors(ERRORS_PAGE_SIZE, page * ERRORS_PAGE_SIZE)
    rows = [
        [InlineKeyboardButton(
            f"👤 {u['telegram_id']} · {u['cnt']} ошиб. · {_fmt_ts(u['last_at'])}",
            callback_data=f"err_u_{u['telegram_id']}_0",
        )]
        for u in users
    ]
    rows += _pager_rows(page, total, "err_au")
    return f"🛠 Админ-панель ошибок\nПользователей с ошибками: {total}", _kb(rows)


def _admin_user_errors_view(target_id: int, page: int):
    total = db.count_errors(target_id)
    errors = db.get_errors(target_id, ERRORS_PAGE_SIZE, page * ERRORS_PAGE_SIZE)
    rows = [[InlineKeyboardButton(_err_btn_label(e), callback_data=f"err_v_{e['id']}")] for e in errors]
    rows += _pager_rows(page, total, f"err_u_{target_id}")
    rows.append([InlineKeyboardButton("⬅️ К списку пользователей", callback_data="err_au_0")])
    return f"👤 Пользователь {target_id}\nОшибок: {total}", InlineKeyboardMarkup(rows)


def _detail_view(e, viewer_is_admin: bool):
    grp = (e["vk_group_name"] or "—")
    if e["vk_group_id"]:
        grp += f" (id {e['vk_group_id']})"
    code = f"VK {e['error_code']}" if e["error_code"] is not None else "—"
    text = (
        f"🆔 Ошибка #{e['id']}\n"
        f"🕒 {_fmt_ts(e['created_at'])} МСК\n"
        f"📍 Этап: {e['stage'] or '—'}\n"
        f"🎬 Платформа: {e['platform'] or '—'}\n"
        f"👥 Группа: {grp}\n"
        f"🔢 Код: {code}\n"
        f"🔗 {e['url'] or '—'}\n\n"
        f"💬 {e['message'] or '—'}"
    )
    tb = e["traceback"]
    if tb:
        budget = 3500 - len(text)
        if budget > 200:
            snippet = tb if len(tb) <= budget else "…(обрезано, полный — кнопкой ниже)…\n" + tb[-budget:]
            text += f"\n\n🧩 Traceback:\n{snippet}"
    if len(text) > 4096:  # запас под лимит Telegram даже при длинном message
        text = text[:4000] + "\n…(обрезано, полный — кнопкой ниже)"
    rows = [[InlineKeyboardButton("📄 Полный traceback файлом", callback_data=f"err_tb_{e['id']}")]]
    if viewer_is_admin:
        rows.append([InlineKeyboardButton("⬅️ Назад", callback_data=f"err_u_{e['telegram_id']}_0")])
    else:
        rows.append([InlineKeyboardButton("⬅️ Назад", callback_data="err_self_0")])
    return text, InlineKeyboardMarkup(rows)


async def cmd_errors(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    uid = update.effective_user.id
    db.ensure_user(uid)
    if _is_admin(uid):
        text, kb = _admin_users_view(0)
        await update.message.reply_text(text, reply_markup=kb)
        return
    if db.count_errors(uid) == 0:
        await update.message.reply_text("✅ У тебя нет залогированных ошибок.")
        return
    text, kb = _own_list_view(uid, 0)
    await update.message.reply_text(text, reply_markup=kb)


async def errors_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    uid = update.effective_user.id
    is_admin = _is_admin(uid)
    parts = query.data.split("_")
    kind = parts[1]

    if kind == "self":
        await _safe_answer(query)
        text, kb = _own_list_view(uid, int(parts[2]))
        await query.edit_message_text(text, reply_markup=kb)

    elif kind == "au":  # admin: список пользователей
        if not is_admin:
            await _safe_answer(query, "Недостаточно прав", show_alert=True)
            return
        await _safe_answer(query)
        text, kb = _admin_users_view(int(parts[2]))
        await query.edit_message_text(text, reply_markup=kb)

    elif kind == "u":  # admin: ошибки конкретного пользователя (err_u_<tgid>_<page>)
        if not is_admin:
            await _safe_answer(query, "Недостаточно прав", show_alert=True)
            return
        await _safe_answer(query)
        text, kb = _admin_user_errors_view(int(parts[2]), int(parts[3]))
        await query.edit_message_text(text, reply_markup=kb)

    elif kind == "v":  # детали ошибки
        eid = int(parts[2])
        e = db.get_error(eid)
        if not e:
            await _safe_answer(query, "Ошибка не найдена", show_alert=True)
            return
        if not is_admin and e["telegram_id"] != uid:
            await _safe_answer(query, "Недостаточно прав", show_alert=True)
            return
        await _safe_answer(query)
        text, kb = _detail_view(e, is_admin)
        await query.edit_message_text(text, reply_markup=kb)

    elif kind == "tb":  # полный traceback файлом
        eid = int(parts[2])
        e = db.get_error(eid)
        if not e:
            await _safe_answer(query, "Ошибка не найдена", show_alert=True)
            return
        if not is_admin and e["telegram_id"] != uid:
            await _safe_answer(query, "Недостаточно прав", show_alert=True)
            return
        await _safe_answer(query)
        content = e["traceback"] or e["message"] or "—"
        bio = BytesIO(content.encode("utf-8"))
        bio.name = f"error_{eid}.txt"
        await query.message.reply_document(document=bio, filename=f"error_{eid}.txt")


TMP_MAX_AGE_HOURS = float(os.getenv("TMP_MAX_AGE_HOURS", "6"))


async def _cleanup_tmp_job(context: ContextTypes.DEFAULT_TYPE) -> None:
    """Удаляет забытые временные файлы скачивания старше TMP_MAX_AGE_HOURS."""
    loop = asyncio.get_running_loop()
    try:
        removed = await loop.run_in_executor(
            None, cleanup_tmp_files, TMP_MAX_AGE_HOURS * 3600
        )
    except Exception:
        logger.exception("Очистка временных файлов не удалась")
        return
    if removed:
        logger.info("Очистка временных файлов: удалено %s объектов", removed)


async def _cleanup_errors_job(context: ContextTypes.DEFAULT_TYPE) -> None:
    """Периодически удаляет старые логи ошибок (см. ERROR_RETENTION_DAYS)."""
    loop = asyncio.get_running_loop()
    deleted = await loop.run_in_executor(None, db.cleanup_old_errors, ERROR_RETENTION_DAYS)
    if deleted:
        logger.info("Очистка логов ошибок: удалено %s записей", deleted)


# ─── Прокси VK: статус и переключатель (/proxy, только для админов) ───────────

def _proxy_status_text() -> str:
    if not vk_proxy.is_configured():
        return (
            "🌐 Прокси VK\n\n"
            "Не настроен — адрес не задан в .env (переменная VK_PROXY).\n"
            "Все запросы к VK идут напрямую с этого сервера."
        )
    state = "🟢 включён" if vk_proxy.is_enabled() else "🔴 выключен"
    return (
        f"🌐 Прокси VK\n\n"
        f"Настроен: да\n"
        f"Сейчас: {state}\n\n"
        "Нажми «Проверить соединение», чтобы узнать, отвечает ли VK "
        "напрямую, доступен ли сам сервер прокси и проходит ли через него VK.\n\n"
        "Прокси используется ТОЛЬКО для запросов к VK — TikTok, YouTube, "
        "Instagram и Likee всегда работают напрямую."
    )


def _proxy_keyboard() -> InlineKeyboardMarkup:
    rows = [[InlineKeyboardButton("🔄 Проверить соединение", callback_data="px_check")]]
    if vk_proxy.is_configured():
        label = "🔴 Выключить прокси" if vk_proxy.is_enabled() else "🟢 Включить прокси"
        rows.append([InlineKeyboardButton(label, callback_data="px_toggle")])
    return InlineKeyboardMarkup(rows)


def _run_proxy_checks() -> str:
    """Выполняет все три проверки синхронно (вызывается в executor'е)."""
    direct_ok, direct_info = vk_proxy.check_vk_direct()
    lines = [
        "🌐 Проверка соединения\n",
        f"{'✅' if direct_ok else '❌'} VK напрямую (без прокси): {direct_info}",
    ]
    if vk_proxy.is_configured():
        server_ok, server_info = vk_proxy.check_proxy_server()
        lines.append(f"{'✅' if server_ok else '❌'} Сервер прокси доступен: {server_info}")
        via_ok, via_info = vk_proxy.check_vk_via_proxy()
        lines.append(f"{'✅' if via_ok else '❌'} VK через прокси: {via_info}")
    else:
        lines.append("➖ Прокси не настроен (VK_PROXY не задан) — остальные проверки пропущены.")
    return "\n".join(lines)


async def cmd_proxy(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not _is_admin(update.effective_user.id):
        await update.message.reply_text("Недостаточно прав.")
        return
    await update.message.reply_text(_proxy_status_text(), reply_markup=_proxy_keyboard())


async def proxy_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    if not _is_admin(update.effective_user.id):
        await _safe_answer(query, "Недостаточно прав", show_alert=True)
        return

    if query.data == "px_toggle":
        if not vk_proxy.is_configured():
            await _safe_answer(query, "Прокси не настроен в .env", show_alert=True)
            return
        vk_proxy.set_enabled(not vk_proxy.is_enabled())
        await _safe_answer(query, "Прокси включён" if vk_proxy.is_enabled() else "Прокси выключен")
        await query.edit_message_text(_proxy_status_text(), reply_markup=_proxy_keyboard())
        return

    if query.data == "px_check":
        await _safe_answer(query, "Проверяю…")
        loop = asyncio.get_running_loop()
        try:
            report = await asyncio.wait_for(
                loop.run_in_executor(None, _run_proxy_checks), timeout=25,
            )
        except asyncio.TimeoutError:
            report = "⏱ Проверка не уложилась в отведённое время — VK или прокси не отвечают вовсе."
        try:
            await query.edit_message_text(report, reply_markup=_proxy_keyboard())
        except BadRequest:
            pass  # "not modified" — прошлая проверка дала тот же результат
        return

    await _safe_answer(query)


# ─── Глобальный обработчик ошибок ─────────────────────────────────────────────

_HARMLESS_BAD_REQUEST_MARKERS = (
    "message is not modified",      # повторное нажатие той же кнопки / страницы
    "query is too old",             # ответ на старую кнопку (после рестарта/паузы)
    "query id is invalid",
    "message to edit not found",    # сообщение уже удалено
)


def _is_harmless_telegram_error(exc: BaseException | None) -> bool:
    return isinstance(exc, BadRequest) and any(
        marker in str(exc).lower() for marker in _HARMLESS_BAD_REQUEST_MARKERS
    )


async def _safe_answer(query, *args, **kwargs) -> None:
    """query.answer(), который не роняет обработчик.

    На старой кнопке Telegram отвечает «Query is too old» — раньше обработчик
    падал на первой же строке и не выполнял само действие (удалить, выбрать…).
    """
    try:
        await query.answer(*args, **kwargs)
    except BadRequest as exc:
        logger.info("query.answer не прошёл: %s", exc)
    except TelegramNetworkError:
        logger.info("query.answer: сеть до Telegram", exc_info=True)


async def _on_error(update: object, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Непойманные исключения из обработчиков.

    Без него такие ошибки видны только в консоли сервера, а пользователь просто
    не получает ответа. Пишем в /errors и сообщаем пользователю, что случилось.
    """
    exc = context.error
    if _is_harmless_telegram_error(exc):
        # «Сообщение не изменилось», «кнопка устарела» — не ошибка для пользователя.
        logger.info("Игнорирую безобидную ошибку Telegram: %s", exc)
        return
    logger.error("Необработанная ошибка при обработке апдейта", exc_info=exc)

    # Не достучались до самого Telegram — сообщить пользователю всё равно не выйдет.
    # (BadRequest в PTB — тоже наследник NetworkError, но это ошибка запроса,
    # о ней пользователю сказать можно и нужно.)
    if isinstance(exc, TelegramNetworkError) and not isinstance(exc, BadRequest):
        return
    if not isinstance(update, Update):
        return
    chat = update.effective_chat
    if chat is None:
        return
    user = update.effective_user
    _record_error(user.id if user else chat.id, exc, stage="обработка сообщения")

    if update.callback_query is not None:
        try:
            await update.callback_query.answer()  # убираем «часики» на кнопке
        except Exception:
            pass

    if _is_network_error(exc):
        text = _unreachable_text("VK")
    else:
        text = "❌ Что-то пошло не так. Попробуй ещё раз."
    try:
        await context.bot.send_message(chat.id, text + "\n\nℹ️ Подробности — в /errors")
    except Exception:
        logger.debug("_on_error: не удалось уведомить пользователя", exc_info=True)


# ─── Общий /cancel ────────────────────────────────────────────────────────────

async def cmd_cancel(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    await update.message.reply_text("Отменено.")
    return ConversationHandler.END


# ─── Устаревшие кнопки, таймауты диалогов, нетекстовые сообщения ──────────────

# Фото, стикер, голосовое и т.п. — всё, что не текст и не команда.
_NON_TEXT = ~filters.TEXT & ~filters.COMMAND


async def _expect_text(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """В состоянии, где бот ждёт текст, пришло фото/стикер/голосовое.

    Раньше такое сообщение молча игнорировалось. Возврат None — диалог остаётся
    в текущем состоянии.
    """
    await update.message.reply_text(
        "Я жду текстовое сообщение — напиши текстом или нажми /cancel."
    )


def _timeout_handler(text: str) -> TypeHandler:
    """Обработчик ConversationHandler.TIMEOUT: сообщает, что диалог закрыт по
    неактивности (раньше бот молча переставал реагировать на ответы)."""
    async def on_timeout(update: object, context: ContextTypes.DEFAULT_TYPE) -> None:
        chat = update.effective_chat if isinstance(update, Update) else None
        if chat is not None:
            await _notify(context.bot, chat.id, text)
    return TypeHandler(Update, on_timeout)


async def handle_stale_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Нажатие кнопки, которую уже никто не ждёт (диалог истёк по таймауту или
    бот перезапускался). Раньше кнопка просто «крутилась» без ответа."""
    await _safe_answer(
        update.callback_query,
        "Эта кнопка устарела. Начни заново — например, пришли ссылку ещё раз.",
        show_alert=True,
    )


# ─── Main ────────────────────────────────────────────────────────────────────

def main() -> None:
    global _download_semaphore
    if not TELEGRAM_TOKEN:
        raise SystemExit("Не задан TELEGRAM_TOKEN в .env")

    db.init_db()
    _download_semaphore = asyncio.Semaphore(DOWNLOAD_CONCURRENCY)

    persistence = PicklePersistence(
        filepath=os.path.join(db.DATA_DIR, "bot_state.pickle")
    )
    # concurrent_updates=True — апдейты обрабатываются параллельно, поэтому медленная
    # операция в одном потоке не «замораживает» ответы остальным сообщениям.
    # Увеличенные таймауты и пул соединений — чтобы случайные обрывы/медленная
    # сеть до api.telegram.org не валили обработку с TimedOut.
    app = (
        Application.builder()
        .token(TELEGRAM_TOKEN)
        .persistence(persistence)
        # После инициализации, до старта polling, восстанавливаем отложенные
        # публикации из БД — чтобы рестарт бота не терял запланированные видео.
        .post_init(_restore_scheduled_posts)
        .concurrent_updates(True)
        .connect_timeout(30.0)
        .read_timeout(30.0)
        .write_timeout(30.0)
        .pool_timeout(30.0)
        .get_updates_connect_timeout(30.0)
        .get_updates_read_timeout(30.0)
        .build()
    )

    upload_conv = ConversationHandler(
        # entry_point принимает только распознанные URL платформ — иначе любой текст
        # в состоянии UP_CUSTOM_DESC/UP_CUSTOM_TIME перебивался бы entry_point-ом.
        entry_points=[MessageHandler(_URL_FILTER, handle_link)],
        states={
            # В «промежуточных» состояниях (только кнопки) добавляем URL-хендлер,
            # чтобы юзер мог прислать новую ссылку и сразу перезапустить диалог.
            UP_GROUP: [
                CallbackQueryHandler(handle_group_choice, pattern=r"^upgroup_"),
                MessageHandler(_URL_FILTER, handle_link),
            ],
            UP_DESC: [
                CallbackQueryHandler(handle_desc_choice, pattern=r"^updesc_"),
                MessageHandler(_URL_FILTER, handle_link),
            ],
            UP_CUSTOM_DESC: [
                # URL-ссылка проверяется первой — если юзер передумал и скинул новую
                # ссылку вместо описания, перезапускаем диалог.
                MessageHandler(_URL_FILTER, handle_link),
                MessageHandler(filters.TEXT & ~filters.COMMAND, handle_custom_desc),
                MessageHandler(_NON_TEXT, _expect_text),
            ],
            UP_TIME: [
                CallbackQueryHandler(handle_time_choice, pattern=r"^(now|custom|slot_)"),
                MessageHandler(_URL_FILTER, handle_link),
            ],
            UP_CUSTOM_TIME: [
                MessageHandler(_URL_FILTER, handle_link),
                MessageHandler(filters.TEXT & ~filters.COMMAND, handle_custom_time),
                MessageHandler(_NON_TEXT, _expect_text),
            ],
            ConversationHandler.TIMEOUT: [_timeout_handler(
                "⌛ Время на выбор вышло — публикация не запланирована.\n"
                "Пришли ссылку на видео ещё раз, чтобы начать заново."
            )],
        },
        fallbacks=[CommandHandler("cancel", cmd_cancel)],
        per_message=False,
        conversation_timeout=300,
        name="upload_conv",
        persistent=False,
        allow_reentry=False,  # entry_point теперь URL-специфичен, allow_reentry не нужен
    )

    token_conv = ConversationHandler(
        entry_points=[
            CommandHandler("settoken", cmd_settoken),
            CallbackQueryHandler(settoken_from_button, pattern=r"^settoken_change$"),
        ],
        states={
            TOKEN_WAIT: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, handle_token),
                MessageHandler(_NON_TEXT, _expect_text),
            ],
            ConversationHandler.TIMEOUT: [_timeout_handler(
                f"⌛ Время на ввод токена вышло. Нажми «{BTN_TOKEN}», чтобы задать его."
            )],
        },
        fallbacks=[CommandHandler("cancel", cmd_cancel)],
        per_message=False,
        conversation_timeout=300,
        name="token_conv",
        persistent=False,
        allow_reentry=True,
    )

    groups_conv = ConversationHandler(
        entry_points=[
            CommandHandler("groups", cmd_groups),
            CallbackQueryHandler(groups_button, pattern=r"^(g_add|g_del_|g_rename_|g_pg_|noop$)"),
            # Кнопки «Сохранить / Своё имя» — и как entry point: они должны работать
            # после таймаута диалога и для ссылок, присланных вне диалога
            # (handle_community_link). Иначе нажатие просто «висело».
            CallbackQueryHandler(groups_add_confirm, pattern=r"^g_(confirmname|manualname)$"),
        ],
        states={
            G_ADD_ID: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, groups_add_id),
                MessageHandler(_NON_TEXT, _expect_text),
            ],
            G_ADD_CONFIRM: [CallbackQueryHandler(groups_add_confirm, pattern=r"^g_(confirmname|manualname)$")],
            G_ADD_NAME: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, groups_add_name),
                MessageHandler(_NON_TEXT, _expect_text),
            ],
            G_RENAME: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, groups_rename),
                MessageHandler(_NON_TEXT, _expect_text),
            ],
            ConversationHandler.TIMEOUT: [_timeout_handler(
                "⌛ Время ожидания вышло. Если добавлял группу — просто пришли ссылку "
                "на сообщество ещё раз, я её подхвачу."
            )],
        },
        fallbacks=[CommandHandler("cancel", cmd_cancel)],
        per_message=False,
        conversation_timeout=300,
        name="groups_conv",
        persistent=False,
        allow_reentry=True,
    )

    templates_conv = ConversationHandler(
        entry_points=[
            CommandHandler("templates", cmd_templates),
            CallbackQueryHandler(templates_button, pattern=r"^(t_add|t_del_|t_edit_|t_pg_|noop$)"),
        ],
        states={
            T_TITLE: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, templates_title),
                MessageHandler(_NON_TEXT, _expect_text),
            ],
            T_BODY: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, templates_body),
                MessageHandler(_NON_TEXT, _expect_text),
            ],
            ConversationHandler.TIMEOUT: [_timeout_handler(
                "⌛ Время на ввод заготовки вышло — она не сохранена. "
                "Открой «📝 Описания», чтобы начать заново."
            )],
        },
        fallbacks=[CommandHandler("cancel", cmd_cancel)],
        per_message=False,
        conversation_timeout=300,
        name="templates_conv",
        persistent=False,
        allow_reentry=True,
    )

    app.add_handler(CommandHandler("start", cmd_start))
    # Админ-панель / логи ошибок (вне диалогов, как /start).
    app.add_handler(CommandHandler("errors", cmd_errors))
    app.add_handler(CommandHandler("admin", cmd_errors))
    app.add_handler(CallbackQueryHandler(errors_callback, pattern=r"^err_"))
    # Статус и переключатель прокси VK (только для админов).
    app.add_handler(CommandHandler("proxy", cmd_proxy))
    app.add_handler(CallbackQueryHandler(proxy_callback, pattern=r"^px_"))
    # Кнопки меню — до диалогов, чтобы перехватывать нажатия даже внутри разговора
    app.add_handler(MessageHandler(filters.Text(MENU_BUTTON_TEXTS), main_menu_button))
    app.add_handler(CallbackQueryHandler(handle_token_delete, pattern=r"^settoken_delete$"))
    app.add_handler(token_conv)
    app.add_handler(groups_conv)
    app.add_handler(templates_conv)
    app.add_handler(upload_conv)
    app.add_handler(CallbackQueryHandler(handle_cancel_upload, pattern="^cancel_upload$"))
    # Ссылка на сообщество VK вне диалогов — последним, чтобы не перехватывать
    # текст в активных диалогах (описание / заготовка могут содержать ссылку).
    app.add_handler(MessageHandler(_VK_COMMUNITY_FILTER & ~filters.COMMAND, handle_community_link))
    # Кнопка, которую никто не обработал (диалог истёк / бот перезапускался), —
    # самым последним: отвечаем «кнопка устарела» вместо вечных «часиков».
    app.add_handler(CallbackQueryHandler(handle_stale_callback))
    # Любая непойманная ошибка — пользователю сообщение, а не тишина.
    app.add_error_handler(_on_error)

    # Периодическая очистка временных файлов скачивания: остатки от ошибок,
    # отмен и таймаутов (поток скачивания нельзя прервать — он дописывает файл
    # уже после того, как задача сдалась).
    app.job_queue.run_repeating(
        _cleanup_tmp_job,
        interval=timedelta(hours=1),
        first=timedelta(minutes=5),
        name="cleanup_tmp",
    )

    # Периодическая очистка старых логов ошибок.
    app.job_queue.run_repeating(
        _cleanup_errors_job,
        interval=timedelta(days=ERROR_CLEANUP_INTERVAL_DAYS),
        first=timedelta(minutes=1),
        name="cleanup_errors",
    )

    logger.info("Бот запущен")
    app.run_polling()


if __name__ == "__main__":
    main()
