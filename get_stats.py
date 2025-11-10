""" Эта функция для апдейта стенда на гугл диске, требует перед запуском иметь ссылки на каналы на листе - Каналы
"""
import os
import time
import re
import json
import hashlib
import tempfile
from datetime import datetime, timedelta
from urllib.parse import urlparse
import asyncio


import gspread
from oauth2client.service_account import ServiceAccountCredentials
import requests
from dotenv import load_dotenv
import openai
from openai import OpenAI, RateLimitError, AuthenticationError, PermissionDeniedError
from twelvelabs import TwelveLabs, TooManyRequestsError
from twelvelabs.tasks import TasksRetrieveResponse
from twelvelabs.core.api_error import ApiError

import sys, logging
from logging.handlers import RotatingFileHandler

# ================== CONSTANTS ==================
URL_1 = "https://api.tgstat.ru/channels/get"
URL_2 = "https://api.tgstat.ru/channels/posts"
URL_3 = "https://api.tgstat.ru/posts/stat"

with open('prompts/openai_sys_role.txt', 'r', encoding='utf-8') as f2:
    OPENAI_SYS_ROLE = f2.read().strip()
with open('prompts/pegasus_sys_role.txt', 'r', encoding='utf-8') as f3:
    PEGASUS_SYS_ROLE = f3.read().strip()
with open('prompts/headers.json', 'r', encoding='utf-8') as f4:
    final_headers = json.load(f4)

ADMIN_SPREADSHEET_NAME: str = "Sellebra TGstat (admin)"
CHANNELS: str = 'Каналы'
SUGGESTIONS: str ='Рекомендации'
PROFILE: str = 'Профиль'
MAIN: str = 'Main'
LOG: str = 'Log'

# ================== LOGGER ==================
LOGGER_NAME = "analyse_admin"
LOG_FILE = f"/var/log/{LOGGER_NAME}"
os.makedirs(os.path.dirname(LOG_FILE), exist_ok=True)

# Инициализация отдельного логгера
logger = logging.getLogger(LOGGER_NAME)
logger.setLevel(logging.INFO)
if not logger.handlers:
    try:
        os.makedirs(os.path.dirname(LOG_FILE), exist_ok=True)
        fh = RotatingFileHandler(LOG_FILE, maxBytes=10*1024*1024, backupCount=5, encoding="utf-8")
    except Exception as e:
        fallback = f"/tmp/{LOGGER_NAME}.log"
        print(f"[WARN] Не удалось открыть {LOG_FILE} для записи ({e}). Пишу в {fallback}")
        fh = RotatingFileHandler(fallback, maxBytes=10*1024*1024, backupCount=5, encoding="utf-8")
    fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
    fh.setFormatter(fmt)
    logger.addHandler(fh)
    sh = logging.StreamHandler(sys.stdout)
    sh.setFormatter(fmt)
    logger.addHandler(sh)
logger.propagate = False

# ================== AUTH ==================
load_dotenv()
TGSTAT_API_KEY = os.getenv("TGSTAT_API_KEY")
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
openai.api_key = OPENAI_API_KEY
PEGASUS_API_KEY = os.getenv("PEGASUS_API_KEY")

client1 = OpenAI(api_key=OPENAI_API_KEY)
client2 = TwelveLabs(api_key=PEGASUS_API_KEY)

scope = ['https://spreadsheets.google.com/feeds',
         'https://www.googleapis.com/auth/drive']
creds = ServiceAccountCredentials.from_json_keyfile_name('creds.json', scope)
gs_client = gspread.authorize(creds)

def get_or_create_worksheet(spreadsheet_name, title, rows=100, cols=20):
    try:
        return spreadsheet_name.worksheet(title)
    except gspread.exceptions.WorksheetNotFound:
        logger.warning(f"⚠️ Лист '{title}' не найден, создаю новый...")
        return spreadsheet_name.add_worksheet(title=title, rows=rows, cols=cols)

# ================== GLOBAL VARIABLES ==================   
admin_spreadsheet = gs_client.open(ADMIN_SPREADSHEET_NAME)
admin_main = get_or_create_worksheet(admin_spreadsheet, MAIN)
admin_log = get_or_create_worksheet(admin_spreadsheet, LOG)

# ================== FUNCTIONS ==================
def get_channel_info(channel_id):
    url = URL_1
    params = {
        'token': TGSTAT_API_KEY,
        'channelId': channel_id
    }

    response = requests.get(url, params=params, timeout=15).json()
    if response.get("status", "") == "error":
        raise Exception(response['error'])
    ch = response.get("response", {})
    return {
        'Название канала': ch.get("title", ""),
        'link': f"https://t.me/{ch.get('username', '')}" if ch.get("username") else channel_id,
        'ID': ch.get("id", ""),
        'Количество подписчиков': ch.get("participants_count", 0)
    }


def extract_channels_from_sheet(channels_worksheet):
    all_data = channels_worksheet.get_all_values()
    headers = all_data[0]
    data = all_data[1:]
    link_col_index = headers.index("link") # TODO: make it a variable
    channels_list = []
    for row in data:
        if link_col_index < len(row):
            link = row[link_col_index].strip()
            if link:
                channels_list.append(link)
    return channels_list


def save_to_sheet_channels(data, worksheet):
    header = ["Название канала", "link", "ID", "Количество подписчиков"]
    rows = [[ch['Название канала'], ch['link'], ch['ID'], ch["Количество подписчиков"]] for ch in data]
    worksheet.clear()
    worksheet.append_row(header, value_input_option='RAW')
    worksheet.append_rows(rows, value_input_option='RAW')


def get_top_posts(channel_id, days_back, limit=50):
    url = URL_2
    date_to = datetime.today()
    date_from = date_to - timedelta(days=days_back)
    params = {
        'token': TGSTAT_API_KEY,
        'channelId': channel_id,
        'limit': limit,
        'startDate': date_from.strftime('%Y-%m-%d'),
        'endDate': date_to.strftime('%Y-%m-%d'),
        'extended': 1
    }
    response = requests.get(url, params=params, timeout=15)
    try:
        return response.json().get("response", {}).get("items", [])
    except Exception as e:
        raise Exception(f"Ошибка парсинга JSON для channel {channel_id}: {e}")


def transform_to_normal_date(timestamp):
    try:
        dt = datetime.fromtimestamp(int(timestamp))
        return dt.strftime("%d.%m.%Y"), dt.strftime("%H:%M")
    except:
        return "", ""


def fetch_post_stats(post_link):
    url = URL_3
    parsed_url = urlparse(post_link)
    path_parts = parsed_url.path.split('/')
    if len(path_parts) < 3:
        return None
    params = {"token": TGSTAT_API_KEY, "postId": post_link}
    try:
        response = requests.get(url, params=params, timeout=15)
        data = response.json()
        if data.get("status") == "ok":
            return data["response"]
    except Exception as e:
        raise Exception(f"Exception for post {post_link}: {str(e)}")


def calculate_engagement(views, reactions, comments, forwards):
    return round((reactions + forwards + comments) / views * 100, 2) if views > 0 else 0


def extract_top_posts(company_id: int, company_name: str, channels_data, days_back, top_n):
    final_rows = []
    os.makedirs("extracted_data", exist_ok=True)
    
    all_posts = []
    all_stats = []
    
    for ch in channels_data:
        logger.info(f"🔍 Анализируем канал: {ch['Название канала']}")
        channel_id = ch['ID']
        try:
            posts = get_top_posts(channel_id, days_back)
            if not posts:
                # Warning
                raise Exception(f"Нет постов в канале")
                
            # Collect posts for JSON
            all_posts.extend([{
                "channel_id": channel_id,
                "channel_name": ch['Название канала'],
                "post": post,
                "timestamp": datetime.now().isoformat()
            } for post in posts])
            
            channel_posts = []
            for post in posts:
                text = post.get("text", "")
                if len(text.strip()) == 0:
                    continue
                post_link = post.get("link", "")
                if not post_link:
                    continue
                stats = fetch_post_stats(post_link)
                if not stats:
                    continue
                    
                # Collect stats for JSON
                all_stats.append({
                    "channel_id": channel_id,
                    "channel_name": ch['Название канала'],
                    "post_link": post_link,
                    "stats": stats,
                    "timestamp": datetime.now().isoformat()
                })
                
                views = stats.get("viewsCount", 0)
                reactions = stats.get("reactionsCount", 0)
                comments = stats.get("commentsCount", 0)
                forwards = stats.get("forwardsCount", 0)
                engagement = calculate_engagement(
                    views, reactions, comments, forwards)
                post["engagement"] = engagement
                post["views"] = views
                post["reactions"] = reactions
                post["comments"] = comments
                post["forwards"] = forwards
                post["channel_info"] = ch
                post["processed_stats"] = stats
                channel_posts.append(post)
                
            top_channel_posts = sorted(
                channel_posts, key=lambda x: x["engagement"], reverse=True
            )[:top_n]
            
            for post in top_channel_posts:
                text = post.get("text", "")
                views = post.get("views", 0)
                reactions = post.get("reactions", 0)
                comments = post.get("comments", 0)
                forwards = post.get("forwards", 0)
                engagement = post.get("engagement", 0)
                post_link = post.get("link", "")
                media = post.get("media", {})
                file_url = media.get("file_url", "")
                video_link = file_url if file_url and file_url.endswith(
                    ".mp4") else ""
                date_only, time_only = transform_to_normal_date(
                    post.get("date", ""))
                post_length = len(text) if text else 0
                row = [
                    ch['Название канала'],
                    ch["Количество подписчиков"],
                    text,
                    post_link,
                    video_link,
                    date_only,
                    time_only,
                    post_length,
                    views,
                    reactions,
                    comments,
                    forwards,
                    engagement
                ]
                final_rows.append(row)
                
        except Exception as e:
            # Warning
            admin_log.insert_row([company_id, company_name, f"Ошибка при обработке {channel_id}: {e}", datetime.today().isoformat()], 2)
    
    # Save posts to channels_stats.json
    with open("extracted_data/channels_stats.json", "w", encoding="utf-8") as f:
        json.dump(all_posts, f, ensure_ascii=False, indent=2)
    
    # Save stats to posts_stats.json
    with open("extracted_data/posts_stats.json", "w", encoding="utf-8") as f:
        json.dump(all_stats, f, ensure_ascii=False, indent=2)
    
    return final_rows


def translate_into_russian(text):
    prompt = f"""
    Переведи текст на русский язык и пришли ТОЛЬКО перведенный текст.
    \"{text}\"
    """
    response = client1.chat.completions.create(
        model="gpt-4o-mini",
        messages=[
            {"role": "user", "content": prompt}
        ],
        temperature=0.8
    )
    return response.choices[0].message.content


def extract_json_from_response(content):
    """Extract JSON from markdown-wrapped content"""
    match = re.search(r"```json\s*(\{.*?\})\s*```", content, re.DOTALL)
    try:
        if match:
            json_str = match.group(1)
            return json.loads(json_str)
        else:
            return json.loads(content)
    except json.JSONDecodeError as e:
        raise Exception(f"Ошибка при парсинге JSON: {e}")


def generate_index_name(url: str) -> str:
    """Generate unique index name based on video URL"""
    parsed = urlparse(url)
    basename = os.path.basename(parsed.path)
    name_hash = hashlib.md5(url.encode()).hexdigest()[:6]
    return f"video-index-{basename}-{name_hash}"


def get_or_create_index(name: str):
    """Create the index (only if not exists)"""
    existing = client2.indexes.list()
    for idx in existing:
        if idx.index_name == name:
            logger.info(f"✅ Используем существующий индекс: {idx.index_name}")
            return idx

    models = [{"model_name": "pegasus1.2", "model_options": ["visual", "audio"]}]
    index = client2.indexes.create(index_name=name, models=models)
    logger.info(f"✅ Индекс создан: id={index.id}")
    return index

# TODO: remove function
def download_video(url: str) -> str:
    """Download video from URL to temp file"""
    logger.info("📥 Загружаем видео...")
    with tempfile.NamedTemporaryFile(delete=False, suffix=".mp4") as tmp_file:
        video_path = tmp_file.name
        response = requests.get(url, stream=True, timeout=60)
        response.raise_for_status()
        for chunk in response.iter_content(chunk_size=8192):
            if chunk:
                tmp_file.write(chunk)
    logger.info(f"📁 Видео сохранено в папку: {video_path}")
    return video_path


def transcribe_video(url: str) -> str:
    """Transcribe and summarize video"""
    if not url or not url.strip():
        return ""

    try:
        # video_path = download_video(url)
        index_name = generate_index_name(url)
        index = get_or_create_index(index_name)

        # with open(video_path, "rb") as video_file:
        task = client2.tasks.create(index_id=index.id, video_url=url)
        logger.info(f"🚀 Task started: id={task.id}, video_id={task.video_id}")

        def on_task_update(task: TasksRetrieveResponse):
            logger.info(f"⏳ Status = {task.status}")

        task = client2.tasks.wait_for_done(task_id=task.id, callback=on_task_update)

        if task.status != "ready":
            raise RuntimeError(f"Indexing failed with status: {task.status}")


        res = client2.summarize(video_id=task.video_id,
                               type="summary", prompt=PEGASUS_SYS_ROLE)

        # if os.path.exists(video_path):
        #     os.remove(video_path)

        return res.summary
    except ApiError as e:
        error_body = getattr(e, 'body', {})
        if isinstance(error_body, dict):
            raise Exception(f"Ошибка TwelveLabs API: {error_body}")
        raise Exception(f"Ошибка TwelveLabs API: {e}")
    except TooManyRequestsError as e:
        raise Exception("Ошибка TwelveLabs API: превышен лимит запросов")
    except Exception as e:
        # if 'video_path' in locals() and os.path.exists(video_path):
        #     os.remove(video_path)
        raise


def rewrite_post_into_blocks(post_text):
    """Analyze post and return structured data"""
    prompt = f"""
    Проанализируй следующий Telegram-пост и ответь строго в JSON формате по полям:
    - tema: тема поста (коротко)
    - format: формат (текст / видео / карусель / опрос и т.п.)
    - length: длина поста в символах
    - style: серьёзный / юморной / экспертный / сторителлинг и т.п.
    - cta: какой призыв к действию есть, или "нет", если есть, то явно указать
    - zagolovok_5_slov: сгенерируй новый заголовок до 5 слов
    - zagolovok_len: длина сгенерированного заголовка
    - fact: есть ли научный факт или ссылка на исследование: да/нет
    - benefit: есть ли конкретная польза или инструкция: да/нет
    - comment_call: есть ли призыв прокомментировать: да/нет
    - insight: краткий вывод, в чём сила поста
    - filter: определи, является ли пост Личным или Профессиональным. 
      `Личное` — посты о личных мероприятиях, личных вещах, событиях, не связанных с сельским хозяйством.
      `Профессиональное` — посты, связанные с сельским хозяйством, кормами, животноводством, советами для фермеров.
    Текст поста:
    \"\"\"{post_text}\"\"\"
    """
    response = client1.chat.completions.create(
        model="gpt-4o",
        messages=[{"role": "system", "content": OPENAI_SYS_ROLE},
                    {"role": "user", "content": prompt}],
        temperature=0.4
    )
    response_text = response.choices[0].message.content

    return extract_json_from_response(response_text)


def rewrite_post_with_context(post_text, context):
    """Rewrite post with company context"""
    prompt = f"""
    Контекст: {context}
    Ниже популярный пост из Telegram:
    \"{post_text}\"
    На основе этого поста и контекста создай уникальный Telegram-пост для ПрофКорм.
    Сохрани идею и пользу, но полностью перепиши текст под стиль ПрофКорм.
    Не упоминай чужие бренды. Пиши ясно, экспертно и по делу. Объём — до 2049 символов с пробелами.
    """
    response = client1.chat.completions.create(
        model="gpt-4o",
        messages=[{"role": "system", "content": OPENAI_SYS_ROLE},
                    {"role": "user", "content": prompt}],
        temperature=0.8
    )

    return response.choices[0].message.content


def create_video_suggestion(transcription, company_context):
    """Create video suggestion based on transcription and context"""
    if not transcription or transcription.startswith("Error:"):
        return ""

    prompt = f"""
    Контекст компании: {company_context}
    
    Ниже описание и скрипт видео конкурента:
    \"{transcription}\"
    
    На основе этого описания создай подробное предложение для съемки похожего видео для нашей компании.
    Включи:
    1. Адаптацию сценария под наш бренд и продукты
    2. Конкретные технические требования к съемке
    3. Рекомендации по локации и реквизиту
    4. Предложения по тексту/речи
    5. Идеи для визуальных эффектов или графики
    
    Сохрани структуру и эмоциональное воздействие оригинала, но адаптируй под наш стиль и аудиторию.
    """

    response = client1.chat.completions.create(
        model="gpt-4o",
        messages=[
            {"role": "system", "content": "Ты креативный директор, который адаптирует видео-контент под бренд компании."},
            {"role": "user", "content": prompt}
        ],
        temperature=0.7
    )
    return response.choices[0].message.content


def complete_ai_analysis_for_sheet(company_id: int, company_name: str, company_context: str, post_num: int, worksheet):
    """Complete AI analysis for all posts in the sheet including video processing"""
    headers = worksheet.row_values(1)
    new_data = worksheet.get_values(f"2:{post_num + 1}")
    
    post_text_col = headers.index("Пост - Текст поста")
    video_url_col = headers.index(
        "Ссылка на видео") if "Ссылка на видео" in headers else -1

    ai_columns = [
        "Предложение по посту", "Предложение по видео", "Тема поста", "Формат",
        "Стиль", "CTA", "Заголовок", "Длина заголовка",
        "✅ Научный факт/исследование", "✅ Конкретная польза (как сделать)",
        "✅ Призыв комментировать", "Инсайт/заметка", "Фильтр"
    ]

    for col in ai_columns:
        if col not in headers:
            headers.append(col)

    if len(headers) > worksheet.col_count:
        worksheet.add_cols(len(headers) - worksheet.col_count)

    worksheet.update(range_name="1:1", values=[headers])

    admin_log.insert_row([company_id, company_name, f"🔄 Обрабатываем {post_num} строк с полным AI анализом...", datetime.today().isoformat()], 2)

    enhanced_rows = []
    for i, row in enumerate(new_data):
        logger.info(f"Обрабатываем строку {i+1}...")

        while len(row) < len(headers):
            row.append("")

        post_text = row[post_text_col] if post_text_col < len(row) else ""
        video_url = row[video_url_col] if video_url_col >= 0 and video_url_col < len(
            row) else ""

        if not post_text.strip():
            enhanced_rows.append(row)
            continue
        
        start_time = time.time()
        analysis = rewrite_post_into_blocks(post_text)
        end_time = time.time()

        logger.info(f"Пост проанализирован за {end_time - start_time:.2f} секунд")
        
        start_time = time.time()
        rewritten_post = rewrite_post_with_context(post_text, company_context)
        end_time = time.time()

        logger.info(f"Пост переписан за {end_time - start_time:.2f} секунд")

        # Video processing
        video_suggestion = ""
        if video_url.strip():
            logger.info(f"🎥 Обрабатываем видео: {video_url}")
            try:
                start_time = time.time()
                transcription = transcribe_video(video_url.strip())
                end_time = time.time()

                logger.info(f"Видео транскрибированно за {end_time - start_time:.2f} секунд")

                if transcription:
                    start_time = time.time()
                    translated_transcription = translate_into_russian(
                        transcription)
                    end_time = time.time()

                    logger.info(f"Транскрипт переведен за {end_time - start_time:.2f} секунд")

                    start_time = time.time()
                    video_suggestion = create_video_suggestion(
                        translated_transcription, company_context)
                    end_time = time.time()

                    logger.info(f"Сгенерирован новый сюжет видео за {end_time - start_time:.2f} секунд")
            except Exception as e:
                admin_log.insert_row([company_id, company_name, f"Ошибка при обработке видео в посте {i+1}: {e}", datetime.today().isoformat()], 2)
                logger.warning(f"Ошибка при обработке видео в посте {i+1}: {e}")

        # Update row with all AI data
        col_mapping = {
            "Предложение по посту": rewritten_post,
            "Предложение по видео": video_suggestion,
            "Тема поста": analysis.get("tema", ""),
            "Формат": analysis.get("format", ""),
            "Стиль": analysis.get("style", ""),
            "CTA": analysis.get("cta", ""),
            "Заголовок": analysis.get("zagolovok_5_slov", ""),
            "Длина заголовка": analysis.get("zagolovok_len", 0),
            "✅ Научный факт/исследование": analysis.get("fact", ""),
            "✅ Конкретная польза (как сделать)": analysis.get("benefit", ""),
            "✅ Призыв комментировать": analysis.get("comment_call", ""),
            "Инсайт/заметка": analysis.get("insight", ""),
            "Фильтр": analysis.get("filter", "")
        }

        for col_name, value in col_mapping.items():
            if col_name in headers:
                col_idx = headers.index(col_name)
                row[col_idx] = value

        enhanced_rows.append(row)
        logger.info(f"  ✅ Строка {i+1} обработана")

    worksheet.update(range_name=f"2:{post_num+1}", values=enhanced_rows)

    logger.info(f"✅ Полный AI анализ завершен для компании {company_name} c id {company_id}")
    logger.info(f"📊 Обработано строк: {len(enhanced_rows)}")

async def extract_context(spreadsheet):
    try:
        worksheet = spreadsheet.worksheet(PROFILE)
    except gspread.exceptions.WorksheetNotFound:
        raise Exception(f"Нет листа {PROFILE}")

    top_left_cell = worksheet.cell(1, 1).value
    if not top_left_cell.strip():
        raise Exception(f"Профиль компании должен быть указан на листе {PROFILE} в ячейке A1")
    
    return top_left_cell

# ------------------ RUN ------------------
async def process_table(company_id: int, company_name: str, company_url: str, days_back=60):
    logger.info(f"🔄 Обрабатываем таблицу клиента {company_name} c id {company_id}...")
    try: 
        spreadsheet = gs_client.open_by_url(company_url)
    except:
        raise Exception("Неверный URL")
    company_context = await extract_context(spreadsheet)
    # TODO: what if table not exist
    channels_sheet = get_or_create_worksheet(spreadsheet, CHANNELS)
    # TODO: what if table not exist
    suggestions_sheet = get_or_create_worksheet(spreadsheet, SUGGESTIONS)
    # TODO: should always update?
    suggestions_headers = [
        "Название канала",
        "Количество подписчиков",
        "Пост - Текст поста",
        "Ссылка на пост",
        "Ссылка на видео",
        "Дата публикации",
        "Время публикации",
        "Длинна поста",
        "Просмотры",
        "Реакции",
        "Комментарии",
        "Репосты",
        "Вовлеченность"
    ]
    suggestions_sheet.update(range_name='1:1', values=[suggestions_headers])

    # --- Сбор информации о каналах ---
    raw_channels = extract_channels_from_sheet(channels_sheet)
    channel_infos = []
    for ch in raw_channels:
        try:
            channel_id = ch.strip()
            info = get_channel_info(channel_id)
            if info:
                channel_infos.append(info)
        except Exception as e:
            # Warning
            admin_log.insert_row([company_id, company_name, f"Ошибка при обработке {channel_id}: {e}", datetime.today().isoformat()], 2)

    if not channel_infos:
        raise Exception("Каналы не найдены")

    # Сохраняем в Google Sheets
    save_to_sheet_channels(channel_infos, channels_sheet)

    # --- Логируем ---
    admin_log.insert_row([company_id, company_name, f"Обработано {len(channel_infos)} каналов", datetime.today().isoformat()], 2)

    # --- Сбор постов ---
    channels_data = channels_sheet.get_all_records()
    data = [ch for ch in channels_data if ch.get('ID') and ch.get('Название канала')]
    rows = extract_top_posts(company_id, company_name, data, days_back, top_n=10)

    logger.info(f"Всего выбрано постов: {len(rows)}")

    if not rows:
        raise Exception("Нет постов")

    # --- Сохраняем в Google Sheets ---
    suggestions_sheet.insert_rows(rows, value_input_option='RAW', row=2)

    # --- Логируем ---
    admin_log.insert_row([company_id, company_name, f"Собрано {len(rows)} рекомендаций", datetime.today().isoformat()], 2)

    complete_ai_analysis_for_sheet(company_id, company_name, company_context, len(rows), suggestions_sheet)
    
    admin_log.insert_row([company_id, company_name, "AI анализ завершен", datetime.today().isoformat()], 2)
 
    

def get_col_idx(col_name, headers):
    try:
        col_idx = headers.index(col_name.lower())
    except ValueError:
        raise Exception("Колонка '{col_name}' не найдена")
    return col_idx

# async def create_client(i: int, company_id: int, company_name: str, company_url: str, headers):
#     created_col = get_col_idx('Created', headers)
#     updated_col = get_col_idx('Updated', headers)
#     status_col = get_col_idx('Status', headers)

#     admin_main.update_cell(i+2, created_col+1, datetime.today().strftime('%Y-%m-%d'))
    
#     await process_table(company_id, company_name, company_url)

#     admin_main.update_cell(i+2, status_col+1, 'In progress')
    # admin_main.update_cell(i+2, updated_col+1, datetime.today().strftime('%Y-%m-%d'))

def main():
    all_data = admin_main.get_all_values()
    if not all_data:
        logger.error(f"Лист '{MAIN}' пустой")
        return

    headers = [x.lower() for x in all_data[0]]
    rows = all_data[1:]
    
    try:
        id_col = get_col_idx('id', headers)
        name_col = get_col_idx('Name', headers)
        url_col = get_col_idx('URL', headers)
        status_col = get_col_idx('Scheduler Status', headers)
        processing_col = get_col_idx('Processing', headers)

        clients_to_process = []

        for i, row in enumerate(rows):
            client_id = row[id_col].strip() if id_col < len(row) else ''
            client_name = row[name_col].strip() if name_col < len(row) else ''
            client_url = row[url_col].strip() if url_col < len(row) else ''
            client_status = row[status_col].strip() if status_col < len(row) else ''
            

            if client_status == 'Start' or client_status == 'In progress':
                if not client_id.isdigit():
                    admin_log.insert_row([client_id, client_name, f"Неправильный id '{client_id}' для клиента в строке {i}", datetime.today().isoformat()], 2)
                    logger.error(f"Неправильный id '{client_id}' для клиента в строке {i}")
                    admin_main.update_cell(i+2, processing_col+1, 'Ошибка')
                client_id = int(client_id)
                if client_name and client_url:
                    clients_to_process.append((i, client_id, client_name, client_url, client_status))
                    admin_main.update_cell(i+2, processing_col+1, 'В ожидании...')
                else:
                    admin_log.insert_row([client_id, client_name, f"Не указано название или ссылка на таблицу для клиента в строке {i}", datetime.today().isoformat()], 2)
                    logger.error(f"Не указано название или ссылка на таблицу для клиента в строке {i}")
                    admin_main.update_cell(i+2, processing_col+1, 'Ошибка')
        
        for client in clients_to_process:
            i, client_id, client_name, client_url, client_status = client 
            admin_main.update_cell(i+2, processing_col+1, 'В исполнении')
            try:
                if client_status == 'Start':
                    asyncio.run(process_table(client_id, client_name, client_url))
                    admin_main.update_cell(i+2, status_col+1, 'In progress')
                else:
                    asyncio.run(process_table(client_id, client_name, client_url, 7))
                admin_main.update_cell(i+2, processing_col+1, 'Готово')
            except PermissionDeniedError:
                admin_log.insert_row([client_id, client_name, "Ошибка OpenAI API: включите VPN", datetime.today().isoformat()], 2)
                logger.error(f"Ошибка OpenAI API: включите VPN")
                admin_main.update_cell(i+2, processing_col+1, 'Ошибка')
            except RateLimitError:
                admin_log.insert_row([client_id, client_name, "Ошибка OpenAI API: исчерпан лимит запросов", datetime.today().isoformat()], 2)
                logger.error(f"Ошибка OpenAI API: исчерпан лимит запросов")
                admin_main.update_cell(i+2, processing_col+1, 'Ошибка')
            except AuthenticationError:
                admin_log.insert_row([client_id, client_name, "Ошибка OpenAI API: ошибка аутентификации", datetime.today().isoformat()], 2)
                logger.error(f"Ошибка OpenAI API: ошибка аутентификации")
                admin_main.update_cell(i+2, processing_col+1, 'Ошибка')
            except Exception as e:
                admin_log.insert_row([client_id, client_name, str(e), datetime.today().isoformat()], 2)
                logger.error(str(e))
                admin_main.update_cell(i+2, processing_col+1, 'Ошибка')
        
        for client in clients_to_process:
            i, client_id, client_name, client_url, client_status = client
            if admin_main.cell(i+2, processing_col+1).value == 'Готово':
                admin_main.update_cell(i+2, processing_col+1, '')
    
    except Exception as e:
        logger.error(str(e))
        

if __name__ == "__main__":
    main()
