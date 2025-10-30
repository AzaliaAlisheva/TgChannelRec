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
from openai import OpenAI
from twelvelabs import TwelveLabs
from twelvelabs.tasks import TasksRetrieveResponse

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
MAIN: str = 'Main'
LOG: str = 'Log'
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

# ================== FUNCTIONS ==================
def get_or_create_worksheet(spreadsheet_name, title, rows=100, cols=20):
    try:
        return spreadsheet_name.worksheet(title)
    except gspread.exceptions.WorksheetNotFound:
        print(f"⚠️ Лист '{title}' не найден, создаю новый...")
        return spreadsheet_name.add_worksheet(title=title, rows=rows, cols=cols)

def get_channel_info(channel_id):
    url = URL_1
    params = {
        'token': TGSTAT_API_KEY,
        'channelId': channel_id
    }
    try:
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
    except Exception as e:
        print(f"❌ Ошибка при обработке {channel_id}: {e}")
        return None

def extract_channels_from_sheet(channels_worksheet):
    all_data = channels_worksheet.get_all_values()
    headers = all_data[0]
    data = all_data[1:]
    link_col_index = headers.index("link")
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
        print(f"Ошибка парсинга JSON для channel {channel_id}: {e}")
        return []

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
        print(f"Exception for post {post_link}: {str(e)}")
    return None

def calculate_engagement(views, reactions, comments, forwards):
    return round((reactions + forwards + comments) / views * 100, 2) if views > 0 else 0

def extract_top_posts(channels_data, days_back, top_n):
    final_rows = []
    os.makedirs("extracted_data", exist_ok=True)
    
    all_posts = []
    all_stats = []
    
    for ch in channels_data:
        print(f"\n🔍 Анализируем канал: {ch['Название канала']}")
        channel_id = ch['ID']
        try:
            posts = get_top_posts(channel_id, days_back)
            if not posts:
                print("Нет постов.")
                continue
                
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
                    views,
                    post_link,
                    video_link,
                    "",
                    "",
                    date_only,
                    time_only,
                    "",
                    "",
                    post_length,
                    "",
                    "",
                    views,
                    reactions,
                    comments,
                    forwards,
                    "",
                    "",
                    "",
                    "",
                    "",
                    engagement,
                    "",
                    ""
                ]
                final_rows.append(row)
                
        except Exception as e:
            print(f"Ошибка при обработке {ch['Название канала']}: {e}")
    
    # Save posts to channels_stats.json
    with open("extracted_data/channels_stats.json", "w", encoding="utf-8") as f:
        json.dump(all_posts, f, ensure_ascii=False, indent=2)
    
    # Save stats to posts_stats.json
    with open("extracted_data/posts_stats.json", "w", encoding="utf-8") as f:
        json.dump(all_stats, f, ensure_ascii=False, indent=2)
    
    print(f"Всего выбрано постов: {len(final_rows)}")
    return final_rows

def save_to_sheet_suggestions(rows, worksheet):
    worksheet.clear()
    worksheet.append_row(final_headers, value_input_option='RAW')
    worksheet.append_rows(rows, value_input_option='RAW')
    return worksheet

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

def insert_ai_suggestions(rows, worksheet):
    header = [
        "Название канала",
        "Количество подписчиков",
        "Пост - Текст поста",
        "Дата публикации",
        "Количество просмотров",
        "Ссылка на пост",
        "Ссылка на видео",
        "Предложение по посту",
        "Предложение по видео"
    ]
    worksheet.clear()
    worksheet.append_row(header, value_input_option='RAW')
    worksheet.append_rows(rows, value_input_option='RAW')
    
# def generate_suggestions(channels_sheet, suggestions_sheet):

#     records = channels_sheet.get_all_records()
#     updated_rows = []

#     for _, row in enumerate(records):
#         print(f"🔍 Анализ поста: {row['Название канала']}")

#         text = row.get("Пост - Текст поста", "")
#         video_url = row.get("Ссылка на видео", "")

#         # Обработка текста поста
#         try:
#             text_suggestion = rewrite_post_with_context(text)
#         except Exception as e:
#             print(f"❌ Ошибка в rewrite_post_with_context: {e}")
#             text_suggestion = ""
#             continue

#         # Обработка видео
#         try:
#             if video_url:
#                 video_suggestion = translate_into_russian(
#                     transcribe_video(video_url))
#             else:
#                 video_suggestion = ""
#         except Exception as e:
#             print(f"❌ Ошибка в transcribe_video: {e}")
#             video_suggestion = ""
#             continue

#         updated_row = [
#             row["Название канала"],
#             row["Количество подписчиков"],
#             text,
#             row["Дата публикации"],
#             row["Количество просмотров"],
#             row["Ссылка на пост"],
#             video_url,
#             text_suggestion,
#             video_suggestion
#         ]
#         updated_rows.append(updated_row)
#     insert_ai_suggestions(updated_rows, suggestions_sheet)

def extract_json_from_response(content):
    """Extract JSON from markdown-wrapped content"""
    match = re.search(r"```json\s*(\{.*?\})\s*```", content, re.DOTALL)
    if match:
        json_str = match.group(1)
        try:
            return json.loads(json_str)
        except json.JSONDecodeError as e:
            print(f"Ошибка при парсинге JSON: {e}")
            return None
    else:
        try:
            return json.loads(content)
        except json.JSONDecodeError as e:
            print(f"Ошибка при попытке распарсить JSON без markdown: {e}")
            return None

def generate_index_name(url: str) -> str:
    """Generate unique index name based on video URL"""
    parsed = urlparse(url)
    basename = os.path.basename(parsed.path)
    name_hash = hashlib.md5(url.encode()).hexdigest()[:6]
    return f"video-index-{basename}-{name_hash}"

def get_or_create_index(name: str):
    """Create the index (only if not exists)"""
    existing = client2.index.list()
    for idx in existing:
        if idx.name == name:
            print(f"✅ Using existing index: {idx.name}")
            return idx

    models = [{"name": "pegasus1.2", "options": ["visual", "audio"]}]
    index = client2.index.create(name=name, models=models)
    print(f"✅ Index created: id={index.id}, name={index.name}")
    return index

def download_video(url: str) -> str:
    """Download video from URL to temp file"""
    print("📥 Downloading video...")
    try:
        with tempfile.NamedTemporaryFile(delete=False, suffix=".mp4") as tmp_file:
            video_path = tmp_file.name
            response = requests.get(url, stream=True, timeout=60)
            response.raise_for_status()
            for chunk in response.iter_content(chunk_size=8192):
                if chunk:
                    tmp_file.write(chunk)
        print(f"📁 Video saved to: {video_path}")
        return video_path
    except Exception as e:
        print(f"❌ Error downloading video: {e}")
        raise


def transcribe_video(url: str) -> str:
    """Transcribe and summarize video"""
    if not url or not url.strip():
        return ""

    try:
        video_path = download_video(url)
        index_name = generate_index_name(url)
        index = get_or_create_index(index_name)

        task = client2.tasks.create(index_id=index.id, video_url=video_path)
        print(f"🚀 Task started: id={task.id}, video_id={task.video_id}")

        def on_task_update(task: TasksRetrieveResponse):
            print(f"⏳ Status = {task.status}")

        task = client2.tasks.wait_for_done(task_id=task.id, callback=on_task_update)

        if task.status != "ready":
            raise RuntimeError(f"Indexing failed with status: {task.status}")


        res = client2.summarize(video_id=task.video_id,
                               type="summary", prompt=PEGASUS_SYS_ROLE)

        if os.path.exists(video_path):
            os.remove(video_path)

        return res.summary

    except Exception as e:
        print(f"❌ Error transcribing video {url}: {e}")
        if 'video_path' in locals() and os.path.exists(video_path):
            os.remove(video_path)
        return f"Error: {str(e)}"


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
    try:
        response = client1.chat.completions.create(
            model="gpt-4o",
            messages=[{"role": "system", "content": OPENAI_SYS_ROLE},
                      {"role": "user", "content": prompt}],
            temperature=0.4
        )
        response_text = response.choices[0].message.content

        return extract_json_from_response(response_text) or {}
    except Exception as e:
        print(f"Error analyzing post: {e}")
        return {}


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
    try:
        response = client1.chat.completions.create(
            model="gpt-4o",
            messages=[{"role": "system", "content": OPENAI_SYS_ROLE},
                      {"role": "user", "content": prompt}],
            temperature=0.8
        )

        return response.choices[0].message.content
    except Exception as e:
        print(f"Error rewriting post: {e}")
        return ""


def create_video_suggestion(transcription):
    """Create video suggestion based on transcription and context"""
    if not transcription or transcription.startswith("Error:"):
        return ""

    prompt = f"""
    Контекст компании: {CONTEXT}
    
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

    try:
        response = client1.chat.completions.create(
            model="gpt-4o",
            messages=[
                {"role": "system", "content": "Ты креативный директор, который адаптирует видео-контент под бренд компании."},
                {"role": "user", "content": prompt}
            ],
            temperature=0.7
        )
        return response.choices[0].message.content
    except Exception as e:
        print(f"❌ Error creating video suggestion: {e}")
        return f"Базовая транскрипция: {transcription}"


def complete_ai_analysis_for_sheet(worksheet, company_context):
    """Complete AI analysis for all posts in the sheet including video processing"""
    try:
        all_data = worksheet.get_all_values()
        if not all_data:
            print("❌ Лист пустой")
            return

        headers = all_data[0]
        rows = all_data[1:]

        try:
            post_text_col = headers.index("Пост - Текст поста")
            video_url_col = headers.index(
                "Ссылка на видео") if "Ссылка на видео" in headers else -1
        except ValueError:
            print("❌ Колонка 'Пост - Текст поста' не найдена")
            return

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

        worksheet.update("1:1", [headers])

        print(f"🔄 Обрабатываем {len(rows)} строк с полным AI анализом...")

        enhanced_rows = []
        for i, row in enumerate(rows, start=2):
            print(f"Обрабатываем строку {i}...")

            while len(row) < len(headers):
                row.append("")

            post_text = row[post_text_col] if post_text_col < len(row) else ""
            video_url = row[video_url_col] if video_url_col >= 0 and video_url_col < len(
                row) else ""

            if not post_text.strip():
                enhanced_rows.append(row)
                continue

            # AI Analysis for text
            print("📝 Анализируем текст поста...")
            analysis = rewrite_post_into_blocks(post_text)
            rewritten_post = rewrite_post_with_context(post_text, company_context)

            # Video processing
            video_suggestion = ""
            if video_url.strip():
                print(f"🎥 Обрабатываем видео: {video_url}")
                try:
                    transcription = transcribe_video(video_url.strip())
                    if transcription and not transcription.startswith("Error:"):
                        translated_transcription = translate_into_russian(
                            transcription)
                        video_suggestion = create_video_suggestion(
                            translated_transcription)

                    else:
                        video_suggestion = "Ошибка обработки видео"
                except Exception as e:
                    print(f"  ❌ Ошибка обработки видео: {e}")
                    video_suggestion = f"Ошибка: {str(e)}"

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
            print(f"  ✅ Строка {i} обработана")

        worksheet.update(f"2:{len(enhanced_rows)+1}", enhanced_rows)

        print("✅ Полный AI анализ завершен для листа")
        print(f"📊 Обработано строк: {len(enhanced_rows)}")

    except Exception as e:
        print(f"❌ Ошибка при полном AI анализе листа : {e}")

# ------------------ RUN ------------------
async def process_table(file_url: str, company_context: str, days_back=60):
    start_time = time.time()
    print("🚀 Запуск анализа...")

    spreadsheet = gs_client.open_by_url(file_url)
    channels_sheet = get_or_create_worksheet(spreadsheet, CHANNELS)
    suggestions_sheet = get_or_create_worksheet(spreadsheet, SUGGESTIONS)

    # --- Сбор информации о каналах ---
    raw_channels = extract_channels_from_sheet(channels_sheet)
    channel_infos = []
    for ch in raw_channels:
        info = get_channel_info(ch.strip())
        if info:
            channel_infos.append(info)

    # Сохраняем в Google Sheets
    save_to_sheet_channels(channel_infos, channels_sheet)

    # --- Сбор постов ---
    channels_data = channels_sheet.get_all_records()
    data = [ch for ch in channels_data if ch.get('ID') and ch.get('Название канала')]
    rows = extract_top_posts(data, days_back, top_n=10)

    # --- Сохраняем в Google Sheets ---
    save_to_sheet_suggestions(rows, suggestions_sheet)
    complete_ai_analysis_for_sheet(suggestions_sheet, company_context)

    # --- Засечка времени ---
    end_time = time.time()
    elapsed = end_time - start_time
    minutes, seconds = divmod(int(elapsed), 60)
    print(f"\n🎉 Весь процесс AI анализа завершен!")
    print(f"⏱️ Время выполнения: {minutes} мин {seconds} сек")


async def create_client(i: int, client_name: str, client_url: str, admin_main, headers):
    id_col = headers.index('id')
    created_col = headers.index('created')
    updated_col = headers.index('updated')
    status_col = headers.index('status')
    context_col = headers.index('company context')

    # admin_main.update_cell(i+2, created_col+1, datetime.today().strftime('%Y-%m-%d'))
    
    company_id = admin_main.cell(i+2, id_col + 1).value
    company_context = admin_main.cell(i+2, context_col+1).value
    await process_table(client_url, company_context)

    # admin_main.update_cell(i+2, updated_col+1, datetime.today().strftime('%Y-%m-%d'))
    admin_main.update_cell(i+2, status_col+1, 'Start')

def main():
    admin_spreadsheet = gs_client.open(ADMIN_SPREADSHEET_NAME)
    admin_main = get_or_create_worksheet(admin_spreadsheet, MAIN)
    admin_log = get_or_create_worksheet(admin_spreadsheet, LOG)

    
    all_data = admin_main.get_all_values()
    if not all_data:
        print(f"❌ Лист '{MAIN}' пустой")
        return

    headers = [x.lower() for x in all_data[0]]
    rows = all_data[1:]
    
    try:
        name_col = headers.index("name")
    except ValueError:
        print("❌ Колонка 'Name' не найдена")
        return
    
    try:
        url_col = headers.index("url")
    except ValueError:
        print("❌ Колонка 'URL' не найдена")
        return
    
    # status_columns = ['id', 'Status', 'Created', 'Updated', 'Comment']

    # for col in status_columns:
    #     if col.lower() not in headers:
    #         headers.append(col)

    #     if len(headers) > admin_main.col_count:
    #         admin_main.add_cols(len(headers) - admin_main.col_count)

    #     admin_main.update("1:1", [headers])

    for i, row in enumerate(rows):
        if name_col < len(row) and url_col < len(row):
            client_name = row[name_col].strip()
            client_url = row[url_col].strip()
            if client_name and client_url:
                asyncio.run(create_client(i, client_name, client_url, admin_main, headers))
                
        

if __name__ == "__main__":
    main()
