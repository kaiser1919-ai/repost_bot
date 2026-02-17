import asyncio
import json
import logging
import pandas as pd
import random
import time
from datetime import datetime
from telethon import TelegramClient
from telethon.tl.types import MessageEntityTextUrl, MessageMediaPhoto, MessageMediaDocument, MessageMediaWebPage
from telethon.errors import FloodWaitError, ChannelPrivateError, ChatAdminRequiredError
from config import (
    TARGET_CHANNEL_ID,
    CSV_FILE_PATH,
    LOG_FILE,
    LAST_SEEN_FILE,
    INTERVAL_MINUTES,
    TEST_MODE,
    api_id,
    api_hash
)

# === Логирование ===
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(LOG_FILE, encoding='utf-8'),
        logging.StreamHandler()
    ]
)

# === Telethon клиент ===
client = TelegramClient('session_user', api_id, api_hash)

# === Загрузка каналов ===
def load_channels():
    df = pd.read_csv(CSV_FILE_PATH, sep=';', encoding='utf-8')
    return df.iloc[:, 2].tolist()

# === Управление last_seen ===
def load_last_seen():
    try:
        with open(LAST_SEEN_FILE, 'r', encoding='utf-8') as f:
            return json.load(f)
    except FileNotFoundError:
        return {}

def save_last_seen(data):
    with open(LAST_SEEN_FILE, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=2, ensure_ascii=False)

# === Получение ID канала ===
def get_entity_id(url):
    username = url.split('/')[-1]
    return f'@{username}'

# === Копирование поста с источником (единый пост!) ===
async def copy_message_with_source(from_chat, msg_id, target_channel):
    message = await client.get_messages(from_chat, ids=msg_id)
    if not message:
        return

    post_link = f"https://t.me/{from_chat.username}/{msg_id}"
    source_text = "Источник"

    if TEST_MODE:
        logging.info(f"[TEST] Found post: {post_link}")
        return

    try:
        await asyncio.sleep(random.uniform(0.8, 2.2))

        # --- Извлекаем чистый текст и entities ---
        raw_text = message.message or ""
        entities = list(message.entities) if message.entities else []

        # --- Формируем полный текст: оригинал + "\n\nИсточник" ---
        full_text = raw_text + "\n\n" + source_text

        # --- Рассчитываем offset для "Источник" ---
        # Важно: offset — в символах Unicode, а не байтах
        source_offset = len(raw_text) + 2  # +2 для "\n\n"
        source_entity = MessageEntityTextUrl(
            offset=source_offset,
            length=len(source_text),
            url=post_link
        )
        entities_with_source = entities + [source_entity]

        # --- Случай 1: Нет медиа — просто текст ---
        if not message.media:
            await client.send_message(
                target_channel,
                full_text,
                formatting_entities=entities_with_source,
                link_preview=False
            )
            logging.info("✅ Sent text-only post with correct source link")
            return

        # --- Случай 2: Альбом (grouped_id) ---
        if message.grouped_id:
            # Получаем все сообщения из альбома
            album_msgs = await client.get_messages(
                from_chat,
                min_id=msg_id - 10,
                max_id=msg_id + 10,
                limit=20
            )
            album = [m for m in album_msgs if m.grouped_id == message.grouped_id]
            album.sort(key=lambda x: x.id)

            # Берём текст и entities из первого сообщения в альбоме
            first_msg = album[0]
            album_raw_text = first_msg.message or ""
            album_entities = list(first_msg.entities) if first_msg.entities else []
            album_full_text = album_raw_text + "\n\n" + source_text
            album_source_offset = len(album_raw_text) + 2
            album_source_entity = MessageEntityTextUrl(
                offset=album_source_offset,
                length=len(source_text),
                url=post_link
            )
            album_entities_with_source = album_entities + [album_source_entity]

            # Собираем медиа
            media_list = []
            for m in album:
                if m.photo:
                    media_list.append(m.photo)
                elif m.document:
                    media_list.append(m.document)
                elif hasattr(m.media, 'document') and m.media.document.mime_type.startswith('video/'):
                    media_list.append(m.media.document)

            if media_list:
                await client.send_file(
                    target_channel,
                    media_list,
                    caption=album_full_text,
                    formatting_entities=album_entities_with_source,
                    link_preview=False
                )
                logging.info(f"✅ Sent album ({len(media_list)} items) with source link")
            return

        # --- Случай 3: Одиночное медиа — отправляем в одном сообщении ---
        media = message.media
        # Определяем тип медиа
        if hasattr(media, 'photo'):
            await client.send_file(
                target_channel,
                media,
                caption=full_text,
                formatting_entities=entities_with_source,
                link_preview=False
            )
            logging.info("✅ Sent photo + text with source link")
        elif hasattr(media, 'document'):
            await client.send_file(
                target_channel,
                media,
                caption=full_text,
                formatting_entities=entities_with_source,
                link_preview=False
            )
            logging.info("✅ Sent document + text with source link")
        else:
            # Неизвестный тип — форвардим как есть
            await client.forward_messages(target_channel, message)
            logging.info("⚠️ Forwarded unknown media")

    except FloodWaitError as e:
        logging.warning(f"FLOOD WAIT: waiting {e.seconds}s")
        await asyncio.sleep(e.seconds)
    except Exception as e:
        logging.error(f"ERROR sending post {post_link}: {type(e).__name__}: {e}")

# === Проверка каналов ===
async def check_channels():
    channels = load_channels()
    last_seen = load_last_seen()

    for channel_url in channels:
        entity = get_entity_id(channel_url)
        try:
            chat = await client.get_entity(entity)
            logging.info(f"🔍 Checking channel: {chat.username or chat.title}")

            await asyncio.sleep(random.uniform(1.0, 3.0))

            async for message in client.iter_messages(chat, limit=5):
                if message.id > last_seen.get(entity, 0):
                    last_seen[entity] = message.id
                    await copy_message_with_source(chat, message.id, TARGET_CHANNEL_ID)
                    await asyncio.sleep(random.uniform(3.0, 8.0))

        except Exception as e:
            logging.error(f"❌ Error with {entity}: {e}")

    save_last_seen(last_seen)

# === Главный цикл ===
async def main_loop():
    await client.start()
    while True:
        logging.info("🔄 Starting channel check...")
        await check_channels()
        sleep_time = random.randint(2, 10) * 60
        logging.info(f"💤 Next check in {sleep_time // 60} minutes ({sleep_time} sec).")
        await asyncio.sleep(sleep_time)

if __name__ == '__main__':
    asyncio.run(main_loop())