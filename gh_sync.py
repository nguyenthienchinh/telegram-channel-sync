"""
Đồng bộ channel nguồn -> channel đích, chạy theo lượt (dùng cho GitHub Actions).
Mỗi lượt: poll bài mới trong MAX_RUNTIME giây, chép sang đích, lưu last_id vào state.json.
Dùng Telethon (tài khoản người dùng), KHÔNG dùng bot.

Biến môi trường:
  TG_API_ID, TG_API_HASH, TG_STRING_SESSION, TG_SOURCE, TG_DEST
  MAX_RUNTIME   (giây, mặc định 270)  - thời gian poll mỗi lượt
  POLL_SECONDS  (giây, mặc định 20)   - khoảng cách giữa 2 lần hỏi bài mới
  TG_FROM_START (=1 để chép từ bài đầu tiên; mặc định chỉ chép bài MỚI từ lúc bắt đầu)
  TG_STATE_FILE (mặc định state.json)
"""
import asyncio
import json
import os
import sys
import tempfile
import time
from pathlib import Path

from telethon import TelegramClient
from telethon.errors import FloodWaitError
from telethon.sessions import StringSession

API_ID = int(os.environ["TG_API_ID"])
API_HASH = os.environ["TG_API_HASH"]
STRING_SESSION = os.environ["TG_STRING_SESSION"]
SOURCE = os.environ["TG_SOURCE"]
DEST = os.environ["TG_DEST"]
MAX_RUNTIME = int(os.getenv("MAX_RUNTIME", "270"))
POLL = int(os.getenv("POLL_SECONDS", "20"))
FROM_START = os.getenv("TG_FROM_START", "0") == "1"
STATE_FILE = Path(os.getenv("TG_STATE_FILE", "state.json"))


def norm(x):
    s = str(x).strip()
    return int(s) if s.lstrip("-").isdigit() else s


def load_last():
    if STATE_FILE.exists():
        return json.loads(STATE_FILE.read_text(encoding="utf-8")).get("last_id")
    return None


def save_last(n):
    STATE_FILE.write_text(json.dumps({"last_id": n}), encoding="utf-8")


async def safe(factory):
    while True:
        try:
            return await factory()
        except FloodWaitError as e:
            print(f"FloodWait {e.seconds}s, chờ...")
            await asyncio.sleep(e.seconds + 1)


async def copy_msgs(client, dst, msgs):
    caption = next((m.message for m in msgs if m.message), "")
    entities = next((m.entities for m in msgs if m.message), None)
    medias = [m.media for m in msgs if m.media]

    if not medias:
        if caption:
            await safe(lambda: client.send_message(
                dst, caption, formatting_entities=entities, link_preview=False))
        return

    try:
        await safe(lambda: client.send_file(
            dst, medias if len(medias) > 1 else medias[0],
            caption=caption, formatting_entities=entities))
        return
    except Exception as e:
        print(f"Gửi trực tiếp lỗi ({type(e).__name__}), tải về rồi up lại")

    with tempfile.TemporaryDirectory() as tmp:
        paths = []
        for m in msgs:
            if m.media:
                p = await safe(lambda m=m: client.download_media(m, file=tmp))
                if p:
                    paths.append(p)
        if paths:
            await safe(lambda: client.send_file(
                dst, paths if len(paths) > 1 else paths[0],
                caption=caption, formatting_entities=entities))
        elif caption:
            await safe(lambda: client.send_message(dst, caption, formatting_entities=entities))


def group_messages(msgs):
    groups, cur, gid = [], [], None
    for m in msgs:
        if getattr(m, "action", None):
            continue
        if m.grouped_id and m.grouped_id == gid:
            cur.append(m)
            continue
        if cur:
            groups.append(cur)
        cur, gid = [m], m.grouped_id
    if cur:
        groups.append(cur)
    return groups


async def main():
    client = TelegramClient(StringSession(STRING_SESSION), API_ID, API_HASH)
    await client.connect()
    if not await client.is_user_authorized():
        print("Session không hợp lệ hoặc đã hết hạn. Tạo lại TG_STRING_SESSION.")
        sys.exit(1)

    src = await client.get_entity(norm(SOURCE))
    dst = await client.get_entity(norm(DEST))
    print(f"Nguồn: {getattr(src, 'title', src)} -> Đích: {getattr(dst, 'title', dst)}")

    last = load_last()
    if last is None:
        if FROM_START:
            last = 0
        else:
            latest = await client.get_messages(src, limit=1)
            last = latest[0].id if latest else 0
        save_last(last)
        print(f"Khởi tạo last_id = {last}")

    start = time.time()
    while True:
        new = [m async for m in client.iter_messages(src, reverse=True, min_id=last)]
        for g in group_messages(new):
            await copy_msgs(client, dst, g)
            last = max(m.id for m in g)
            save_last(last)
            print(f"Đã chép id {last}")
            await asyncio.sleep(1)
        if time.time() - start + POLL >= MAX_RUNTIME:
            break
        await asyncio.sleep(POLL)

    await client.disconnect()
    print("Xong lượt.")


if __name__ == "__main__":
    asyncio.run(main())