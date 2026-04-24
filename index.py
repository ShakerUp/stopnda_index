import asyncio
import websockets
import json
import os
import time
from dotenv import load_dotenv
import logging
from logging.handlers import RotatingFileHandler

from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.ext import (
    Application,
    CommandHandler,
    CallbackQueryHandler,
    ContextTypes
)

load_dotenv()

BOT_TOKEN = os.getenv("BOT_TOKEN")

WS_URL = "wss://fstream.binance.com/market/ws/!markPrice@arr@1s"

COOLDOWN = 900
TRACK_INTERVAL = 15
STEP = 0.6

last_alert = {}
tracking_users = {}  # {chat_id: {symbol: message_id}}
prices_cache = {}

CONFIG_FILE = "config.json"

MAX_TRACKS = 3
TRACK_TIMEOUT = 7200  # 2 hours

last_ws_message = time.time()
alert_queue = asyncio.Queue()

ADMIN_ID = 684460638

# ---------------- LOGGING ----------------

logger = logging.getLogger("binance_ws_bot")
logger.setLevel(logging.INFO)

formatter = logging.Formatter(
    "%(asctime)s | %(levelname)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)

file_handler = RotatingFileHandler(
    "binance_ws.log",
    maxBytes=5_000_000,
    backupCount=5,
    encoding="utf-8"
)
file_handler.setFormatter(formatter)

console_handler = logging.StreamHandler()
console_handler.setFormatter(formatter)

logger.addHandler(file_handler)
logger.addHandler(console_handler)

# ---------------- CONFIG ----------------

def load_config():
    if not os.path.exists(CONFIG_FILE):
        return {"threshold": 0.5, "subscribers": []}

    with open(CONFIG_FILE, "r") as f:
        return json.load(f)


def save_config(config):
    with open(CONFIG_FILE, "w") as f:
        json.dump(config, f, indent=4)


config = load_config()
subscribers = set(config["subscribers"])
blacklist = set(config.get("blacklist", []))

def get_signal_icon(dev):

    dev = abs(dev)

    if dev >= 5:
        return "🚨🚨🚨"

    elif dev >= 4:
        return "🔥"

    elif dev >= 3:
        return "🚨"

    else:
        return "⚠️"

# ---------------- WEBSOCKET ----------------

async def websocket_listener(app):

    global prices_cache, last_ws_message

    while True:

        try:

            logger.info("Connecting to Binance WebSocket...")

            async with websockets.connect(
                WS_URL,
                ping_interval=20,
                ping_timeout=20,
                close_timeout=5
            ) as ws:

                logger.info("✅ Connected to Binance WebSocket")

                async for data in ws:

                    last_ws_message = time.time()

                    try:
                        prices = json.loads(data)
                    except Exception as e:
                        logger.exception(f"JSON parse error: {e}")
                        continue

                    logger.info(f"📥 WS packet received | symbols={len(prices)}")

                    max_negative = None

                    for p in prices:

                        try:
                            symbol = p["s"]

                            if symbol in blacklist:
                                continue

                            mark = float(p["p"])
                            index = float(p["i"])

                            if index == 0:
                                logger.warning(f"{symbol}: index price is zero")
                                continue

                            deviation = (mark - index) / index * 100

                            prices_cache[symbol] = {
                                "mark": mark,
                                "index": index,
                                "dev": deviation,
                                "updated_at": time.time()
                            }

                            if max_negative is None or deviation < max_negative[1]:
                                max_negative = (symbol, deviation, mark, index)

                            if deviation <= -config["threshold"]:

                                now = time.time()
                                send_alert = False

                                if symbol not in last_alert:
                                    send_alert = True
                                else:
                                    last = last_alert[symbol]

                                    if now - last["time"] >= COOLDOWN:
                                        send_alert = True
                                    elif deviation <= last["dev"] - STEP:
                                        send_alert = True

                                if not send_alert:
                                    continue

                                last_alert[symbol] = {
                                    "time": now,
                                    "dev": deviation
                                }

                                logger.info(
                                    f"🚨 ALERT queued | {symbol} | dev={deviation:.3f}% | "
                                    f"mark={mark} | index={index}"
                                )

                                keyboard = InlineKeyboardMarkup([[
                                    InlineKeyboardButton(
                                        "📊 Track",
                                        callback_data=f"track_{symbol}"
                                    )
                                ]])

                                alert_time = time.strftime("%H:%M:%S")
                                icon = get_signal_icon(deviation)

                                text = (
                                    f"{icon} *Отрицательное отклонение!* {icon}\n\n"
                                    f"🔸 *Тикер:* `{symbol}`\n\n"
                                    f"🔹 *Mark Price:* `{mark:.4f}`\n"
                                    f"🔹 *Index Price:* `{index:.4f}`\n"
                                    f"⚠️ *Отклонение:* `{deviation:.3f}%`\n"
                                    f"⏰ *Время алерта*: `{alert_time}`"
                                )

                                await alert_queue.put((text, keyboard))

                        except Exception as e:
                            logger.exception(f"Error processing symbol data: {p} | {e}")

                    if max_negative:
                        s, dev, mark, index = max_negative
                        logger.info(
                            f"📊 Lowest dev now: {s} | {dev:.3f}% | "
                            f"mark={mark:.6f} | index={index:.6f}"
                        )

        except Exception as e:

            logger.exception(f"❌ WebSocket error: {e}")

        logger.info("🔄 Reconnecting to Binance in 5 seconds...")
        await asyncio.sleep(5)

async def websocket_watchdog():

    global last_ws_message

    while True:

        age = time.time() - last_ws_message

        if age > 60:
            logger.warning(f"⚠️ WS stalled — no data for {age:.1f}s")

        else:
            logger.info(f"✅ WS alive | last message {age:.1f}s ago | cached={len(prices_cache)} symbols")

        await asyncio.sleep(20)


# ---------------- SUBSCRIBE ----------------

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id

    if chat_id not in subscribers:
        subscribers.add(chat_id)
        config["subscribers"] = list(subscribers)
        save_config(config)

    welcome_text = (
        "Трекер разрыва индекса на Binance.\n\n"
        "📖 **Доступные команды:**\n"
        "/start — Подписаться на автоматические алерты\n"
        "/stop — Отключить уведомления\n"
        "/track `SYMBOL` — Запустить живой трекинг конкретной монеты (напр. `/track BTCUSDT`)\n\n"
        "📢 *Алерты придут автоматически, когда отклонение станет ниже порога -2%.*"
    )

    await update.message.reply_text(welcome_text, parse_mode="Markdown")


async def stop(update: Update, context: ContextTypes.DEFAULT_TYPE):

    chat_id = update.effective_chat.id

    if chat_id in subscribers:

        subscribers.remove(chat_id)

        config["subscribers"] = list(subscribers)
        save_config(config)

    await update.message.reply_text(
        "❌ Alerts disabled"
    )


async def alert_sender(app):

    while True:

        text, keyboard = await alert_queue.get()

        for user in subscribers:

            try:

                await app.bot.send_message(
                    chat_id=user,
                    text=text,
                    reply_markup=keyboard,
                    parse_mode="Markdown"
                )

            except:
                pass

# ---------------- TRACK COMMAND ----------------

async def track_command(update: Update, context: ContextTypes.DEFAULT_TYPE):

    if not context.args:
        await update.message.reply_text("Usage: /track BTCUSDT")
        return

    symbol = context.args[0].upper()

    await start_tracking(update.effective_chat.id, symbol, context)


# ---------------- BUTTON HANDLER ----------------

async def button_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):

    query = update.callback_query
    await query.answer()

    data = query.data

    if data.startswith("track_"):

        symbol = data.split("_")[1]

        await start_tracking(query.message.chat.id, symbol, context)

    elif data.startswith("stop_"):

      symbol = data.split("_")[1]
      user = query.message.chat.id

      if user in tracking_users and symbol in tracking_users[user]:
          del tracking_users[user][symbol]

      # удаляем сообщение трекинга
      try:
          await query.message.delete()
      except:
          pass

      # отправляем временное сообщение
      msg = await context.bot.send_message(
          chat_id=user,
          text=f"❌ Tracking {symbol} stopped"
      )

      # удаляем через 5 секунд
      await asyncio.sleep(5)

      try:
          await msg.delete()
      except:
          pass


# ---------------- TRACKING ----------------

async def start_tracking(chat_id, symbol, context):

    if chat_id not in tracking_users:
        tracking_users[chat_id] = {}

    # LIMIT
    if len(tracking_users[chat_id]) >= MAX_TRACKS:

        await context.bot.send_message(
            chat_id=chat_id,
            text=f"⚠️ Maximum {MAX_TRACKS} active trackings allowed."
        )
        return

    if symbol in tracking_users[chat_id]:

        await context.bot.send_message(
            chat_id=chat_id,
            text=f"Already tracking {symbol}"
        )
        return

    keyboard = InlineKeyboardMarkup([[

        InlineKeyboardButton(
            "❌ Stop tracking",
            callback_data=f"stop_{symbol}"
        )
    ]])

    msg = await context.bot.send_message(
        chat_id=chat_id,
        text=f"🚀 Started tracking {symbol}",
        reply_markup=keyboard
    )

    tracking_users[chat_id][symbol] = {
        "message_id": msg.message_id,
        "start_time": time.time()
    }

    asyncio.create_task(
        track_loop(chat_id, symbol, msg.message_id, context)
    )

async def track_loop(chat_id, symbol, message_id, context):

    while True:

        if chat_id not in tracking_users:
            return

        if symbol not in tracking_users[chat_id]:
            return

        data = tracking_users[chat_id][symbol]

        # AUTO STOP
        if time.time() - data["start_time"] > TRACK_TIMEOUT:

            del tracking_users[chat_id][symbol]

            try:
                await context.bot.edit_message_text(
                    chat_id=chat_id,
                    message_id=message_id,
                    text=f"⏰ Tracking {symbol} stopped (2h timeout)"
                )
            except:
                pass

            return

        if symbol not in prices_cache:
            await asyncio.sleep(TRACK_INTERVAL)
            continue

        p = prices_cache[symbol]

        update_time = time.strftime("%H:%M:%S")

        text = (
            f"📊 *{symbol} Live Tracking*\n\n"
            f"💰 *Mark Price*: `{p['mark']:.4f}`\n"
            f"📊 *Index Price*: `{p['index']:.4f}`\n"
            f"⚡ *Отклонение*: `{p['dev']:.3f}%`\n\n"
            f"⏱ *Последнее обновление*: `{update_time}`"
        )

        keyboard = InlineKeyboardMarkup([[

            InlineKeyboardButton(
                "❌ Stop tracking",
                callback_data=f"stop_{symbol}"
            )
        ]])

        try:

            await context.bot.edit_message_text(
                chat_id=chat_id,
                message_id=message_id,
                text=text,
                reply_markup=keyboard,
                parse_mode="Markdown"
            )

        except:
            pass

        await asyncio.sleep(TRACK_INTERVAL)
        
        
async def blacklist_command(update: Update, context: ContextTypes.DEFAULT_TYPE):

    if update.effective_user.id != ADMIN_ID:
        return

    if not context.args:
        await update.message.reply_text(
            "Usage:\n"
            "/blacklist add BTCUSDT\n"
            "/blacklist remove BTCUSDT\n"
            "/blacklist list"
        )
        return

    action = context.args[0]

    if action == "list":

        if not blacklist:
            await update.message.reply_text("Blacklist empty")
            return

        text = "Blacklisted symbols:\n" + "\n".join(sorted(blacklist))

        await update.message.reply_text(text)
        return

    if len(context.args) < 2:
        await update.message.reply_text("Specify symbol")
        return

    symbol = context.args[1].upper()

    if action == "add":

        blacklist.add(symbol)

        config["blacklist"] = list(blacklist)
        save_config(config)

        await update.message.reply_text(f"{symbol} added to blacklist")

    elif action == "remove":

        blacklist.discard(symbol)

        config["blacklist"] = list(blacklist)
        save_config(config)

        await update.message.reply_text(f"{symbol} removed from blacklist")


# ---------------- MAIN ----------------

def main():

    app = Application.builder().token(BOT_TOKEN).build()

    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("stop", stop))
    app.add_handler(CommandHandler("track", track_command))
    app.add_handler(CommandHandler("blacklist", blacklist_command))

    app.add_handler(CallbackQueryHandler(button_handler))

    loop = asyncio.get_event_loop()
    loop.create_task(websocket_listener(app))
    loop.create_task(websocket_watchdog())
    loop.create_task(alert_sender(app))

    print("Bot started")

    app.run_polling()


if __name__ == "__main__":
    main()