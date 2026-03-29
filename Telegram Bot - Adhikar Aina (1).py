# Databricks notebook source
# DBTITLE 1,Install Dependencies
%pip install python-telegram-bot fpdf databricks-sdk

# COMMAND ----------

# DBTITLE 1,Imports and Configuration
import asyncio
import json
import os

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.serving import ChatMessage, ChatMessageRole
from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.ext import (
    ApplicationBuilder,
    CallbackQueryHandler,
    CommandHandler,
    ContextTypes,
    MessageHandler,
    filters,
)

from telegram_integration import (
    create_certificate_pdf_bytes,
    get_certificate_for_citizen,
    get_citizen_id_for_chat,
    save_chat_mapping,
    send_telegram_message,
)

TELEGRAM_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
DATABRICKS_TOKEN = os.getenv("DATABRICKS_TOKEN", "")
DATABRICKS_HOST = os.getenv("DATABRICKS_HOST", "https://dbc-8d79a655-2501.cloud.databricks.com")
VS_INDEX = os.getenv("DATABRICKS_VS_INDEX", "workspace.bronze.welfare_schemes_index")
LLM_ENDPOINT = os.getenv("DATABRICKS_LLM_ENDPOINT", "databricks-meta-llama-3-3-70b-instruct")

if not TELEGRAM_TOKEN:
    raise ValueError("TELEGRAM_BOT_TOKEN is required")
if not DATABRICKS_TOKEN:
    raise ValueError("DATABRICKS_TOKEN is required")

w = WorkspaceClient(host=DATABRICKS_HOST, token=DATABRICKS_TOKEN)

print("✅ Telegram bot configuration loaded")

# COMMAND ----------

# DBTITLE 1,Command Handlers
async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "Welcome to Adhikar-Aina.\n"
        "Link your citizen profile first: /link <citizen_id>\n"
        "Example: /link TEST-123ABC"
    )


async def link_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args:
        await update.message.reply_text("Usage: /link <citizen_id>")
        return

    citizen_id = context.args[0].strip()
    chat = update.effective_chat
    user = update.effective_user

    try:
        save_chat_mapping(
            citizen_id=citizen_id,
            chat_id=chat.id,
            username=(user.username if user else None),
        )
        await update.message.reply_text(
            f"✅ Linked successfully. citizen_id={citizen_id}\n"
            "You can now request certificate and receive trigger alerts."
        )
    except Exception as exc:
        await update.message.reply_text(f"❌ Linking failed: {exc}")

# COMMAND ----------

# DBTITLE 1,Message Handler
async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_text = update.message.text
    print(f"📥 Received: {user_text}")

    try:
        await update.message.reply_text("🔎 Scanning the Sovereign Vault for your rights...")

        search_results = w.vector_search_indexes.query_index(
            index_name=VS_INDEX,
            columns=["scheme_name", "benefit_description", "application_url", "scheme_id"],
            query_text=user_text,
            num_results=3,
        )

        matches = search_results.result.data_array
        if not matches:
            await update.message.reply_text(
                "I couldn't find a direct match. Tell me more about your location or occupation."
            )
            return

        prompt = (
            f"User Query: {user_text}\n"
            f"Matches: {json.dumps(matches)}\n"
            "Explain why these match and offer to generate a PDF certificate. Reply in regional language."
        )

        ai_response = w.serving_endpoints.query(
            name=LLM_ENDPOINT,
            messages=[ChatMessage(role=ChatMessageRole.USER, content=prompt)],
        )

        keyboard = [[InlineKeyboardButton("📄 Generate My PDF Certificate", callback_data="gen_cert")]]
        await update.message.reply_text(
            ai_response.choices[0].message.content,
            reply_markup=InlineKeyboardMarkup(keyboard),
        )

    except Exception as exc:
        print(f"❌ Error: {exc}")
        await update.message.reply_text("🔧 System maintenance in progress. Please try again.")

# COMMAND ----------

# DBTITLE 1,Button Callback Handler
async def button_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

    if query.data != "gen_cert":
        return

    chat_id = query.message.chat_id
    await query.edit_message_text("🧾 Fetching your Sovereign Adhikar Certificate from the Lakehouse...")

    try:
        citizen_id = get_citizen_id_for_chat(chat_id)
        if not citizen_id:
            await query.message.reply_text(
                "❌ Your chat is not linked to a citizen profile.\n"
                "Use: /link <citizen_id>"
            )
            return

        cert_data = get_certificate_for_citizen(citizen_id)
        pdf_bytes = create_certificate_pdf_bytes(cert_data)

        await send_telegram_message(
            chat_id=chat_id,
            text="✅ Certificate found and generated.",
            pdf_bytes=pdf_bytes,
            pdf_filename=f"Adhikar_Certificate_{cert_data.get('certificate_id', citizen_id)}.pdf",
            token=TELEGRAM_TOKEN,
        )

    except Exception as exc:
        await query.message.reply_text(f"❌ Could not fetch your certificate: {exc}")

# COMMAND ----------

# DBTITLE 1,Start Bot
app = ApplicationBuilder().token(TELEGRAM_TOKEN).build()
app.add_handler(CommandHandler("start", start_command))
app.add_handler(CommandHandler("link", link_command))
app.add_handler(MessageHandler(filters.TEXT & (~filters.COMMAND), handle_message))
app.add_handler(CallbackQueryHandler(button_callback))

print("🚀 SOVEREIGN BRIDGE LIVE: Running integrated Adhikar-Aina Telegram bot...")

await app.initialize()
await app.start()
await app.updater.start_polling()

print("✅ Bot is running and waiting for messages.")
print("⚠️ NOTE: Stop this cell manually when you want to shut down the bot.")

try:
    while True:
        await asyncio.sleep(1)
except asyncio.CancelledError:
    print("Stopping bot...")
finally:
    await app.updater.stop()
    await app.stop()
    await app.shutdown()
