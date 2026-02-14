"""
Telegram bot interface for b-agent.

Run this script to start a Telegram bot that forwards messages to the Agent.
Each Telegram chat gets its own Agent instance with separate conversation memory.

Usage:
    1. Set TELEGRAM_BOT_TOKEN in your .env file
    2. python telegram_bot.py
"""

import logging
import os

from dotenv import load_dotenv
from telegram import Update
from telegram.ext import (
    Application,
    CommandHandler,
    ContextTypes,
    MessageHandler,
    filters,
)

from agent import Agent

load_dotenv()

logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)

# One Agent per chat so each conversation has its own memory
agents: dict[int, Agent] = {}


def get_agent(chat_id: int) -> Agent:
    """Return the Agent for a chat, creating one if needed."""
    if chat_id not in agents:
        agents[chat_id] = Agent()
    return agents[chat_id]


async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle the /start command."""
    await update.message.reply_text(
        "Hey! I'm b-agent. Send me a message and I'll do my best to help.\n\n"
        "Commands:\n"
        "/clear — reset conversation history\n"
    )


async def clear_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle the /clear command."""
    chat_id = update.effective_chat.id
    if chat_id in agents:
        agents[chat_id].clear_history()
    await update.message.reply_text("Conversation history cleared.")


async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Forward text messages to the Agent and reply with the response."""
    chat_id = update.effective_chat.id
    agent = get_agent(chat_id)

    # Show a typing indicator while the agent thinks
    await update.effective_chat.send_action("typing")

    try:
        response = agent.chat(update.message.text)
    except Exception as e:
        logger.error("Agent error for chat %s: %s", chat_id, e)
        response = f"Something went wrong: {e}"

    # Telegram messages have a 4096-char limit — split if needed
    for i in range(0, len(response), 4096):
        await update.message.reply_text(response[i : i + 4096])


def main() -> None:
    """Start the Telegram bot."""
    token = os.getenv("TELEGRAM_BOT_TOKEN")
    if not token:
        raise ValueError(
            "TELEGRAM_BOT_TOKEN not set. "
            "Copy .env.example to .env and add your Telegram bot token."
        )

    app = Application.builder().token(token).build()

    app.add_handler(CommandHandler("start", start_command))
    app.add_handler(CommandHandler("clear", clear_command))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))

    logger.info("Bot is running. Press Ctrl+C to stop.")
    app.run_polling()


if __name__ == "__main__":
    main()
