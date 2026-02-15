# b-agent

A Python AI agent powered by Claude with tool-use capabilities.

## Features

- **Conversational Memory** — Remembers context across messages in a session
- **Web Search** — Search the web and fetch page content via DuckDuckGo
- **File Processing** — Read text, PDF, and DOCX files
- **API Integration** — Make HTTP requests to external APIs
- **Task Automation** — Run shell commands and Python snippets
- **Google Calendar** — Check upcoming events from your calendar

## Setup

```bash
# Clone the repo
git clone https://github.com/bestes206/b-agent.git
cd b-agent

# Create a virtual environment and install dependencies
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

# Add your Anthropic API key
cp .env.example .env
# Edit .env and add your key
```

## Usage

### Interactive Chat

```bash
python main.py
```

Type your messages to chat with the agent. Use `/clear` to reset conversation history and `/quit` to exit.

### Programmatic Usage

```python
from agent import Agent

agent = Agent()

# Ask a question
response = agent.chat("What is Python?")
print(response)

# The agent remembers previous messages
response = agent.chat("What did I just ask you?")
print(response)

# Use tools automatically
response = agent.chat("Search the web for the latest Python news.")
print(response)

# Reset memory
agent.clear_history()
```

### Examples

```bash
python example.py
```

## Google Calendar

The agent can check your Google Calendar when you ask things like "What's on my calendar today?"

### Setup

1. Go to the [Google Cloud Console](https://console.cloud.google.com/)
2. Create a new project (or select an existing one)
3. Enable the **Google Calendar API** (APIs & Services → Enable APIs)
4. Create OAuth credentials (APIs & Services → Credentials → Create Credentials → OAuth client ID → Desktop app)
5. Download the JSON file and save it as `credentials.json` in the project root
6. The first time you ask about your calendar, a browser window will open to sign in with Google

## Telegram Bot

You can chat with b-agent on your phone via Telegram.

### Setup

1. Open Telegram and message [@BotFather](https://t.me/BotFather)
2. Send `/newbot`, pick a name, and copy the token
3. Add the token to your `.env`:
   ```
   TELEGRAM_BOT_TOKEN=your-token-here
   ```
4. Start the bot:
   ```bash
   python telegram_bot.py
   ```
5. Open your bot in Telegram and start chatting!

Use `/clear` in the chat to reset conversation history.

## Project Structure

```
b-agent/
├── agent.py            # Core Agent class with tool-use loop
├── main.py             # Interactive terminal chat
├── telegram_bot.py     # Telegram bot interface
├── example.py          # Programmatic usage examples
├── requirements.txt    # Python dependencies
├── .env.example        # API key template
├── tools/
│   ├── web_search.py   # Web search and page fetching
│   ├── file_processor.py  # Text, PDF, and DOCX reading
│   ├── api_client.py      # HTTP API requests
│   ├── google_calendar.py # Google Calendar integration
│   └── task_runner.py     # Shell commands and Python execution
└── utils/              # Helper modules
```

## Requirements

- Python 3.8+
- An [Anthropic API key](https://console.anthropic.com/)
