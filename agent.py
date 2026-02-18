"""
b-agent — A Python AI agent powered by Claude.

This is the main agent file. It:
  1. Maintains conversation history (memory)
  2. Decides which tool to use based on your request
  3. Calls the tool, then uses the result to give you a final answer

Usage:
    from agent import Agent
    agent = Agent()
    response = agent.chat("What is the weather API endpoint for OpenWeatherMap?")
"""

import json
import os

import anthropic
from dotenv import load_dotenv

# Import all tools
from tools.web_search import search_web, fetch_page
from tools.file_processor import read_file, list_files
from tools.task_runner import run_command, run_python
from tools.api_client import api_request

# Load environment variables from .env
load_dotenv()

# ---------------------------------------------------------------------------
# Tool definitions — these tell Claude what tools are available and how to
# call them. Claude will pick the right tool based on your message.
# ---------------------------------------------------------------------------
TOOL_DEFINITIONS = [
    {
        "name": "search_web",
        "description": "Search the web for information on a topic. Use this when the user asks a question that requires up-to-date or external information.",
        "input_schema": {
            "type": "object",
            "properties": {
                "query": {
                    "type": "string",
                    "description": "The search query.",
                },
            },
            "required": ["query"],
        },
    },
    {
        "name": "fetch_page",
        "description": "Fetch and read the text content of a web page given its URL.",
        "input_schema": {
            "type": "object",
            "properties": {
                "url": {
                    "type": "string",
                    "description": "The URL of the web page to read.",
                },
            },
            "required": ["url"],
        },
    },
    {
        "name": "read_file",
        "description": "Read the contents of a local file (text, PDF, DOCX, etc.).",
        "input_schema": {
            "type": "object",
            "properties": {
                "file_path": {
                    "type": "string",
                    "description": "Path to the file.",
                },
            },
            "required": ["file_path"],
        },
    },
    {
        "name": "list_files",
        "description": "List files and folders in a directory.",
        "input_schema": {
            "type": "object",
            "properties": {
                "directory": {
                    "type": "string",
                    "description": "Path to the directory. Defaults to current directory.",
                },
            },
            "required": [],
        },
    },
    {
        "name": "run_command",
        "description": "Run a shell command (e.g., ls, curl, git status). Use for task automation.",
        "input_schema": {
            "type": "object",
            "properties": {
                "command": {
                    "type": "string",
                    "description": "The shell command to run.",
                },
            },
            "required": ["command"],
        },
    },
    {
        "name": "run_python",
        "description": "Execute a short Python code snippet and return the output.",
        "input_schema": {
            "type": "object",
            "properties": {
                "code": {
                    "type": "string",
                    "description": "Python code to execute.",
                },
            },
            "required": ["code"],
        },
    },
    {
        "name": "api_request",
        "description": "Make an HTTP request to an external API (GET, POST, etc.).",
        "input_schema": {
            "type": "object",
            "properties": {
                "url": {
                    "type": "string",
                    "description": "The API endpoint URL.",
                },
                "method": {
                    "type": "string",
                    "description": "HTTP method: GET, POST, PUT, DELETE.",
                    "default": "GET",
                },
                "headers": {
                    "type": "object",
                    "description": "Optional HTTP headers as key-value pairs.",
                },
                "body": {
                    "type": "object",
                    "description": "Optional JSON body for POST/PUT requests.",
                },
            },
            "required": ["url"],
        },
    },
]

# Map tool names to their Python functions
TOOL_FUNCTIONS = {
    "search_web": search_web,
    "fetch_page": fetch_page,
    "read_file": read_file,
    "list_files": list_files,
    "run_command": run_command,
    "run_python": run_python,
    "api_request": api_request,
}

# ---------------------------------------------------------------------------
# The Agent class
# ---------------------------------------------------------------------------

SYSTEM_PROMPT = """You are b-agent, a helpful AI assistant with access to tools.

You can:
- Search the web for current information
- Read local files (text, PDF, DOCX)
- List directory contents
- Run shell commands for task automation
- Execute Python code snippets
- Make HTTP API requests

Guidelines:
- Use tools when the user's request requires external data or actions.
- For general knowledge questions, answer directly without tools.
- Always explain what you did and summarize the results clearly.
- Be concise but thorough.
"""


class Agent:
    """A conversational AI agent with tool-use capabilities.

    The agent keeps a conversation history so it remembers prior messages
    within the same session.
    """

    def __init__(self, model: str = "claude-sonnet-4-5-20250929"):
        """Initialize the agent.

        Args:
            model: The Anthropic model to use (default: Claude Sonnet 4.5).
        """
        api_key = os.getenv("ANTHROPIC_API_KEY")
        if not api_key:
            raise ValueError(
                "ANTHROPIC_API_KEY not set. "
                "Copy .env.example to .env and add your key."
            )
        self.client = anthropic.Anthropic(api_key=api_key)
        self.model = model
        # Conversation history — this is how the agent "remembers"
        self.history: list[dict] = []

    def chat(self, user_message: str) -> str:
        """Send a message to the agent and get a response.

        This is the main method you'll use. It:
          1. Adds your message to the conversation history
          2. Sends history + tools to Claude
          3. If Claude wants to use a tool, executes it and loops back
          4. Returns Claude's final text response

        Args:
            user_message: Your message / question / request.

        Returns:
            The agent's text response.
        """
        # Step 1: Add the user message to history
        self.history.append({"role": "user", "content": user_message})

        # Step 2: Loop until Claude gives a final text response (not a tool call)
        while True:
            response = self.client.messages.create(
                model=self.model,
                max_tokens=4096,
                system=SYSTEM_PROMPT,
                tools=TOOL_DEFINITIONS,
                messages=self.history,
            )

            # Step 3: Check if Claude wants to use a tool
            if response.stop_reason == "tool_use":
                # Add Claude's response (which includes the tool_use block) to history
                self.history.append({"role": "assistant", "content": response.content})

                # Execute each tool call and collect results
                tool_results = []
                for block in response.content:
                    if block.type == "tool_use":
                        result = self._execute_tool(block.name, block.input)
                        tool_results.append(
                            {
                                "type": "tool_result",
                                "tool_use_id": block.id,
                                "content": result,
                            }
                        )

                # Add tool results to history so Claude can see them
                self.history.append({"role": "user", "content": tool_results})

            else:
                # Claude gave a final text answer — extract it
                text_parts = [
                    block.text for block in response.content if block.type == "text"
                ]
                assistant_text = "\n".join(text_parts)

                # Save to history and return
                self.history.append(
                    {"role": "assistant", "content": assistant_text}
                )
                return assistant_text

    def _execute_tool(self, tool_name: str, tool_input: dict) -> str:
        """Run a tool by name with the given input and return the result.

        Args:
            tool_name: Name of the tool to execute.
            tool_input: Dict of arguments to pass to the tool function.

        Returns:
            The tool's output as a string.
        """
        func = TOOL_FUNCTIONS.get(tool_name)
        if not func:
            return f"Unknown tool: {tool_name}"

        try:
            return func(**tool_input)
        except Exception as e:
            return f"Tool error ({tool_name}): {e}"

    def clear_history(self):
        """Clear conversation history to start fresh."""
        self.history.clear()
