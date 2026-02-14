"""
b-agent — Interactive chat loop.

Run this to start chatting with your agent in the terminal:
    python main.py

Commands:
    /clear  — Reset conversation history
    /quit   — Exit the program
"""

from rich.console import Console
from rich.markdown import Markdown

from agent import Agent


def main():
    console = Console()
    console.print("\n[bold cyan]b-agent[/bold cyan] — AI Assistant")
    console.print("Type [bold]/quit[/bold] to exit, [bold]/clear[/bold] to reset memory.\n")

    try:
        agent = Agent()
    except ValueError as e:
        console.print(f"[bold red]Setup error:[/bold red] {e}")
        return

    while True:
        try:
            user_input = console.input("[bold green]You:[/bold green] ").strip()
        except (EOFError, KeyboardInterrupt):
            console.print("\nGoodbye!")
            break

        if not user_input:
            continue
        if user_input == "/quit":
            console.print("Goodbye!")
            break
        if user_input == "/clear":
            agent.clear_history()
            console.print("[dim]History cleared.[/dim]\n")
            continue

        console.print("[dim]Thinking...[/dim]")
        try:
            response = agent.chat(user_input)
            console.print()
            console.print(Markdown(response))
            console.print()
        except Exception as e:
            console.print(f"[bold red]Error:[/bold red] {e}\n")


if __name__ == "__main__":
    main()
