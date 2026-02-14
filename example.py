"""
Example usage of b-agent — shows how to use the Agent class programmatically.

Run:  python example.py
"""

from agent import Agent


def main():
    agent = Agent()

    # Example 1: General knowledge (no tool needed)
    print("=== Example 1: General question ===")
    response = agent.chat("What is a Python decorator? Explain in 2 sentences.")
    print(response)
    print()

    # Example 2: Web search
    print("=== Example 2: Web search ===")
    response = agent.chat("Search the web for the latest Python 3.13 features.")
    print(response)
    print()

    # Example 3: File reading
    print("=== Example 3: Read a file ===")
    response = agent.chat("Read the requirements.txt file in the current directory.")
    print(response)
    print()

    # Example 4: Task automation
    print("=== Example 4: Run a command ===")
    response = agent.chat("What Python version is installed? Run a command to check.")
    print(response)
    print()

    # Example 5: Memory — the agent remembers earlier messages
    print("=== Example 5: Memory test ===")
    response = agent.chat("What was the first question I asked you?")
    print(response)
    print()


if __name__ == "__main__":
    main()
