"""Google Calendar tool — read-only access to upcoming events."""

import os
from datetime import datetime, timedelta, timezone

SCOPES = ["https://www.googleapis.com/auth/calendar.readonly"]
CREDENTIALS_FILE = os.path.join(os.path.dirname(__file__), "..", "credentials.json")
TOKEN_FILE = os.path.join(os.path.dirname(__file__), "..", "token.json")


def _get_calendar_service():
    """Authenticate and return a Google Calendar API service object."""
    from google.auth.transport.requests import Request
    from google.oauth2.credentials import Credentials
    from google_auth_oauthlib.flow import InstalledAppFlow
    from googleapiclient.discovery import build

    creds = None

    if os.path.exists(TOKEN_FILE):
        creds = Credentials.from_authorized_user_file(TOKEN_FILE, SCOPES)

    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            if not os.path.exists(CREDENTIALS_FILE):
                raise FileNotFoundError(
                    "credentials.json not found. Download it from Google Cloud Console "
                    "and place it in the project root."
                )
            flow = InstalledAppFlow.from_client_secrets_file(CREDENTIALS_FILE, SCOPES)
            creds = flow.run_local_server(port=0)

        with open(TOKEN_FILE, "w") as f:
            f.write(creds.to_json())

    return build("calendar", "v3", credentials=creds)


def get_calendar_events(days: int = 1) -> str:
    """Fetch upcoming calendar events.

    Args:
        days: Number of days ahead to look (default: 1 = today only).

    Returns:
        Formatted string of events, or a message if none found.
    """
    try:
        service = _get_calendar_service()

        now = datetime.now(timezone.utc)
        end = now + timedelta(days=days)

        result = (
            service.events()
            .list(
                calendarId="primary",
                timeMin=now.isoformat(),
                timeMax=end.isoformat(),
                singleEvents=True,
                orderBy="startTime",
            )
            .execute()
        )

        events = result.get("items", [])
        if not events:
            return f"No events found in the next {days} day(s)."

        lines = [f"Events in the next {days} day(s):\n"]
        for event in events:
            start = event["start"].get("dateTime", event["start"].get("date"))
            end_time = event["end"].get("dateTime", event["end"].get("date"))
            summary = event.get("summary", "(No title)")
            location = event.get("location", "")

            line = f"• {summary}\n  {start} → {end_time}"
            if location:
                line += f"\n  Location: {location}"
            lines.append(line)

        return "\n".join(lines)

    except FileNotFoundError as e:
        return str(e)
    except Exception as e:
        return f"Calendar error: {e}"
