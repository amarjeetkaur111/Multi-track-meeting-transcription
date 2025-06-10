#!/usr/bin/env python3
"""Generate a language-aware, fully structured meeting summary with absolute dates.

Fixes / improvements
--------------------
* **Timezone-aware date** – Replace deprecated ``datetime.utcfromtimestamp`` with
  ``datetime.fromtimestamp(ts, datetime.timezone.utc)`` to silence Pylance and
  keep the datetime explicitly in UTC.
* **Rightmost epoch** – We continue to take the last 13- or 10-digit epoch in
  ``file_id`` so mixed IDs resolve correctly.
* **Debug flag** – ``DEBUG_DATE=1`` prints the extracted date for verification.
"""


from __future__ import annotations

import datetime as dt
import os
import re
import sys
from pathlib import Path

from dotenv import load_dotenv
from openai import AzureOpenAI

load_dotenv()  # Load .env as early as possible

_EPOCH_13 = re.compile(r"\d{13}")  # milliseconds
_EPOCH_10 = re.compile(r"\d{10}")  # seconds (fallback)


def _extract_meeting_date(file_id: str) -> dt.datetime | None:
    """Return UTC datetime (tz‑aware) from last epoch in *file_id*."""
    ms = _EPOCH_13.findall(file_id)
    sec = _EPOCH_10.findall(file_id)
    ts_str = ms[-1] if ms else sec[-1] if sec else None
    if not ts_str:
        return None
    ts = int(ts_str)
    if len(ts_str) == 13:
        ts //= 1000
    try:
        return dt.datetime.fromtimestamp(ts, tz=dt.timezone.utc)
    except (OSError, ValueError):
        return None


def _fmt_iso(d: dt.datetime | None) -> str:
    return d.astimezone(dt.timezone.utc).strftime("%Y-%m-%d") if d else "UNKNOWN-DATE"


def _fmt_human(d: dt.datetime | None) -> str:
    if not d:
        return "UNKNOWN-DATE"
    return f"{d.strftime('%B')} {d.day}, {d.year}"  # e.g. June 3, 2025


# ---------------------------------------------------------------------------
# Main logic
# ---------------------------------------------------------------------------
def main():
    from webhook_utils import async_send_webhook  # <-- import here

    if len(sys.argv) != 2:
        print("Usage: gpt_summary.py <FILE_ID>", file=sys.stderr)
        sys.exit(1)

    file_id = sys.argv[1]
    meeting_dt = _extract_meeting_date(file_id)
    meeting_date_iso = _fmt_iso(meeting_dt)
    meeting_date_human = _fmt_human(meeting_dt)

    if os.getenv("DEBUG_DATE"):
        print(f"[DEBUG] file_id='{file_id}' → iso={meeting_date_iso} human='{meeting_date_human}'")
    
    base_dir = Path(os.getenv("TRANSCRIPTS_FOLDER", "."))
    speaker_path    = Path(os.getenv("TRANSCRIPTS_FOLDER")) /f"{file_id}_speakers.txt"
    default_path    = Path(os.getenv("TRANSCRIPTS_FOLDER")) /f"{file_id}.txt"
    transcript_path = speaker_path if os.path.isfile(speaker_path) else default_path
    chat_path       = Path(os.getenv("TRANSCRIPTS_FOLDER")) /f"{file_id}_chat.txt"
    summary_path    = Path(os.getenv("TRANSCRIPTS_FOLDER")) /f"{file_id}_summary.txt"

    # 1. Check transcript exists
    if not os.path.isfile(transcript_path):
        print(f"Transcript not found: {transcript_path}", file=sys.stderr)
        sys.exit(2)

    # 2. Load ENV vars (make sure you have a .env or exported vars on that server)
    # load_dotenv()

    client = AzureOpenAI(
        azure_endpoint = os.getenv("AZURE_OPENAI_ENDPOINT", "").rstrip("/"),
        api_key        = os.getenv("AZURE_OPENAI_API_KEY"),
        api_version    = os.getenv("AZURE_OPENAI_API_VERSION"),
    )
    deployment_name = os.getenv("AZURE_CHAT_DEPLOYMENT")

    # ------------------------------------------------------------------
    # Load transcript / chat
    # ------------------------------------------------------------------
    transcript = transcript_path.read_text(encoding="utf-8")
    chat = chat_path.read_text(encoding="utf-8") if chat_path.is_file() else ""

    # print(f"--- Transcript ({transcript_path}) ---\n{transcript}\n--- End transcript ---")

    # print(f"--- Chat log ({chat_path}) ---\n{chat or '[empty]'}\n--- End chat log ---\n")


    # 5. Build messages
    system_prompt = f"""You are an Expert Universal Meeting Summarizer.  
        The meeting took place on **{meeting_date_human}** (original ISO: {meeting_date_iso}). When the transcript or chat includes
        relative expressions such as “next week”, “last Monday”, “previous quarter”, or similar, **append the
        exact calendar date** in parentheses, calculated relative to this meeting date. Use ISO format
        `YYYY‑MM‑DD`. Keep the original wording intact, e.g.:

        * “We will deliver the draft **next week (2025-06-11)**.”

        Follow these rules in addition to the existing instructions below.

        1. **Detect** the primary language of the transcript which is the mostly spoken language during the transcript. 
            • At the very top of the output, write the language name **translated into that same language** followed by the meeting date, also formatted in that language’s normal long-date style.  
             – *Example (Spanish)*: “**Español – 9 de junio de 2025**”  
             – *Example (Arabic)*: “**العربية – ٩ يونيو ٢٠٢٥**”
        2. **Translate** every static label—section titles, headings, bullet-labels—into that language.  
        3. Produce **all** output (headings + content) in the primary language.
        4. if the transcript is primarily in English then summary should be in English and should not include any text from other languages rather than the primary language, The summary should remain consistent with the primary language throughout.
        5. **Use emojis/icons in every language**, not just English.  
           • Add a relevant emoji to each major heading *and* to each bullet-label when appropriate (📋, ✅, 📌, ❓, 🗓️, 🔗, etc.).  
           • Choose culturally neutral emojis so they work across languages.
           
        #1. [Translate “Full Narrative Summary”]  
        – Multi-paragraph overview, capture flow & tone, omit “um/uh” and other filler.

        #2. [Translate “Time-Stamped Transcript Index”]  
        – For each key exchange or section, include the timestamp (e.g. “00:05:23 – Budget decision”).

        #3. [Translate “Structured Key Points”]  
        - **[Translate Executive Summary]**: State the primary objectives AND One-paragraph high-level recap.  
        - **[Translate Topics Discussed]**: Bullet-list of main subjects/topics and for each agenda/topic, summarize the core points, arguments, or teachings.
        - **[Translate Decisions & Outcomes]**: What was decided, approved, or deferred and For each, note who proposed it and who agreed.  
        - **[Translate Action Items & Owners]**: Task → responsible person → deadline (if mentioned).  
        - **[Translate Open Questions & Follow-Ups]**: Capture any open questions, doubts, Unresolved issues needing attention or topics deferred for later discussion.
        - **[Translate Notable Quotes]**: Exact wording of important statements, point or decision occurs, include a timestamp (e.g. “(12:15 PM)”).  
        - **[Translate Assessments & Homeworks] with deadline date if mentioned.  
        - **[Translate Shared Materials]**: List any URLs, document titles, filenames, book names, slide decks, or other resources mentioned or shared in chat/transcript AND Include short descriptions (e.g. “Project Plan – shared via URL at 10:05”)  
        - **[Translate Upcoming Events & Deadlines]**: Exams, meetings, celebrations, holidays. Explicit due dates or calendar cues
        - **[Translate Special Occasions] such as holidays, days off.

        #4. [Translate “Chat Highlights”] – If no chat, write “No chat to summarize” else summarize the side-conversations, questions, and action items from the chat log

        **Formatting**: Markdown headings, omit filler, preserve exact wording for quotes, write “None mentioned” for empty sections. Provide a concise emoji-enhanced summary.
        """

    user_content = "\n".join(
        [
            f"This meeting occurred on {meeting_date_human}.",
            "\nHere is the transcript:\n\n" + transcript,
            ("Here is the chat log:\n\n" + chat) if chat else "No chat log was provided for this meeting.",
            "\n\nPlease provide the detailed summary as specified.",
        ]
    )

    messages = [
        {"role": "system", "content": system_prompt},
        {"role": "user",   "content": user_content}
    ]

    # 6. Call Azure OpenAI Chat
    resp = client.chat.completions.create(
        model       = deployment_name,
        messages    = messages,
        temperature = 0,
    )

    summary = resp.choices[0].message.content

    # 7. Write out the summary
    with open(summary_path, "w", encoding="utf-8") as f:
        f.write(summary)

    print(f"✔ Summary written to {summary_path}")
    # Send Webhook Notification to Laravel
    print(f"Sending Webhook for Summary: {summary_path}")
    bbbUrl = os.getenv("BBB_URL")
    webhook_data = {
        "file_id": file_id,
        "script":  f"{bbbUrl.rstrip('/')}/{file_id}.txt",
        "status": "done",   
        "type": "summary"
    }
    async_send_webhook(webhook_data)

    sys.exit(0)


if __name__ == "__main__":
    main()
