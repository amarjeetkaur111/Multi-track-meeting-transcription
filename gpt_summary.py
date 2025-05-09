#!/usr/bin/env python3
import os
import sys
# from dotenv import load_dotenv
from openai import AzureOpenAI
from dotenv import load_dotenv
from pathlib import Path
load_dotenv() 

def main():
    from webhook_utils import async_send_webhook  # <-- import here

    if len(sys.argv) != 2:
        print("Usage: gpt_summary.py <FILE_ID>", file=sys.stderr)
        sys.exit(1)

    file_id = sys.argv[1]
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

    # 3. Read transcript
    with open(transcript_path, "r", encoding="utf-8") as f:
        transcript = f.read()


    # print(f"--- Transcript ({transcript_path}) ---\n{transcript}\n--- End transcript ---")

    # 4. Read chat if it exists
    chat = ""
    if os.path.isfile(chat_path):
        with open(chat_path, "r", encoding="utf-8") as f:
            chat = f.read()

    # print(f"--- Chat log ({chat_path}) ---\n{chat or '[empty]'}\n--- End chat log ---\n")


    # 5. Build messages
    system_prompt = """You are an Expert Universal Meeting Summarizer.  
        1. **Detect** the primary language of the transcript which is the mostly spoken language during the transcript. And write the detected language name at the start 
        2. **Translate** every static label—section titles, headings, bullet-labels—into that language.  
        3. Produce **all** output (headings + content) in the primary language.
        4. if the transcript is primarily in English then summary should be in English and should not include any text from other languages rather than the primary language, The summary should remain consistent with the primary language throughout.

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

        3. **[Translate Chat Highlights]**  
        – If the chat log is empty, write “[Translate No chat to summarize].” else Summarize the side-conversations, questions, and action items from the chat log

        **[Translate Formatting Rules:]**  
        - Use clear Markdown headings.  
        - Strip out “um,” “uh,” and filler.  
        - Preserve exact wording for quotes only.  
        - If a section has no entries, write “[Translate None mentioned].”
        """

    user_content = f"Here is the transcript:\n\n{transcript}\n\n"
    if chat:
        user_content += f"Here is the chat log:\n\n{chat}\n\n"
    else:
        user_content += "No chat log was provided for this meeting.\n\n"
    user_content += "Please provide the detailed summary as specified."

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
