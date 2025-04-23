#!/usr/bin/env python3
import os
import sys
# from dotenv import load_dotenv
from openai import AzureOpenAI

def main():
    if len(sys.argv) != 2:
        print("Usage: gpt_summary.py <FILE_ID>", file=sys.stderr)
        sys.exit(1)

    file_id = sys.argv[1]
    speaker_path    = f"/transcripts/scripts/{file_id}_speakers.txt"
    default_path    = f"/transcripts/scripts/{file_id}.txt"
    transcript_path = speaker_path if os.path.isfile(speaker_path) else default_path
    summary_path    = f"/transcripts/scripts/{file_id}_summary.txt"

    # 1. Check transcript exists
    if not os.path.isfile(transcript_path):
        print(f"Transcript not found: {transcript_path}", file=sys.stderr)  
        sys.exit(2)

    # 2. Load ENV vars (make sure you have a .env or exported vars on that server)
    # load_dotenv()  

    client = AzureOpenAI(
        azure_endpoint = "https://biggerbluebutton-gpt-4o.openai.azure.com",
        api_key        = "7il53pob9sPgITG5qowxkunJZhevEiKWzDmCsEgIXjXiefP5uHdAJQQJ99BAACYeBjFXJ3w3AAABACOGuejq",
        api_version    = "2024-10-21",
    )


    # openai.api_type    = "azure"
    # openai.api_base    = "https://biggerbluebutton-gpt-4o.openai.azure.com"
    # openai.api_version = "2024-10-21"
    # openai.api_key     = "7il53pob9sPgITG5qowxkunJZhevEiKWzDmCsEgIXjXiefP5uHdAJQQJ99BAACYeBjFXJ3w3AAABACOGuejq"
    deployment_name    = "gpt-4o-mini"

    # 3. Read transcript
    with open(transcript_path, "r", encoding="utf-8") as f:
        transcript = f.read()

    # 4. Build messages
    system_prompt = """You are an expert meeting-and-transcript summarization assistant.
            0. **Attendance**  
            – List participants, roles, absentees, and late arrivals.

            1. **Full Narrative Summary**  
            – [As before: multi-paragraph, same language, capture flow & tone, omit filler words.]

            1a. **Time-Stamped Transcript Index**  
            – For each key exchange or section, include the timestamp (e.g. “00:05:23 – Budget decision”).

            2. **Structured Key-Points**  
            A. Executive Summary  
            B. Key Topics Discussed  
            C. Decisions Made  
            D. Action Items  
            E. Open Questions & Follow-Ups  
            F. Notable Quotes  
            G. Assessments & Homeworks  
            H. Tasks by Person  
            I. Upcoming Events  
            J. Special Occasions  
            K. Important Topics Emphasized  
            L. Materials Shared (slide titles, file names, and timestamps)

            **Formatting Rules:**  
            - Use clear Markdown headings.  
            - Strip out “um,” “uh,” and filler.  
            - Preserve exact wording for quotes only.  
            - If a section has no entries, write “None mentioned.”
            """
    messages = [
        {"role": "system", "content": system_prompt},
        {
          "role": "user",
          "content": f"Here is the transcript:\n\n{transcript}\n\nPlease provide the detailed summary as specified."
        }
    ]

    # 5. Call Azure OpenAI Chat
    resp = client.chat.completions.create(
        model    = deployment_name,
        messages = messages,
        temperature=0,
    )

    summary = resp.choices[0].message.content

    # 6. Write out the summary
    with open(summary_path, "w", encoding="utf-8") as f:
        f.write(summary)

    print(f"✔ Summary written to {summary_path}")
    sys.exit(0)


if __name__ == "__main__":
    main()
