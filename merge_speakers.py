#!/usr/bin/env python3
import logging
import os
import re
import sys
import shutil
import xml.etree.ElementTree as ET

# -----------------------------------------------------------------------------
# Configure logging to a file named "merge_speaker.log" in the same directory as this script.
# If you want to log to a different path, adjust LOG_FILE_PATH accordingly.
# -----------------------------------------------------------------------------
logging.basicConfig(
    stream=sys.stdout,  # <-- log to console (stdout)
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)

def parse_xml_with_recording_segments(xml_path):
    """
    Parse BBB events.xml while respecting start/stop recording logic.
    Returns:
      user_map: { userId -> displayName }
      talk_intervals: { userId -> list of (start_ms, end_ms) }
        in 'recording timeline' (skips paused sections).
    """

    if not os.path.exists(xml_path):
        logging.warning(f"events.xml not found at: {xml_path}")
        return {}, {}

    # Attempt to parse
    try:
        tree = ET.parse(xml_path)
        root = tree.getroot()
    except Exception as e:
        logging.error(f"Error parsing {xml_path}: {e}")
        return {}, {}

    events = root.findall('event')
    if not events:
        logging.warning(f"No <event> elements found in {xml_path}")
        return {}, {}

    # Sort all events by their numeric 'timestamp'
    events.sort(key=lambda e: int(e.get('timestamp', '0')))

    user_map = {}
    talk_intervals = {}

    is_recording = False
    recording_started_at = None
    accumulated_recorded_ms = 0
    ongoing_talk = {}  # userId -> talk_start_time_in_recording_ms

    for ev in events:
        eventname = ev.get('eventname')
        timestamp_str = ev.get('timestamp')
        if not timestamp_str:
            continue
        try:
            ts_abs = int(timestamp_str)
        except ValueError:
            continue

        # 1) ParticipantJoinEvent => record user name
        if eventname == "ParticipantJoinEvent":
            user_id = ev.findtext('userId')
            user_name = ev.findtext('name')
            if user_id and user_name:
                user_map[user_id] = user_name
                logging.debug(f"ParticipantJoinEvent => user_id={user_id}, name={user_name}")

        # 2) RecordStatusEvent => start/stop recording
        elif eventname == "RecordStatusEvent":
            status = ev.findtext('status')
            if status is None:
                continue

            if status.lower() == 'true':
                # Start new recording segment
                recording_started_at = ts_abs
                is_recording = True
                logging.info(f"Recording started at {ts_abs}ms (accumulated={accumulated_recorded_ms}ms so far)")
            else:
                # Stop current recording
                if is_recording and recording_started_at is not None:
                    delta = ts_abs - recording_started_at
                    accumulated_recorded_ms += delta
                    logging.info(
                        f"Recording stopped at {ts_abs}ms, segment length={delta}ms, "
                        f"accumulated={accumulated_recorded_ms}ms"
                    )
                is_recording = False
                recording_started_at = None

        # 3) ParticipantTalkingEvent => user started/stopped talking
        elif eventname == "ParticipantTalkingEvent":
            talking_str = ev.findtext('talking')
            participant_id = ev.findtext('participant')
            if not talking_str or not participant_id:
                continue

            user_is_talking = (talking_str.lower() == 'true')
            if is_recording and recording_started_at is not None:
                offset_in_this_segment = ts_abs - recording_started_at
                event_actual_time = accumulated_recorded_ms + offset_in_this_segment

                if user_is_talking:
                    ongoing_talk[participant_id] = event_actual_time
                    logging.debug(f"User {participant_id} started talking at {event_actual_time}ms (abs={ts_abs})")
                else:
                    start_time = ongoing_talk.pop(participant_id, None)
                    if start_time is not None:
                        if participant_id not in talk_intervals:
                            talk_intervals[participant_id] = []
                        talk_intervals[participant_id].append((start_time, event_actual_time))
                        logging.debug(f"User {participant_id} stopped talking at {event_actual_time}ms (abs={ts_abs}). "
                                      f"Interval=({start_time}, {event_actual_time})")

        # else ignore other events

    return user_map, talk_intervals

def parse_time_str_to_millis(time_str):
    """
    Convert 'HH:MM:SS.sss' -> total ms, e.g. '00:00:09.400' => 9400
    """
    hh, mm, ssms = time_str.split(':')
    ss, ms = ssms.split('.')
    return ((int(hh) * 3600) + (int(mm) * 60) + int(ss)) * 1000 + int(ms)

def merge_speakers_with_transcript(transcript_file, user_map, talk_intervals, output_file):
    """
    Read lines of form:
      [HH:MM:SS.sss HH:MM:SS.sss] Some text
    -> produce:
      [HH:MM:SS.sss HH:MM:SS.sss] <speaker name>: Some text
    by matching times to user talk intervals in naive overlap.
    """
    all_intervals = []
    for uid, intervals in talk_intervals.items():
        for (st, en) in intervals:
            all_intervals.append((uid, st, en))
    all_intervals.sort(key=lambda x: x[1])  # sort by start time

    logging.info(f"Found {len(all_intervals)} talk intervals. Merging with transcript...")

    pattern = re.compile(r'^\[(\d{2}:\d{2}:\d{2}\.\d{3})\s+(\d{2}:\d{2}:\d{2}\.\d{3})\]\s+(.*)$')
    output_lines = []

    with open(transcript_file, 'r', encoding='utf-8') as fin:
        for line in fin:
            line = line.rstrip()
            if not line:
                continue

            match = pattern.match(line)
            if not match:
                # if line doesn't match the pattern, just keep it as-is
                output_lines.append(line)
                continue

            start_str, end_str, text_content = match.groups()
            start_ms = parse_time_str_to_millis(start_str)
            end_ms   = parse_time_str_to_millis(end_str)

            # naive overlap approach: pick the first user interval that overlaps
            speaker_id = None
            for (uid, s_val, e_val) in all_intervals:
                # overlap check
                if not (end_ms < s_val or start_ms > e_val):
                    speaker_id = uid
                    break

            if speaker_id and speaker_id in user_map:
                speaker_name = user_map[speaker_id]
            else:
                speaker_name = "Unknown Speaker"

            new_line = f"[{start_str} {end_str}] {speaker_name}: {text_content}"
            output_lines.append(new_line)

    with open(output_file, 'w', encoding='utf-8') as fout:
        fout.write("\n".join(output_lines))

    logging.info(f"Speaker-labeled transcript created at {output_file}")

def main():
    if len(sys.argv) < 4:
        print(f"Usage: python {sys.argv[0]} <events.xml> <input_transcript.txt> <output_speaker_transcript.txt>")
        sys.exit(1)

    events_xml = sys.argv[1]
    transcript_txt = sys.argv[2]
    output_txt = sys.argv[3]

    logging.info(f"Loading events from: {events_xml}")
    user_map, talk_intervals = parse_xml_with_recording_segments(events_xml)
    logging.info(f"Found {len(user_map)} user(s), intervals for {len(talk_intervals)} user(s).")

    if not user_map and not talk_intervals:
        logging.warning("No speaker data found. Copying transcript unchanged.")
        shutil.copy(transcript_txt, output_txt)
        logging.info("Done. No speaker labels added.")
        sys.exit(0)

    merge_speakers_with_transcript(transcript_txt, user_map, talk_intervals, output_txt)
    logging.info("Done.")

if __name__ == "__main__":
    main()
