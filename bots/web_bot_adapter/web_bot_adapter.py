import asyncio
import copy
import datetime
import hashlib
import io
import json
import logging
import os
import shutil
import stat
import subprocess
import tempfile
import threading
import time
import zipfile
import secrets
import socket
import re
from pathlib import Path
from time import sleep

import numpy as np
import requests
from collections import defaultdict, deque
from django.conf import settings
from pyvirtualdisplay import Display
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from websockets.sync.server import serve

from bots.automatic_leave_configuration import AutomaticLeaveConfiguration
from bots.bot_adapter import BotAdapter
from bots.models import ParticipantEventTypes, RecordingViews
from bots.utils import half_ceil, scale_i420

from .debug_screen_recorder import DebugScreenRecorder
from .ui_methods import (
    UiBlockedByCaptchaException,
    UiCouldNotJoinMeetingWaitingForHostException,
    UiCouldNotJoinMeetingWaitingRoomTimeoutException,
    UiIncorrectPasswordException,
    UiLoginAttemptFailedException,
    UiLoginRequiredException,
    UiMeetingNotFoundException,
    UiRequestToJoinDeniedException,
    UiRetryableException,
    UiRetryableExpectedException,
)

logger = logging.getLogger(__name__)


def ensure_chromedriver() -> str:
    driver_path = Path(os.getenv("CHROMEDRIVER_PATH", "/usr/local/bin/chromedriver"))
    if driver_path.exists():
        return str(driver_path)

    driver_path.parent.mkdir(parents=True, exist_ok=True)

    try:
        chrome_version_output = subprocess.check_output(["google-chrome", "--version"], text=True).strip()
        chrome_version = chrome_version_output.split()[2]
    except Exception as exc:
        raise RuntimeError("Unable to determine Google Chrome version") from exc

    base_url = os.getenv("CHROMEDRIVER_BASE_URL", "https://storage.googleapis.com/chrome-for-testing-public")
    download_url = f"{base_url}/{chrome_version}/linux64/chromedriver-linux64.zip"

    response = requests.get(download_url, timeout=60)
    response.raise_for_status()

    with tempfile.TemporaryDirectory() as tmp_dir:
        archive_bytes = io.BytesIO(response.content)
        with zipfile.ZipFile(archive_bytes) as zf:
            member_name = "chromedriver-linux64/chromedriver"
            if member_name not in zf.namelist():
                raise RuntimeError(f"chromedriver binary not found in archive from {download_url}")

            extracted_path = Path(tmp_dir) / "chromedriver"
            with zf.open(member_name) as src, extracted_path.open("wb") as dst:
                shutil.copyfileobj(src, dst)

            extracted_path.chmod(extracted_path.stat().st_mode | stat.S_IEXEC)

            tmp_target = driver_path.parent / f".{driver_path.name}.{os.getpid()}.tmp"
            shutil.move(str(extracted_path), tmp_target)
            os.replace(tmp_target, driver_path)

    return str(driver_path)


class WebBotAdapter(BotAdapter):
    def __init__(
        self,
        *,
        display_name,
        send_message_callback,
        meeting_url,
        add_video_frame_callback,
        wants_any_video_frames_callback,
        add_audio_chunk_callback,
        add_mixed_audio_chunk_callback,
        add_encoded_mp4_chunk_callback,
        upsert_caption_callback,
        upsert_chat_message_callback,
        add_participant_event_callback,
        automatic_leave_configuration: AutomaticLeaveConfiguration,
        recording_view: RecordingViews,
        should_create_debug_recording: bool,
        start_recording_screen_callback,
        stop_recording_screen_callback,
        video_frame_size: tuple[int, int],
        record_chat_messages_when_paused: bool,
        disable_incoming_video: bool,
    ):
        self.display_name = display_name
        self.send_message_callback = send_message_callback
        self.add_audio_chunk_callback = add_audio_chunk_callback
        self.add_mixed_audio_chunk_callback = add_mixed_audio_chunk_callback
        self.add_video_frame_callback = add_video_frame_callback
        self.wants_any_video_frames_callback = wants_any_video_frames_callback
        self.add_encoded_mp4_chunk_callback = add_encoded_mp4_chunk_callback
        self.upsert_caption_callback = upsert_caption_callback
        self.upsert_chat_message_callback = upsert_chat_message_callback
        self.add_participant_event_callback = add_participant_event_callback
        self.start_recording_screen_callback = start_recording_screen_callback
        self.stop_recording_screen_callback = stop_recording_screen_callback
        self.recording_view = recording_view
        self.record_chat_messages_when_paused = record_chat_messages_when_paused
        self.disable_incoming_video = disable_incoming_video
        self.meeting_url = meeting_url

        # This is an internal ID that comes from the platform. It is currently only used for MS Teams.
        self.meeting_uuid = None

        self.video_frame_size = video_frame_size

        self.driver = None

        self.send_frames = True

        self.left_meeting = False
        self.was_removed_from_meeting = False
        self.cleaned_up = False

        self.websocket_port = None
        self.websocket_server = None
        self.websocket_thread = None
        self.last_websocket_message_processed_time = None
        self.websocket_client_connected = False
        self.websocket_client_connected_at = None
        self.websocket_client_disconnected_at = None
        self.last_media_message_processed_time = None
        self.last_audio_message_processed_time = None
        self.first_buffer_timestamp_ms_offset = time.time() * 1000
        self.media_sending_enable_timestamp_ms = None

        self.participants_info = {}
        self.track_labels = {}
        self.only_one_participant_in_meeting_at = None
        self.video_frame_ticker = 0

        self.automatic_leave_configuration = automatic_leave_configuration

        self.should_create_debug_recording = should_create_debug_recording
        self.debug_screen_recorder = None

        self.silence_detection_activated = False
        self.joined_at = None
        self.recording_permission_granted_at = None
        self.meeting_ended_sent = False

        self.ready_to_send_chat_messages = False

        self.recording_paused = False
        self.disable_only_one_auto_leave = False

        # Some platforms (notably Telemost) can surface multiple internal identifiers for the same human
        # (e.g. multiple audio tracks, plus our UI-based active-speaker identifier). To avoid showing
        # "multiple copies" of the same user in transcripts, we can alias participant ids to a single
        # canonical id for the session when it's safe to do so.
        self.participant_id_aliases = {}
        self.last_active_speaker_ui_id = None
        self.last_active_speaker_at = None
        self._pending_per_participant_audio = defaultdict(deque)
        self._pending_per_participant_audio_timers = {}
        self._pending_per_participant_audio_lock = threading.Lock()

    def pause_recording(self):
        self.recording_paused = True

    def start_or_resume_recording(self):
        self.recording_paused = False

    def process_encoded_mp4_chunk(self, message):
        if self.recording_paused:
            return

        self.last_media_message_processed_time = time.time()
        if len(message) > 4:
            encoded_mp4_data = message[4:]
            logger.info(f"encoded mp4 data length {len(encoded_mp4_data)}")
            self.add_encoded_mp4_chunk_callback(encoded_mp4_data)

    def get_participant(self, participant_id):
        participant_id = self.resolve_participant_id(participant_id)
        if participant_id in self.participants_info:
            return {
                "participant_uuid": participant_id,
                "participant_full_name": self.participants_info[participant_id]["fullName"],
                "participant_user_uuid": None,
                "participant_is_the_bot": self.participants_info[participant_id]["isCurrentUser"],
                "participant_is_host": self.participants_info[participant_id].get("isHost", False),
            }

        return None

    def resolve_participant_id(self, participant_id: str) -> str:
        """
        Resolve a participant id through any aliases recorded for this session.
        """
        if not participant_id:
            return participant_id
        seen = set()
        current = participant_id
        while current in self.participant_id_aliases and current not in seen:
            seen.add(current)
            current = self.participant_id_aliases[current]
        return current

    def _is_uuid_like(self, value: str) -> bool:
        value = (value or "").strip()
        if not value:
            return False
        return bool(re.fullmatch(r"[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}", value))

    def _maybe_alias_track_id_to_recent_active_speaker(self, track_id: str, *, now: float | None = None) -> str:
        """
        Telemost can surface opaque WebRTC track ids (UUIDs) that don't map cleanly to a human.
        For those, we attribute audio to the most recent UI-based active speaker (green border)
        to avoid creating extra "UUID participants" and reduce duplicate speakers (e.g. multiple
        copies of "Muzaffar").

        This is intentionally time-bounded to reduce mis-attribution risk. Note: we do NOT persistently
        alias UUID track ids, because Telemost can mix speakers into a single track; instead, we route
        each audio chunk to the current active speaker UI id.
        """
        track_id = (track_id or "").strip()
        if not track_id:
            return track_id
        if not self._is_uuid_like(track_id):
            return track_id

        now = time.time() if now is None else now
        if not self.last_active_speaker_ui_id or self.last_active_speaker_at is None:
            return track_id
        # Tight window: ActiveSpeaker UI updates should be very close to the audio frames.
        if now - self.last_active_speaker_at > 3.0:
            return track_id

        canonical = self.resolve_participant_id(self.last_active_speaker_ui_id)
        if not canonical or canonical == track_id:
            return track_id

        # Don't map to the bot itself.
        canonical_name = (self.participants_info.get(canonical, {}).get("fullName") or "").strip()
        if canonical_name and canonical_name == self.display_name:
            return track_id

        return canonical

    def _active_non_bot_full_names(self) -> list[str]:
        names = []
        for p in self.participants_info.values():
            if not p.get("active"):
                continue
            full_name = (p.get("fullName") or "").strip()
            if not full_name:
                continue
            if full_name == self.display_name:
                continue
            names.append(full_name)
        return sorted(set(names))

    def _maybe_alias_participant_to_canonical(self, participant_id: str, full_name: str) -> str:
        """
        Backwards-compatible "safe" aliasing: only when exactly one active non-bot name exists.

        (We also have a more permissive de-duplication helper that may run for Telemost UI ids.)
        """
        participant_id = (participant_id or "").strip()
        full_name = (full_name or "").strip()
        if not participant_id or not full_name:
            return participant_id

        active_names = self._active_non_bot_full_names()
        if len(active_names) != 1 or active_names[0] != full_name:
            return participant_id

        candidates = []
        for pid, info in self.participants_info.items():
            if pid == participant_id:
                continue
            if (info.get("fullName") or "").strip() != full_name:
                continue
            candidates.append(pid)

        if not candidates:
            return participant_id

        ui_candidates = [c for c in candidates if c.startswith("ui_")]
        canonical = ui_candidates[0] if ui_candidates else candidates[0]
        self.participant_id_aliases[participant_id] = canonical

        # Collapse in-memory entries to reduce duplicates in UI and future lookups.
        self.participants_info.pop(participant_id, None)
        if participant_id in self.track_labels and canonical not in self.track_labels:
            self.track_labels[canonical] = self.track_labels[participant_id]

        return canonical

    def _dedupe_participant_ids_by_full_name(self, full_name: str, *, fallback_canonical_id: str | None = None) -> str | None:
        """
        Platforms can emit multiple internal IDs for the same person.

        When we see a reliable full_name, we can collapse all known IDs with that full_name to a single
        canonical id to avoid showing duplicates in the UI/transcript. To reduce risk of merging distinct
        humans that share a name, we avoid dedupe if there are multiple UI ids for the same name.
        """
        full_name = (full_name or "").strip()
        if not full_name:
            return None

        matching_ids = [pid for pid, info in self.participants_info.items() if (info.get("fullName") or "").strip() == full_name]
        if len(matching_ids) <= 1:
            return matching_ids[0] if matching_ids else fallback_canonical_id

        ui_ids = sorted([pid for pid in matching_ids if pid.startswith("ui_")])
        if len(ui_ids) > 1:
            # Ambiguous: multiple UI tiles share the same name.
            return fallback_canonical_id

        canonical = ui_ids[0] if ui_ids else (fallback_canonical_id or matching_ids[0])
        canonical = self.resolve_participant_id(canonical)

        for pid in list(matching_ids):
            if pid == canonical:
                continue
            # Don't alias if already chained to canonical.
            if self.resolve_participant_id(pid) == canonical:
                continue
            self.participant_id_aliases[pid] = canonical
            self.participants_info.pop(pid, None)
            if pid in self.track_labels and canonical not in self.track_labels:
                self.track_labels[canonical] = self.track_labels[pid]

        return canonical

    def meeting_uuid_mismatch(self, user):
        # If no meeting id was provided, then don't try to detect a mismatch
        if not user.get("meetingId"):
            return False

        # If the meeting uuid is not set, then set it to the user's meeting id
        if not self.meeting_uuid:
            self.meeting_uuid = user.get("meetingId")
            logger.info(f"meeting_uuid set to {self.meeting_uuid} for user {user}")
            return False

        if self.meeting_uuid != user.get("meetingId"):
            logger.info(f"meeting_uuid mismatch detected. meeting_uuid: {self.meeting_uuid} user_meeting_id: {user.get('meetingId')} for user {user}")
            return True

        return False

    def handle_participant_update(self, user):
        if self.meeting_uuid_mismatch(user):
            return

        user_before = self.participants_info.get(user["deviceId"], {"active": False})
        self.participants_info[user["deviceId"]] = user

        if user_before.get("active") and not user["active"]:
            self.add_participant_event_callback({"participant_uuid": user["deviceId"], "event_type": ParticipantEventTypes.LEAVE, "event_data": {}, "timestamp_ms": int(time.time() * 1000)})
            return

        if not user_before.get("active") and user["active"]:
            self.add_participant_event_callback({"participant_uuid": user["deviceId"], "event_type": ParticipantEventTypes.JOIN, "event_data": {}, "timestamp_ms": int(time.time() * 1000)})
            return

        if bool(user_before.get("isHost")) != bool(user.get("isHost")):
            changes = {
                "isHost": {
                    "before": user_before.get("isHost"),
                    "after": user.get("isHost"),
                }
            }
            self.add_participant_event_callback({"participant_uuid": user["deviceId"], "event_type": ParticipantEventTypes.UPDATE, "event_data": changes, "timestamp_ms": int(time.time() * 1000)})
            return

    def process_video_frame(self, message):
        if self.recording_paused:
            return

        self.last_media_message_processed_time = time.time()
        if len(message) > 24:  # Minimum length check
            # Bytes 4-12 contain the timestamp
            timestamp = int.from_bytes(message[4:12], byteorder="little")

            # Get stream ID length and string
            stream_id_length = int.from_bytes(message[12:16], byteorder="little")
            message[16 : 16 + stream_id_length].decode("utf-8")

            # Get width and height after stream ID
            offset = 16 + stream_id_length
            width = int.from_bytes(message[offset : offset + 4], byteorder="little")
            height = int.from_bytes(message[offset + 4 : offset + 8], byteorder="little")

            # Keep track of the video frame dimensions
            if self.video_frame_ticker % 300 == 0:
                logger.info(f"video dimensions {width} {height} message length {len(message) - offset - 8}")
            self.video_frame_ticker += 1

            # Scale frame to video frame size
            expected_video_data_length = width * height + 2 * half_ceil(width) * half_ceil(height)
            video_data = np.frombuffer(message[offset + 8 :], dtype=np.uint8)

            # Check if len(video_data) does not agree with width and height
            if len(video_data) == expected_video_data_length:  # I420 format uses 1.5 bytes per pixel
                scaled_i420_frame = scale_i420(video_data, (width, height), self.video_frame_size)
                if self.wants_any_video_frames_callback() and self.send_frames:
                    self.add_video_frame_callback(scaled_i420_frame, timestamp * 1000)

            else:
                logger.info(f"video data length does not agree with width and height {len(video_data)} {width} {height}")

    def process_mixed_audio_frame(self, message):
        if self.recording_paused:
            return

        self.last_media_message_processed_time = time.time()
        if len(message) > 12:
            # Convert the float32 audio data to numpy array
            audio_data = np.frombuffer(message[4:], dtype=np.float32)

            # Convert float32 to PCM 16-bit by multiplying by 32768.0
            audio_data = (audio_data * 32768.0).astype(np.int16)

            # Only mark last_audio_message_processed_time if the audio data has at least one non-zero value
            if np.any(audio_data):
                self.last_audio_message_processed_time = time.time()

            if (self.wants_any_video_frames_callback is None or self.wants_any_video_frames_callback()) and self.send_frames:
                self.add_mixed_audio_chunk_callback(chunk=audio_data.tobytes())

    def _queue_pending_per_participant_audio(self, track_id, timestamp, audio_bytes):
        if not track_id or audio_bytes is None:
            return
        with self._pending_per_participant_audio_lock:
            self._pending_per_participant_audio[track_id].append((timestamp, audio_bytes))
            if track_id in self._pending_per_participant_audio_timers:
                return
            timer = threading.Timer(0.3, self._flush_pending_per_participant_audio, args=(track_id, None))
            timer.daemon = True
            self._pending_per_participant_audio_timers[track_id] = timer
            timer.start()

    def _flush_pending_per_participant_audio(self, track_id, canonical_id=None):
        if not track_id:
            return
        with self._pending_per_participant_audio_lock:
            timer = self._pending_per_participant_audio_timers.pop(track_id, None)
            if canonical_id is not None and timer:
                try:
                    timer.cancel()
                except Exception:
                    pass
            chunks = list(self._pending_per_participant_audio.pop(track_id, deque()))
        if not chunks:
            return

        participant_id = canonical_id or track_id
        if canonical_id is None and self._is_uuid_like(track_id):
            participant_id = self._maybe_alias_track_id_to_recent_active_speaker(track_id, now=time.time())
            if participant_id == track_id and self.last_active_speaker_ui_id:
                candidate = self.resolve_participant_id(self.last_active_speaker_ui_id)
                if candidate:
                    participant_id = candidate

        participant_id = self.resolve_participant_id(participant_id)
        self.ensure_participant_entry(participant_id)

        for timestamp, audio_bytes in chunks:
            self.add_audio_chunk_callback(participant_id, timestamp, audio_bytes)

    def process_per_participant_audio_frame(self, message):
        if self.recording_paused:
            return

        self.last_media_message_processed_time = time.time()
        if len(message) > 12:
            # Byte 5 contains the participant ID length
            participant_id_length = int.from_bytes(message[4:5], byteorder="little")
            raw_participant_id = message[5 : 5 + participant_id_length].decode("utf-8")

            # Convert the float32 audio data to numpy array
            audio_data = np.frombuffer(message[(5 + participant_id_length) :], dtype=np.float32)

            # Convert float32 to PCM 16-bit by multiplying by 32768.0
            audio_data = (audio_data * 32768.0).astype(np.int16)

            # Only alias when we have a signal; silent frames shouldn't move identity around.
            now = time.time()
            participant_id = raw_participant_id
            if np.any(audio_data):
                participant_id = self._maybe_alias_track_id_to_recent_active_speaker(participant_id, now=now)
            timestamp = datetime.datetime.utcnow()
            audio_bytes = audio_data.tobytes()

            if participant_id == raw_participant_id and self._is_uuid_like(raw_participant_id):
                self._queue_pending_per_participant_audio(raw_participant_id, timestamp, audio_bytes)
                return

            participant_id = self.resolve_participant_id(participant_id)
            self.ensure_participant_entry(participant_id)

            self.add_audio_chunk_callback(participant_id, timestamp, audio_bytes)

    def update_only_one_participant_in_meeting_at(self):
        if not self.joined_at:
            return

        # If nobody other than the bot was ever in the meeting, then don't activate this. We only want to activate if someone else was in the meeting and left
        if len(self.participants_info) <= 1:
            return

        all_participants_in_meeting = [x for x in self.participants_info.values() if x["active"]]
        if len(all_participants_in_meeting) == 1 and all_participants_in_meeting[0]["fullName"] == self.display_name:
            if self.only_one_participant_in_meeting_at is None:
                self.only_one_participant_in_meeting_at = time.time()
                logger.info(f"only_one_participant_in_meeting_at set to {self.only_one_participant_in_meeting_at}")
        else:
            self.only_one_participant_in_meeting_at = None

    def ensure_participant_entry(self, participant_id, label=None):
        if not participant_id:
            return
        participant_id = self.resolve_participant_id(participant_id)
        if participant_id in self.participants_info:
            return

        participant_name = label or self.track_labels.get(participant_id) or participant_id
        self.participants_info[participant_id] = {
            "deviceId": participant_id,
            "fullName": participant_name,
            "isCurrentUser": False,
            "active": True,
        }

    def handle_removed_from_meeting(self):
        if self.meeting_ended_sent:
            return
        self.left_meeting = True
        self.meeting_ended_sent = True
        self.send_message_callback({"message": self.Messages.MEETING_ENDED})

    def handle_meeting_ended(self):
        if self.meeting_ended_sent:
            return
        self.left_meeting = True
        self.meeting_ended_sent = True
        self.send_message_callback({"message": self.Messages.MEETING_ENDED})

    def _finalize_after_auto_leave(self):
        """Ensure state is marked ended and cleanup runs even if driver/UI is unavailable."""
        try:
            self.handle_meeting_ended()
        except Exception as e:
            logger.info(f"Error marking meeting ended during auto-leave: {e}")
        try:
            self.cleanup()
        except Exception as e:
            logger.info(f"Error during cleanup in auto-leave: {e}")

    def handle_failed_to_join(self, reason):
        logger.info(f"failed to join meeting with reason {reason}")
        self.subclass_specific_handle_failed_to_join(reason)

    def handle_caption_update(self, json_data):
        if self.recording_paused:
            return

        # Count a caption as audio activity
        self.last_audio_message_processed_time = time.time()
        self.upsert_caption_callback(json_data["caption"])

    def handle_chat_message(self, json_data):
        if self.recording_paused and not self.record_chat_messages_when_paused:
            return

        self.upsert_chat_message_callback(json_data)

    def mask_transcript_if_required(self, json_data):
        if not settings.MASK_TRANSCRIPT_IN_LOGS:
            return json_data

        json_data_masked = copy.deepcopy(json_data)
        if json_data.get("caption") and json_data.get("caption").get("text"):
            json_data_masked["caption"]["text"] = hashlib.sha256(json_data.get("caption").get("text").encode("utf-8")).hexdigest()
        return json_data_masked

    def handle_websocket(self, websocket):
        audio_format = None

        try:
            logger.info("Websocket client connected")
            self.websocket_client_connected = True
            self.websocket_client_connected_at = self.websocket_client_connected_at or time.time()
            self.websocket_client_disconnected_at = None
            for message in websocket:
                # Get first 4 bytes as message type
                message_type = int.from_bytes(message[:4], byteorder="little")

                if message_type == 1:  # JSON
                    json_data = json.loads(message[4:].decode("utf-8"))
                    if json_data.get("type") == "CaptionUpdate":
                        logger.info("Received JSON message: %s", self.mask_transcript_if_required(json_data))
                    else:
                        logger.info("Received JSON message: %s", json_data)

                    # Handle audio format information
                    if isinstance(json_data, dict):
                        if json_data.get("type") == "AudioFormatUpdate":
                            audio_format = json_data["format"]
                            logger.info(f"audio format {audio_format}")

                        elif json_data.get("type") == "CaptionUpdate":
                            self.handle_caption_update(json_data)

                        elif json_data.get("type") == "ChatMessage":
                            self.handle_chat_message(json_data)

                        elif json_data.get("type") == "UsersUpdate":
                            for user in json_data["newUsers"]:
                                user["active"] = user["humanized_status"] == "in_meeting"
                                self.handle_participant_update(user)
                            for user in json_data["removedUsers"]:
                                user["active"] = False
                                self.handle_participant_update(user)
                            for user in json_data["updatedUsers"]:
                                user["active"] = user["humanized_status"] == "in_meeting"
                                self.handle_participant_update(user)

                                if user["humanized_status"] == "removed_from_meeting" and user["fullName"] == self.display_name:
                                    # if this is the only participant with that name in the meeting, then we can assume that it was us who was removed
                                    if len([x for x in self.participants_info.values() if x["fullName"] == self.display_name]) == 1:
                                        self.handle_removed_from_meeting()

                            self.update_only_one_participant_in_meeting_at()

                        elif json_data.get("type") == "SilenceStatus":
                            if not json_data.get("isSilent"):
                                self.last_audio_message_processed_time = time.time()

                        elif json_data.get("type") == "ChatStatusChange":
                            if json_data.get("change") == "ready_to_send":
                                self.ready_to_send_chat_messages = True
                                self.send_message_callback({"message": self.Messages.READY_TO_SEND_CHAT_MESSAGE})

                        elif json_data.get("type") == "MeetingStatusChange":
                            if json_data.get("change") == "removed_from_meeting":
                                self.handle_removed_from_meeting()
                            if json_data.get("change") == "meeting_ended":
                                self.handle_meeting_ended()
                            if json_data.get("change") == "failed_to_join":
                                self.handle_failed_to_join(json_data.get("reason"))

                        elif json_data.get("type") == "RecordingPermissionChange":
                            if json_data.get("change") == "granted":
                                self.after_bot_can_record_meeting()
                            elif json_data.get("change") == "denied":
                                self.after_bot_recording_permission_denied()

                        elif json_data.get("type") == "AudioTrackSeen":
                            track_id = json_data.get("trackId")
                            label = json_data.get("label")
                            if track_id:
                                self.track_labels[track_id] = label or track_id

                        elif json_data.get("type") == "ParticipantNameUpdate":
                            track_id = json_data.get("trackId")
                            full_name = (json_data.get("fullName") or "").strip()
                            if track_id and full_name:
                                # When there's exactly one non-bot name in the meeting, some platforms can
                                # emit multiple internal ids for that same person. Alias them to avoid
                                # showing multiple copies of the same speaker.
                                canonical_id = self._maybe_alias_participant_to_canonical(track_id, full_name)
                                canonical_id = self.resolve_participant_id(canonical_id)

                                # Additionally, if we have a UI-based id for this name (Telemost),
                                # prefer it as canonical and dedupe any other ids with the same full_name.
                                canonical_id = self._dedupe_participant_ids_by_full_name(full_name, fallback_canonical_id=canonical_id) or canonical_id

                                # Update in-memory participant label so subsequent utterances use a friendly name.
                                before = None
                                if canonical_id in self.participants_info:
                                    before = self.participants_info[canonical_id].get("fullName")
                                    self.participants_info[canonical_id]["fullName"] = full_name
                                self.track_labels[canonical_id] = full_name
                                self.ensure_participant_entry(canonical_id, full_name)
                                self._flush_pending_per_participant_audio(track_id, canonical_id)

                                # Propagate to DB via a participant UPDATE event so existing utterances update too.
                                if self.add_participant_event_callback:
                                    try:
                                        self.add_participant_event_callback(
                                            {
                                                "participant_uuid": canonical_id,
                                                "event_type": ParticipantEventTypes.UPDATE,
                                                "event_data": {"fullName": {"before": before, "after": full_name}},
                                                "timestamp_ms": int(time.time() * 1000),
                                            }
                                        )
                                    except Exception as e:
                                        logger.info(f"Error emitting ParticipantNameUpdate as ParticipantEvent: {e}")

                        elif json_data.get("type") == "Diag" and json_data.get("message") == "ActiveSpeaker":
                            ui_id = (json_data.get("uiId") or "").strip()
                            if ui_id:
                                self.last_active_speaker_ui_id = ui_id
                                self.last_active_speaker_at = time.time()

                elif message_type == 2:  # VIDEO
                    self.process_video_frame(message)
                elif message_type == 3:  # AUDIO
                    self.process_mixed_audio_frame(message)
                elif message_type == 4:  # ENCODED_MP4_CHUNK
                    self.process_encoded_mp4_chunk(message)
                elif message_type == 5:  # PER_PARTICIPANT_AUDIO
                    self.process_per_participant_audio_frame(message)

                self.last_websocket_message_processed_time = time.time()
        except Exception as e:
            logger.info(f"Websocket error: {e}")
            raise e
        finally:
            # If the page (or Chrome) crashes, the websocket client will disconnect.
            # We don't want to keep the bot in JOINED_RECORDING forever in that case.
            self.websocket_client_connected = False
            self.websocket_client_disconnected_at = time.time()

    def run_websocket_server(self):
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)

        port = self.get_websocket_port()
        max_retries = 10

        for attempt in range(max_retries):
            try:
                self.websocket_server = serve(
                    self.handle_websocket,
                    "localhost",
                    port,
                    compression=None,
                    max_size=None,
                )
                logger.info(f"Websocket server started on ws://localhost:{port}")
                self.websocket_port = port
                self.websocket_server.serve_forever()
                break
            except OSError as e:
                if e.errno == 98:  # Address already in use
                    logger.info(f"Port {port} is already in use, trying next port...")
                    port += 1
                    if attempt == max_retries - 1:
                        raise Exception(f"Could not find available port after {max_retries} attempts")
                    continue
                raise  # Re-raise other OSErrors

    def send_request_to_join_denied_message(self):
        self.send_message_callback({"message": self.Messages.REQUEST_TO_JOIN_DENIED})

    def send_meeting_not_found_message(self):
        self.send_message_callback({"message": self.Messages.MEETING_NOT_FOUND})

    def send_login_required_message(self):
        self.send_message_callback({"message": self.Messages.LOGIN_REQUIRED})

    def capture_screenshot_and_mhtml_file(self):
        # Take a screenshot and mhtml file of the page, because it is helpful to have for debugging
        current_time = datetime.datetime.now()
        timestamp = current_time.strftime("%Y%m%d_%H%M%S")
        screenshot_path = f"/tmp/ui_element_not_found_{timestamp}.png"
        try:
            self.driver.save_screenshot(screenshot_path)
        except Exception as e:
            logger.info(f"Error saving screenshot: {e}")
            screenshot_path = None

        mhtml_file_path = f"/tmp/page_snapshot_{timestamp}.mhtml"
        try:
            result = self.driver.execute_cdp_cmd("Page.captureSnapshot", {})
            mhtml_bytes = result["data"]  # Extract the data from the response dictionary
            with open(mhtml_file_path, "w", encoding="utf-8") as f:
                f.write(mhtml_bytes)
        except Exception as e:
            logger.info(f"Error saving mhtml: {e}")
            mhtml_file_path = None

        return screenshot_path, mhtml_file_path, current_time

    def send_login_attempt_failed_message(self):
        screenshot_path, mhtml_file_path, current_time = self.capture_screenshot_and_mhtml_file()

        self.send_message_callback(
            {
                "message": self.Messages.LOGIN_ATTEMPT_FAILED,
                "mhtml_file_path": mhtml_file_path,
                "screenshot_path": screenshot_path,
            }
        )

    def send_incorrect_password_message(self):
        self.send_message_callback({"message": self.Messages.COULD_NOT_CONNECT_TO_MEETING})

    def send_blocked_by_platform_captcha_message(self):
        screenshot_path, mhtml_file_path, current_time = self.capture_screenshot_and_mhtml_file()
        captcha_url = getattr(self, "captcha_url", None)
        self.send_message_callback(
            {
                "message": self.Messages.CAPTCHA_REQUIRED,
                "reason": "captcha",
                "current_url": getattr(self.driver, "current_url", None),
                "current_time": current_time.isoformat(),
                "mhtml_file_path": mhtml_file_path,
                "screenshot_path": screenshot_path,
                "captcha_url": captcha_url,
            }
        )

    def send_debug_screenshot_message(self, step, exception, inner_exception):
        current_time = datetime.datetime.now()
        timestamp = current_time.strftime("%Y%m%d_%H%M%S")
        screenshot_path = f"/tmp/ui_element_not_found_{timestamp}.png"
        try:
            self.driver.save_screenshot(screenshot_path)
        except Exception as e:
            logger.info(f"Error saving screenshot: {e}")
            screenshot_path = None

        mhtml_file_path = f"/tmp/page_snapshot_{timestamp}.mhtml"
        try:
            result = self.driver.execute_cdp_cmd("Page.captureSnapshot", {})
            mhtml_bytes = result["data"]  # Extract the data from the response dictionary
            with open(mhtml_file_path, "w", encoding="utf-8") as f:
                f.write(mhtml_bytes)
        except Exception as e:
            logger.info(f"Error saving mhtml: {e}")
            mhtml_file_path = None

        self.send_message_callback(
            {
                "message": self.Messages.UI_ELEMENT_NOT_FOUND,
                "step": step,
                "current_time": current_time,
                "mhtml_file_path": mhtml_file_path,
                "screenshot_path": screenshot_path,
                "exception_type": exception.__class__.__name__ if exception else "exception_not_available",
                "exception_message": exception.__str__() if exception else "exception_message_not_available",
                "inner_exception_type": inner_exception.__class__.__name__ if inner_exception else "inner_exception_not_available",
                "inner_exception_message": inner_exception.__str__() if inner_exception else "inner_exception_message_not_available",
            }
        )

    def add_subclass_specific_chrome_options(self, options):
        pass

    def init_driver(self):
        options = webdriver.ChromeOptions()

        options.add_argument("--autoplay-policy=no-user-gesture-required")
        options.add_argument("--use-fake-device-for-media-stream")
        options.add_argument("--use-fake-ui-for-media-stream")
        options.add_argument(f"--window-size={self.video_frame_size[0]},{self.video_frame_size[1]}")
        options.add_argument("--start-fullscreen")
        # options.add_argument('--headless=new')
        options.add_argument("--disable-gpu")
        options.add_argument("--disable-extensions")
        options.add_argument("--disable-application-cache")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--disable-blink-features=AutomationControlled")
        options.add_experimental_option("excludeSwitches", ["enable-automation"])
        options.add_argument("--allow-insecure-localhost")
        options.add_argument("--allow-running-insecure-content")
        options.add_argument("--ignore-certificate-errors")

        # Sandbox disabled to match tested working setup
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-setuid-sandbox")

        prefs = {
            "credentials_enable_service": False,
            "profile.password_manager_enabled": False,
        }
        options.add_experimental_option("prefs", prefs)

        proxy_server = os.getenv("CHROME_PROXY_SERVER")
        if proxy_server:
            # Example: http://user:pass@host:port, socks5://host:port, http://host:port
            options.add_argument(f"--proxy-server={proxy_server}")
            proxy_bypass_list = os.getenv("CHROME_PROXY_BYPASS_LIST")
            if proxy_bypass_list:
                options.add_argument(f"--proxy-bypass-list={proxy_bypass_list}")
            logger.info("Using Chrome proxy server from CHROME_PROXY_SERVER")

        self.add_subclass_specific_chrome_options(options)

        if self.driver:
            # Simulate closing browser window
            try:
                self.driver.close()
            except Exception as e:
                logger.info(f"Error closing driver: {e}")

            try:
                self.driver.quit()
            except Exception as e:
                logger.info(f"Error closing existing driver: {e}")
            self.driver = None

        # Use a unique Chrome profile directory per bot session.
        # Without this, concurrent web bots can collide on the default profile and Chrome fails with:
        # "session not created: probably user data directory is already in use"
        try:
            if getattr(self, "chrome_user_data_dir", None):
                if not os.getenv("KEEP_CHROME_PROFILE_DIR"):
                    shutil.rmtree(self.chrome_user_data_dir, ignore_errors=True)
        except Exception as e:
            logger.info(f"Error cleaning previous chrome_user_data_dir: {e}")

        profile_template_dir = os.getenv("CHROME_PROFILE_TEMPLATE_DIR")
        if profile_template_dir and os.path.isdir(profile_template_dir):
            # Create a fresh per-bot dir but seeded from an existing template (useful to reuse cookies to reduce captcha frequency).
            tmp_dir = tempfile.mkdtemp(prefix="attendee-chrome-profile-")
            try:
                shutil.rmtree(tmp_dir, ignore_errors=True)
                shutil.copytree(profile_template_dir, tmp_dir)
            except Exception as e:
                logger.info(f"Error copying CHROME_PROFILE_TEMPLATE_DIR to user-data-dir: {e}")
                tmp_dir = tempfile.mkdtemp(prefix="attendee-chrome-profile-")
            self.chrome_user_data_dir = tmp_dir
        else:
            self.chrome_user_data_dir = tempfile.mkdtemp(prefix="attendee-chrome-profile-")

        options.add_argument(f"--user-data-dir={self.chrome_user_data_dir}")

        chromedriver_path = ensure_chromedriver()
        self.driver = webdriver.Chrome(options=options, service=Service(executable_path=chromedriver_path))
        logger.info(f"web driver server initialized at port {self.driver.service.port}")

        # Telemost (and some other platforms) can ship strict CSP (connect-src) that blocks
        # WebSocket connections to our local ws://localhost:<port> bridge. Bypass CSP via CDP.
        try:
            self.driver.execute_cdp_cmd("Page.setBypassCSP", {"enabled": True})
        except Exception as e:
            logger.info(f"Could not enable CSP bypass via CDP: {e}")

        initial_data_code = f"""window.__attendeeInitialData = {{
            websocketPort: {self.websocket_port},
            videoFrameWidth: {self.video_frame_size[0]},
            videoFrameHeight: {self.video_frame_size[1]},
            botName: {json.dumps(self.display_name)},
            addClickRipple: {'true' if self.should_create_debug_recording else 'false'},
            recordingView: '{self.recording_view}',
            sendMixedAudio: {'true' if self.add_mixed_audio_chunk_callback else 'false'},
            sendPerParticipantAudio: {'true' if self.add_audio_chunk_callback else 'false'},
            collectCaptions: {'false' if self.add_audio_chunk_callback else 'true'}
        }};
        window.initialData = window.__attendeeInitialData;"""

        # Define the CDN libraries needed
        CDN_LIBRARIES = ["https://cdnjs.cloudflare.com/ajax/libs/protobufjs/7.4.0/protobuf.min.js", "https://cdnjs.cloudflare.com/ajax/libs/pako/2.1.0/pako.min.js"]

        # Download all library code
        libraries_code = ""
        for url in CDN_LIBRARIES:
            response = requests.get(url)
            if response.status_code == 200:
                libraries_code += response.text + "\n"
            else:
                raise Exception(f"Failed to download library from {url}")

        # Get directory of current file
        current_dir = os.path.dirname(os.path.abspath(__file__))
        # Read subclass-specific payload using path relative to current file
        with open(os.path.join(current_dir, "..", self.get_chromedriver_payload_file_name()), "r") as file:
            payload_code = file.read()

        # Combine them ensuring libraries load first
        combined_code = f"""
            {initial_data_code}
            {self.subclass_specific_initial_data_code()}
            {libraries_code}
            {payload_code}
        """

        # Add the combined script to execute on new document
        self.driver.execute_cdp_cmd("Page.addScriptToEvaluateOnNewDocument", {"source": combined_code})

    def init(self):
        self.display_var_for_debug_recording = os.environ.get("DISPLAY")
        if os.environ.get("DISPLAY") is None:
            # Create virtual display only if no real display is available
            self.display = Display(visible=0, size=(1930, 1090))
            self.display.start()
            self.display_var_for_debug_recording = self.display.new_display_var

        self.vnc_process = None
        self.captcha_url = None
        self.captcha_token = None

        if self.should_create_debug_recording:
            self.debug_screen_recorder = DebugScreenRecorder(self.display_var_for_debug_recording, self.video_frame_size, BotAdapter.DEBUG_RECORDING_FILE_PATH)
            self.debug_screen_recorder.start()

        # Start websocket server in a separate thread
        websocket_thread = threading.Thread(target=self.run_websocket_server, daemon=True)
        websocket_thread.start()

        sleep(0.5)  # Give the websocketserver time to start
        if not self.websocket_port:
            raise Exception("WebSocket server failed to start")

        repeatedly_attempt_to_join_meeting_thread = threading.Thread(target=self.repeatedly_attempt_to_join_meeting, daemon=True)
        repeatedly_attempt_to_join_meeting_thread.start()

    def _find_free_port(self, start: int = 5900, end: int = 5999) -> int:
        for port in range(start, end + 1):
            try:
                with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                    s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                    s.bind(("0.0.0.0", port))
                    return port
            except Exception:
                continue
        raise RuntimeError("No free port available for x11vnc")

    def _ensure_captcha_session(self) -> str:
        """
        Start x11vnc for this bot's display and register a short-lived token in Redis.
        Returns a public URL to open the captcha page.
        """
        if self.captcha_url:
            return self.captcha_url

        if not getattr(self, "display_var_for_debug_recording", None):
            raise RuntimeError("DISPLAY not set for captcha session")

        token = secrets.token_urlsafe(24)
        vnc_port = self._find_free_port()

        # Start VNC bound on the container network (NOT exposed publicly).
        cmd = [
            "x11vnc",
            "-display",
            str(self.display_var_for_debug_recording),
            "-rfbport",
            str(vnc_port),
            "-forever",
            "-shared",
            "-nopw",
        ]
        self.vnc_process = subprocess.Popen(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        logger.info(f"Started x11vnc for captcha on {self.display_var_for_debug_recording} port {vnc_port}")

        # Store token -> connection info in Redis with TTL.
        redis_url = os.getenv("REDIS_URL", "redis://redis:6379/5")
        try:
            import redis

            r = redis.Redis.from_url(redis_url)
            key = f"attendee:captcha:{token}"
            value = json.dumps(
                {
                    "bot_object_id": getattr(self, "bot_object_id", None),
                    "vnc_host": "attendee-worker",
                    "vnc_port": vnc_port,
                    "created_at": time.time(),
                }
            )
            r.setex(key, int(os.getenv("CAPTCHA_TOKEN_TTL_SECONDS", "600")), value)
        except Exception as e:
            logger.info(f"Could not store captcha token in Redis: {e}")

        site_domain = os.getenv("SITE_DOMAIN", "dev.salmetov.fun")
        self.captcha_token = token
        self.captcha_url = f"https://{site_domain}/captcha/{token}"
        return self.captcha_url

    def _stop_vnc_server(self) -> None:
        try:
            proc = getattr(self, "vnc_process", None)
            if not proc:
                return
            if proc.poll() is None:
                proc.terminate()
                try:
                    proc.wait(timeout=5)
                except Exception:
                    proc.kill()
            self.vnc_process = None
        except Exception as e:
            logger.info(f"Error stopping x11vnc: {e}")

    def _delete_captcha_token(self) -> None:
        token = getattr(self, "captcha_token", None)
        if not token:
            return
        redis_url = os.getenv("REDIS_URL", "redis://redis:6379/5")
        try:
            import redis

            r = redis.Redis.from_url(redis_url)
            r.delete(f"attendee:captcha:{token}")
        except Exception as e:
            logger.info(f"Could not delete captcha token from Redis: {e}")
        finally:
            self.captcha_token = None
            self.captcha_url = None

    def wait_for_captcha_to_clear(self, timeout_seconds: int) -> bool:
        """
        Wait for the platform captcha page to be cleared by a human operator.
        Returns True if captcha disappeared, False on timeout/abort.
        """
        try:
            deadline = time.time() + max(1, int(timeout_seconds))
            while time.time() < deadline and not self.left_meeting and not self.cleaned_up:
                try:
                    url = getattr(self.driver, "current_url", "") or ""
                except Exception:
                    url = ""
                if not ("showcaptcha" in url or ("captcha" in url and "yandex" in url)):
                    try:
                        self._stop_vnc_server()
                        self._delete_captcha_token()
                    except Exception:
                        pass
                    return True
                sleep(1)
        except Exception:
            return False
        return False

    def should_retry_joining_meeting_that_requires_login_by_logging_in(self):
        return False

    def repeatedly_attempt_to_join_meeting(self):
        logger.info(f"Trying to join meeting at {self.meeting_url}")

        # Expected exceptions are ones that we expect to happen and are not a big deal, so we only increment num_retries once every three expected exceptions
        num_expected_exceptions = 0
        num_retries = 0
        max_retries = 3
        while num_retries <= max_retries:
            try:
                self.init_driver()
                self.attempt_to_join_meeting()
                logger.info("Successfully joined meeting")
                break

            except UiLoginRequiredException:
                if not self.should_retry_joining_meeting_that_requires_login_by_logging_in():
                    self.send_login_required_message()
                    return

            except UiLoginAttemptFailedException:
                self.send_login_attempt_failed_message()
                return

            except UiRequestToJoinDeniedException:
                self.send_request_to_join_denied_message()
                return

            except UiCouldNotJoinMeetingWaitingRoomTimeoutException:
                self.send_message_callback({"message": self.Messages.LEAVE_MEETING_WAITING_ROOM_TIMEOUT_EXCEEDED})
                return

            except UiCouldNotJoinMeetingWaitingForHostException:
                self.send_message_callback({"message": self.Messages.LEAVE_MEETING_WAITING_FOR_HOST})
                return

            except UiMeetingNotFoundException:
                self.send_meeting_not_found_message()
                return

            except UiIncorrectPasswordException:
                self.send_incorrect_password_message()
                return

            except UiBlockedByCaptchaException:
                logger.info("Blocked by platform captcha during join attempt")
                try:
                    self._ensure_captcha_session()
                except Exception as e:
                    logger.info(f"Could not start captcha session: {e}")
                self.send_blocked_by_platform_captcha_message()

                wait_seconds = int(os.getenv("CAPTCHA_WAIT_SECONDS", os.getenv("WAIT_FOR_CAPTCHA_SOLVE_SECONDS", "600")))
                logger.info(f"Waiting up to {wait_seconds}s for captcha to be solved via {self.captcha_url}")
                if self.wait_for_captcha_to_clear(wait_seconds):
                    logger.info("Captcha cleared; retrying join flow without restarting the driver")
                    try:
                        self.attempt_to_join_meeting()
                        logger.info("Successfully joined meeting after captcha solve")
                        break
                    except Exception as e:
                        logger.info(f"Join attempt after captcha solve failed: {e}")
                        return
                else:
                    logger.info("Captcha was not cleared before timeout")
                    return

            except UiRetryableExpectedException as e:
                if num_retries >= max_retries:
                    logger.info(f"Failed to join meeting and the {e.__class__.__name__} exception is retryable but the number of retries exceeded the limit and there were {num_expected_exceptions} expected exceptions, so returning")
                    self.send_debug_screenshot_message(step=e.step, exception=e, inner_exception=e.inner_exception)
                    return

                num_expected_exceptions += 1
                if num_expected_exceptions % 5 == 0:
                    num_retries += 1
                    logger.info(f"Failed to join meeting and the {e.__class__.__name__} exception is expected and {num_expected_exceptions} expected exceptions have occurred, so incrementing num_retries. This usually indicates that the meeting has not started yet, so we will wait for the configured amount of time which is 180 seconds before retrying")
                    # We're going to start a new pod to see if that fixes the issue
                    self.send_message_callback({"message": self.Messages.BLOCKED_BY_PLATFORM_REPEATEDLY})
                    return
                else:
                    logger.info(f"Failed to join meeting and the {e.__class__.__name__} exception is expected so not incrementing num_retries, but {num_expected_exceptions} expected exceptions have occurred")

            except UiRetryableException as e:
                if num_retries >= max_retries:
                    logger.info(f"Failed to join meeting and the {e.__class__.__name__} exception is retryable but the number of retries exceeded the limit, so returning")
                    self.send_debug_screenshot_message(step=e.step, exception=e, inner_exception=e.inner_exception)
                    return

                if self.left_meeting or self.cleaned_up:
                    logger.info(f"Failed to join meeting and the {e.__class__.__name__} exception is retryable but the bot has left the meeting or cleaned up, so returning")
                    return

                logger.info(f"Failed to join meeting and the {e.__class__.__name__} exception is retryable so retrying")

                num_retries += 1

            except Exception as e:
                if num_retries >= max_retries:
                    logger.exception(f"Failed to join meeting and the unexpected {e.__class__.__name__} exception with message {e.__str__()} is retryable but the number of retries exceeded the limit, so returning.")
                    self.send_debug_screenshot_message(step="unknown", exception=e, inner_exception=None)
                    return

                if self.left_meeting or self.cleaned_up:
                    logger.exception(f"Failed to join meeting and the unexpected {e.__class__.__name__} exception with message {e.__str__()} is retryable but the bot has left the meeting or cleaned up, so returning.")
                    return

                logger.exception(f"Failed to join meeting and the unexpected {e.__class__.__name__} exception with message {e.__str__()} is retryable so retrying")

                num_retries += 1

            sleep(1)

        self.after_bot_joined_meeting()
        self.subclass_specific_after_bot_joined_meeting()

    def after_bot_joined_meeting(self):
        self.send_message_callback({"message": self.Messages.BOT_JOINED_MEETING})
        self.joined_at = time.time()
        # If we never get participant updates, treat ourselves as the only participant after join
        if not self.participants_info:
            self.only_one_participant_in_meeting_at = self.only_one_participant_in_meeting_at or self.joined_at
        self.update_only_one_participant_in_meeting_at()

    def after_bot_recording_permission_denied(self):
        self.send_message_callback({"message": self.Messages.BOT_RECORDING_PERMISSION_DENIED, "denied_reason": BotAdapter.BOT_RECORDING_PERMISSION_DENIED_REASON.HOST_DENIED_PERMISSION})

    def after_bot_can_record_meeting(self):
        if self.recording_permission_granted_at is not None:
            return

        self.recording_permission_granted_at = time.time()
        self.send_message_callback({"message": self.Messages.BOT_RECORDING_PERMISSION_GRANTED})
        self.send_frames = True
        self.driver.execute_script("window.ws?.enableMediaSending();")
        self.first_buffer_timestamp_ms_offset = self.driver.execute_script("return performance.timeOrigin;")

        if self.start_recording_screen_callback:
            sleep(2)
            if self.debug_screen_recorder:
                self.debug_screen_recorder.stop()
            self.start_recording_screen_callback(self.display_var_for_debug_recording)

        self.media_sending_enable_timestamp_ms = time.time() * 1000

    def leave(self):
        if self.left_meeting:
            return
        if self.was_removed_from_meeting:
            return
        if self.stop_recording_screen_callback:
            self.stop_recording_screen_callback()

        try:
            logger.info("disable media sending")
            self.driver.execute_script("window.ws?.disableMediaSending();")

            self.click_leave_button()
        except Exception as e:
            logger.info(f"Error during leave: {e}")
        finally:
            self.send_message_callback({"message": self.Messages.MEETING_ENDED})
            self.left_meeting = True

    def abort_join_attempt(self):
        try:
            self.driver.close()
        except Exception as e:
            logger.info(f"Error closing driver: {e}")

    def cleanup(self):
        if self.stop_recording_screen_callback:
            self.stop_recording_screen_callback()

        try:
            logger.info("disable media sending")
            self.driver.execute_script("window.ws?.disableMediaSending();")
        except Exception as e:
            logger.info(f"Error during media sending disable: {e}")

        # Wait for websocket buffers to be processed
        if self.last_websocket_message_processed_time:
            time_when_shutdown_initiated = time.time()
            while time.time() - self.last_websocket_message_processed_time < 2 and time.time() - time_when_shutdown_initiated < 30:
                logger.info(f"Waiting until it's 2 seconds since last websockets message was processed or 30 seconds have passed. Currently it is {time.time() - self.last_websocket_message_processed_time} seconds and {time.time() - time_when_shutdown_initiated} seconds have passed")
                sleep(0.5)

        try:
            if self.driver:
                # Simulate closing browser window
                try:
                    self.subclass_specific_before_driver_close()
                    self.driver.close()
                except Exception as e:
                    logger.info(f"Error closing driver: {e}")

                # Then quit the driver
                try:
                    self.driver.quit()
                except Exception as e:
                    logger.info(f"Error quitting driver: {e}")
        except Exception as e:
            logger.info(f"Error during cleanup: {e}")

        if self.debug_screen_recorder:
            self.debug_screen_recorder.stop()

        # Properly shutdown the websocket server
        if self.websocket_server:
            try:
                self.websocket_server.shutdown()
            except Exception as e:
                logger.info(f"Error shutting down websocket server: {e}")

        # Stop VNC server (if any)
        try:
            self._stop_vnc_server()
            self._delete_captcha_token()
        except Exception:
            pass

        self.cleaned_up = True

        # Clean up Chrome profile directory
        try:
            if getattr(self, "chrome_user_data_dir", None):
                if os.getenv("KEEP_CHROME_PROFILE_DIR"):
                    logger.info(f"KEEP_CHROME_PROFILE_DIR is set; keeping chrome_user_data_dir at {self.chrome_user_data_dir}")
                else:
                    shutil.rmtree(self.chrome_user_data_dir, ignore_errors=True)
                self.chrome_user_data_dir = None
        except Exception as e:
            logger.info(f"Error cleaning chrome_user_data_dir during cleanup: {e}")

    def check_auto_leave_conditions(self) -> None:
        if self.left_meeting:
            return
        if self.cleaned_up:
            return

        # If the injected websocket client disconnects after we started sending frames, assume the browser/page died.
        # End the meeting so the bot transitions to post-processing instead of staying stuck in "recording".
        ws_disconnect_timeout_seconds = int(os.getenv("WEBSOCKET_DISCONNECT_AUTO_END_SECONDS", "15"))
        if (
            self.send_frames
            and self.joined_at is not None
            and self.websocket_client_disconnected_at is not None
            and not self.websocket_client_connected
            and not self.meeting_ended_sent
            and time.time() - self.websocket_client_disconnected_at > ws_disconnect_timeout_seconds
        ):
            logger.info(
                "Auto-ending meeting because websocket client disconnected "
                f"for >{ws_disconnect_timeout_seconds}s (likely browser/page crashed)"
            )
            self.handle_meeting_ended()
            return

        if not getattr(self, "disable_only_one_auto_leave", False):
            if self.only_one_participant_in_meeting_at is not None:
                if time.time() - self.only_one_participant_in_meeting_at > self.automatic_leave_configuration.only_participant_in_meeting_timeout_seconds:
                    logger.info(f"Auto-leaving meeting because there was only one participant in the meeting for {self.automatic_leave_configuration.only_participant_in_meeting_timeout_seconds} seconds")
                    self.send_message_callback({"message": self.Messages.ADAPTER_REQUESTED_BOT_LEAVE_MEETING, "leave_reason": BotAdapter.LEAVE_REASON.AUTO_LEAVE_ONLY_PARTICIPANT_IN_MEETING})
                    self._finalize_after_auto_leave()
                    return

        if not self.silence_detection_activated and self.joined_at is not None and time.time() - self.joined_at > self.automatic_leave_configuration.silence_activate_after_seconds:
            self.silence_detection_activated = True
            self.last_audio_message_processed_time = time.time()
            logger.info(f"Silence detection activated after {self.automatic_leave_configuration.silence_activate_after_seconds} seconds")

        if self.last_audio_message_processed_time is not None and self.silence_detection_activated:
            if time.time() - self.last_audio_message_processed_time > self.automatic_leave_configuration.silence_timeout_seconds:
                logger.info(f"Auto-leaving meeting because there was no audio for {self.automatic_leave_configuration.silence_timeout_seconds} seconds")
                self.send_message_callback({"message": self.Messages.ADAPTER_REQUESTED_BOT_LEAVE_MEETING, "leave_reason": BotAdapter.LEAVE_REASON.AUTO_LEAVE_SILENCE})
                self._finalize_after_auto_leave()
                return

        if self.joined_at is not None and self.automatic_leave_configuration.max_uptime_seconds is not None:
            if time.time() - self.joined_at > self.automatic_leave_configuration.max_uptime_seconds:
                logger.info(f"Auto-leaving meeting because bot has been running for more than {self.automatic_leave_configuration.max_uptime_seconds} seconds")
                self.send_message_callback({"message": self.Messages.ADAPTER_REQUESTED_BOT_LEAVE_MEETING, "leave_reason": BotAdapter.LEAVE_REASON.AUTO_LEAVE_MAX_UPTIME})
                self._finalize_after_auto_leave()
                return

    def is_ready_to_send_chat_messages(self):
        return self.ready_to_send_chat_messages

    def webpage_streamer_get_peer_connection_offer(self):
        return self.driver.execute_script("return window.botOutputManager.getBotOutputPeerConnectionOffer();")

    def webpage_streamer_start_peer_connection(self, offer_response):
        self.driver.execute_script(f"window.botOutputManager.startBotOutputPeerConnection({json.dumps(offer_response)});")

    def webpage_streamer_play_bot_output_media_stream(self, output_destination):
        self.driver.execute_script(f"window.botOutputManager.playBotOutputMediaStream({json.dumps(output_destination)});")

    def webpage_streamer_stop_bot_output_media_stream(self):
        self.driver.execute_script("window.botOutputManager.stopBotOutputMediaStream();")

    def is_bot_ready_for_webpage_streamer(self):
        if not self.driver:
            return False
        return self.driver.execute_script("return window.botOutputManager?.isReadyForWebpageStreamer();")

    def ready_to_show_bot_image(self):
        self.send_message_callback({"message": self.Messages.READY_TO_SHOW_BOT_IMAGE})

    def get_first_buffer_timestamp_ms(self):
        if self.media_sending_enable_timestamp_ms is None:
            return None
        # Doing a manual offset for now to correct for the screen recorder delay. This seems to work reliably.
        return self.media_sending_enable_timestamp_ms

    def send_raw_image(self, image_bytes):
        # If we have a memoryview, convert it to bytes
        if isinstance(image_bytes, memoryview):
            image_bytes = image_bytes.tobytes()

        # Pass the raw bytes directly to JavaScript
        # The JavaScript side can convert it to appropriate format
        self.driver.execute_script(
            """
            const bytes = new Uint8Array(arguments[0]);
            window.botOutputManager.displayImage(bytes);
        """,
            list(image_bytes),
        )

    def send_raw_audio(self, bytes, sample_rate):
        """
        Sends raw audio bytes to the Google Meet call.

        :param bytes: Raw audio bytes in PCM format
        :param sample_rate: Sample rate of the audio in Hz
        """
        if not self.driver:
            print("Cannot send audio - driver not initialized")
            return

        # Convert bytes to Int16Array for JavaScript
        audio_data = np.frombuffer(bytes, dtype=np.int16).tolist()

        # Call the JavaScript function to enqueue the PCM chunk
        self.driver.execute_script(f"window.botOutputManager.playPCMAudio({audio_data}, {sample_rate})")

    def send_chat_message(self, text, to_user_uuid):
        logger.info("send_chat_message not supported in web bots")

    # Sub-classes can override this to add class-specific initial data code
    def subclass_specific_initial_data_code(self):
        return ""

    # Sub-classes can override this to add class-specific after bot joined meeting code
    def subclass_specific_after_bot_joined_meeting(self):
        pass

    # Sub-classes can override this to handle class-specific failed to join issues
    def subclass_specific_handle_failed_to_join(self, reason):
        pass

    # Sub-classes can override this to add class-specific before driver close code
    def subclass_specific_before_driver_close(self):
        pass
