import json
import logging

from bots.telemost_bot_adapter.telemost_ui_methods import TelemostUIMethods
from bots.web_bot_adapter import WebBotAdapter

logger = logging.getLogger(__name__)


class TelemostBotAdapter(WebBotAdapter, TelemostUIMethods):
    """
    Telemost adapter reuses the Google Meet web bot flow since Telemost UI is similar.
    Login helpers are disabled by default.
    """

    def __init__(
        self,
        *args,
        telemost_closed_captions_language: str | None,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        # Reuse Google Meet UI helpers; keep expected attribute names populated
        self.telemost_closed_captions_language = telemost_closed_captions_language
        self.google_meet_closed_captions_language = telemost_closed_captions_language
        self.google_meet_bot_login_is_available = False
        self.google_meet_bot_login_should_be_used = False
        self.create_google_meet_bot_login_session_callback = lambda: None
        self.google_meet_bot_login_session = None
        # Disable auto-leave-only-one for Telemost
        self.disable_only_one_auto_leave = True

    def should_retry_joining_meeting_that_requires_login_by_logging_in(self):
        return False

    def get_chromedriver_payload_file_name(self):
        return "telemost_bot_adapter/telemost_chromedriver_payload.js"

    def get_websocket_port(self):
        return 8770

    def is_sent_video_still_playing(self):
        result = self.driver.execute_script("return window.botOutputManager.isVideoPlaying();")
        logger.info(f"is_sent_video_still_playing result = {result}")
        return result

    def send_video(self, video_url):
        logger.info(f"send_video called with video_url = {video_url}")
        self.driver.execute_script(f"window.botOutputManager.playVideo({json.dumps(video_url)})")

    def send_chat_message(self, text, to_user_uuid=None):
        self.driver.execute_script(f"window?.sendChatMessage({json.dumps(text)})")

    def get_staged_bot_join_delay_seconds(self):
        return 5

    def subclass_specific_after_bot_joined_meeting(self):
        self.after_bot_can_record_meeting()

    def check_auto_leave_conditions(self):
        # Detect kicked/removed banner
        try:
            if self.detect_removed_from_meeting():
                return
            if self.detect_meeting_ended():
                return
        except Exception as e:
            logger.info(f"Error during detect_removed_from_meeting: {e}")
        # Fall back to base checks
        return super().check_auto_leave_conditions()
