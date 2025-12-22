import logging
import time

from selenium.common.exceptions import NoSuchElementException, TimeoutException
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.common.keys import Keys

from bots.models import RecordingViews
from bots.web_bot_adapter.ui_methods import (
    UiCouldNotClickElementException,
    UiCouldNotJoinMeetingWaitingRoomTimeoutException,
    UiCouldNotLocateElementException,
    UiBlockedByCaptchaException,
    UiRetryableException,
    UiRetryableExpectedException,
)

logger = logging.getLogger(__name__)


class TelemostUIMethods:
    def _is_captcha_page(self) -> bool:
        try:
            url = (self.driver.current_url or "").lower()
            if "showcaptcha" in url:
                return True
            if "captcha" in url and "yandex" in url:
                return True
        except Exception:
            return False
        return False

    def _raise_if_captcha_page(self, step: str):
        if self._is_captcha_page():
            raise UiBlockedByCaptchaException("Telemost blocked by captcha", step=step, inner_exception=None)

    def _is_in_meeting(self) -> bool:
        try:
            leave_button = self.find_element_by_selector(
                By.XPATH,
                '//button[@title="Выйти из встречи" or contains(@class,"endCallButton") or contains(@aria-label,"Выйти")]',
            )
            if leave_button:
                return True
        except Exception:
            pass
        return False

    def locate_element(self, step, condition, wait_time_seconds=30):
        try:
            element = WebDriverWait(self.driver, wait_time_seconds).until(condition)
            return element
        except Exception as e:
            logger.info(f"Exception raised in locate_element for {step}")
            raise UiCouldNotLocateElementException(f"Exception raised in locate_element for {step}", step, e)

    def find_element_by_selector(self, selector_type, selector):
        try:
            return self.driver.find_element(selector_type, selector)
        except NoSuchElementException:
            return None
        except Exception as e:
            logger.info(f"Unknown error occurred in find_element_by_selector. Exception type = {type(e)}")
            return None

    def click_element_forcefully(self, element, step):
        try:
            self.driver.execute_script("arguments[0].click();", element)
        except Exception as e:
            logger.info(f"Error occurred when forcefully clicking element for step {step}, will retry")
            raise UiCouldNotClickElementException("Error occurred when forcefully clicking element", step, e)

    def click_element(self, element, step):
        try:
            element.click()
        except Exception as e:
            logger.info(f"Error occurred when clicking element for step {step}, attempting js click fallback. Exception class name was {e.__class__.__name__}")
            try:
                self.driver.execute_script("arguments[0].click();", element)
                return
            except Exception as inner_e:
                logger.info(f"JS click fallback failed for step {step}. Exception class name was {inner_e.__class__.__name__}")
                raise UiCouldNotClickElementException("Error occurred when clicking element", step, inner_e)

    def get_layout_to_select(self):
        if self.recording_view == RecordingViews.SPEAKER_VIEW:
            return "sidebar"
        elif self.recording_view == RecordingViews.GALLERY_VIEW:
            return "tiled"
        elif self.recording_view == RecordingViews.SPEAKER_VIEW_NO_SIDEBAR:
            return "spotlight"
        else:
            return "sidebar"

    def attempt_to_join_meeting(self):
        self.driver.get(self.meeting_url)
        self._raise_if_captcha_page("navigate_to_meeting")

        # Step 1: click "Продолжить в браузере" if present
        try:
            continue_button = WebDriverWait(self.driver, 8).until(
                EC.presence_of_element_located((By.XPATH, '//button[.//div[text()="Продолжить в браузере"] or contains(., "Продолжить в браузере")]'))
            )
            logger.info("Clicking 'Продолжить в браузере'")
            self.click_element_forcefully(continue_button, "continue_in_browser")
            self._raise_if_captcha_page("continue_in_browser")
        except TimeoutException:
            logger.info("Continue-in-browser button not found, proceeding")
        except UiCouldNotClickElementException as e:
            logger.info("Could not click continue-in-browser, retryable")
            raise UiRetryableExpectedException("Could not click continue-in-browser", "continue_in_browser", e)

        # Step 2: fill name input if present
        name_filled = False
        name_locators = [
            (By.XPATH, '//input[@type="text" and (contains(@placeholder,"имя") or contains(@aria-label,"имя"))]'),
            (By.XPATH, '//input[@type="text"]'),
            (By.XPATH, '//input'),
        ]
        for locator in name_locators:
            try:
                name_input = WebDriverWait(self.driver, 8).until(EC.presence_of_element_located(locator))
                logger.info(f"Filling name input found via locator {locator}")
                try:
                    name_input.clear()
                except Exception:
                    pass
                try:
                    name_input.send_keys(Keys.CONTROL + "a")
                    name_input.send_keys(Keys.BACK_SPACE)
                except Exception:
                    logger.info("Could not select+backspace existing name; proceeding")
                name_input.send_keys(self.display_name)
                name_filled = True
                break
            except TimeoutException:
                continue
            except Exception as e:
                logger.info(f"Error filling name input via {locator}: {e}")
                continue

        if not name_filled:
            logger.info("Name input not found; continuing without filling")

        # Step 3: click join button
        try:
            join_button = WebDriverWait(self.driver, 20).until(
                EC.element_to_be_clickable((By.XPATH, '//button[contains(.,"Подключиться")]'))
            )
            try:
                self.driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", join_button)
            except Exception:
                logger.info("Could not scroll join button into view; continuing")
            # Toggle mic and camera off if buttons are present
            try:
                mic_button = self.driver.find_element(
                    By.XPATH,
                    '//button[@title="Выключить микрофон" or contains(@class,"MicrophoneButton")]',
                )
                logger.info("Clicking mic button to mute")
                self.click_element(mic_button, "mute_microphone")
            except Exception:
                logger.info("Mic button not found, skipping mute")
            try:
                cam_button = self.driver.find_element(
                    By.XPATH,
                    '//button[@title="Выключить камеру" or contains(@class,"CameraButton")]',
                )
                logger.info("Clicking camera button to disable video")
                self.click_element(cam_button, "disable_camera")
            except Exception:
                logger.info("Camera button not found, skipping disable camera")

            logger.info("Clicking join button")
            self.click_element(join_button, "join_button")
        except Exception as e:
            logger.info(f"Could not click join button via primary path: {e}. Trying fallbacks.")
            fallback_selectors = [
                '//div[@role="button" and contains(.,"Подключиться")]',
                '//button[@type="submit"]',
                '//button[contains(.,"Войти") or contains(.,"Начать")]',
            ]
            clicked = False
            for selector in fallback_selectors:
                try:
                    candidates = self.driver.find_elements(By.XPATH, selector)
                    for candidate in candidates:
                        try:
                            self.driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", candidate)
                        except Exception:
                            pass
                        try:
                            self.driver.execute_script("arguments[0].click();", candidate)
                            clicked = True
                            logger.info(f"Clicked join fallback selector {selector}")
                            break
                        except Exception:
                            continue
                    if clicked:
                        break
                except Exception:
                    continue

            if not clicked:
                raise UiRetryableException("Could not click join button", "join_button", e)

        # Wait until we actually see in-meeting UI; Telemost can redirect to captcha or other pages.
        join_deadline = time.time() + 40
        while time.time() < join_deadline:
            self._raise_if_captcha_page("wait_for_in_meeting")
            if self._is_in_meeting():
                return
            time.sleep(0.5)

        raise UiRetryableException("Timed out waiting for in-meeting UI after clicking join", "wait_for_in_meeting", None)

    def detect_removed_from_meeting(self):
        try:
            removed_banner = self.driver.find_element(
                By.XPATH,
                '//div[contains(@class,"MessageBox") and .//div[contains(text(),"Вы были удалены со встречи")]]',
            )
            if removed_banner:
                logger.info("Detected removed-from-meeting banner")
                self.handle_removed_from_meeting()
                return True
        except Exception:
            return False
        return False

    def detect_meeting_ended(self):
        # Only evaluate after we've joined
        if not self.joined_at:
            return False
        elapsed = time.time() - self.joined_at

        # Explicit end banners: allow fairly quickly after join
        if elapsed > 5:
            try:
                ended_banner = self.driver.find_elements(
                    By.XPATH,
                    '//*[contains(text(),"Встреча завершена") or contains(text(),"Звонок завершен") or contains(text(),"Звонок завершён") or contains(text(),"встреча завершена для всех") or contains(text(),"организатор завершил") or contains(text(),"отправьте им ссылку на встречу")]',
                )
                if ended_banner:
                    logger.info("Detected meeting ended banner")
                    self.handle_meeting_ended()
                    return True
            except Exception:
                pass

        # Heuristics: telemost redirects to homepage without /j/<id> and/or shows home tiles. Delay to avoid join-time redirects.
        if elapsed > 20:
            try:
                path = self.driver.execute_script("return window.location.pathname || ''")
                if "/j/" not in path:
                    logger.info(f"Detected meeting ended via path redirect: {path}")
                    self.handle_meeting_ended()
                    return True
            except Exception:
                pass

            try:
                home_tiles = self.driver.find_elements(
                    By.XPATH,
                    '//*[contains(text(),"Создать видеовстречу") or contains(text(),"Подключиться") or contains(text(),"Запланировать")]',
                )
                if home_tiles and "/j/" not in (self.driver.current_url or ""):
                    logger.info("Detected telemost home screen, treating as meeting ended")
                    self.handle_meeting_ended()
                    return True
            except Exception:
                pass

        return False

    # Placeholder overrides to satisfy WebBotAdapter hooks
    def click_leave_button(self):
        try:
            leave_button = WebDriverWait(self.driver, 10).until(
                EC.presence_of_element_located(
                    (
                        By.XPATH,
                        '//button[@title="Выйти из встречи" or contains(@class,"endCallButton")]',
                    )
                )
            )
            logger.info("Clicking Telemost leave button")
            self.click_element(leave_button, "leave_button")
        except Exception as e:
            logger.info(f"Could not click leave button: {e}")
            raise UiRetryableException("Could not click leave button", "leave_button", e)

    def subclass_specific_after_bot_joined_meeting(self):
        pass
