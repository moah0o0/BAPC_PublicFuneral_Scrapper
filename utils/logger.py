"""
로깅 모듈
파일 + 텔레그램 알림 지원
"""

import logging
import datetime
import html
import uuid
from pathlib import Path
from typing import Optional
import requests

from config import Config, TelegramConfig


KST = datetime.timezone(datetime.timedelta(hours=9))


class TelegramHandler(logging.Handler):
    """텔레그램으로 로그 전송하는 핸들러"""

    def __init__(self, bot_token: str, chat_id: str, level: int = logging.ERROR):
        super().__init__(level)
        self.bot_token = bot_token
        self.chat_id = chat_id
        self.api_url = f"https://api.telegram.org/bot{bot_token}/sendMessage"

    def emit(self, record: logging.LogRecord):
        try:
            msg = self.format(record)
            # HTML 이스케이프
            msg = html.escape(msg)

            # 너무 긴 메시지는 잘라서 전송
            if len(msg) > 4000:
                msg = msg[:2000] + "\n\n(...)\n\n" + msg[-1500:]

            requests.get(
                self.api_url,
                params={
                    'chat_id': self.chat_id,
                    'text': f"<code>{msg}</code>",
                    'parse_mode': 'HTML',
                },
                timeout=10
            )
        except Exception:
            self.handleError(record)


class ScraperLogger:
    """스크래퍼 로거"""

    def __init__(self, config: Config):
        self.config = config
        self.telegram_config = config.telegram
        self.log_path = config.log_path
        self._setup_logger()

    def _setup_logger(self):
        """로거 설정"""
        self.logger = logging.getLogger("funeral_scraper")
        self.logger.setLevel(logging.DEBUG)

        # 기존 핸들러 제거
        self.logger.handlers.clear()

        # 파일 핸들러
        file_handler = logging.FileHandler(
            self.log_path,
            encoding='utf-8'
        )
        file_handler.setLevel(logging.DEBUG)
        file_formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        file_handler.setFormatter(file_formatter)
        self.logger.addHandler(file_handler)

        # 콘솔 핸들러
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.INFO)
        console_formatter = logging.Formatter(
            '%(asctime)s - %(levelname)s - %(message)s',
            datefmt='%H:%M:%S'
        )
        console_handler.setFormatter(console_formatter)
        self.logger.addHandler(console_handler)

    def info(self, message: str):
        """일반 로그"""
        self.logger.info(message)

    def error(self, message: str, exc_info: bool = False):
        """에러 로그"""
        self.logger.error(message, exc_info=exc_info)

    def debug(self, message: str):
        """디버그 로그"""
        self.logger.debug(message)

    def warning(self, message: str):
        """경고 로그"""
        self.logger.warning(message)

    def log_general(self, message: str, send_telegram: bool = True):
        """
        일반 알림 로그 (텔레그램 전송 포함)
        기존 LOG_GENERAL 함수 대체
        """
        date_time = datetime.datetime.now(KST).strftime("%Y년 %m월 %d일 %H시 %M분 %S.%f초")
        log_msg = f"[일반 통보] {message}\n-({date_time})"

        self.logger.info(log_msg)

        if send_telegram:
            self._send_telegram_general(message, date_time)

    def log_error(
        self,
        function_name: str,
        error_message: str,
        add_text: str = "",
        send_telegram: bool = True
    ):
        """
        에러 알림 로그 (텔레그램 전송 포함)
        기존 LOG_ERROR 함수 대체
        """
        uuid_code = str(uuid.uuid1())
        date_time = datetime.datetime.now(KST).strftime("%Y년 %m월 %d일 %H시 %M분 %S.%f초")

        log_msg = f"""
===========================
에러 발생 통보({function_name})
-① 고유번호({uuid_code})
-② 발생시간({date_time})
-③ 부가 메시지({add_text})

{error_message}"""

        self.logger.error(log_msg)

        if send_telegram:
            self._send_telegram_error(function_name, uuid_code, date_time, add_text, error_message)

    def _send_telegram_general(self, message: str, date_time: str):
        """텔레그램 일반 알림 전송"""
        try:
            telegram_msg = f'<b>[일반 통보] {message}</b>\n-({date_time})'
            requests.get(
                f"https://api.telegram.org/bot{self.telegram_config.bot_token}/sendMessage",
                params={
                    'chat_id': self.telegram_config.general_channel,
                    'text': telegram_msg,
                    'parse_mode': 'HTML',
                    'disable_notification': True
                },
                timeout=10
            )
        except Exception as e:
            self.logger.warning(f"텔레그램 전송 실패: {e}")

    def _send_telegram_error(
        self,
        function_name: str,
        uuid_code: str,
        date_time: str,
        add_text: str,
        error_message: str
    ):
        """텔레그램 에러 알림 전송"""
        try:
            # 에러 메시지가 너무 길면 잘라서 전송
            truncated_error = error_message
            if len(error_message) > 1000:
                truncated_error = error_message[:500] + '\n\n(...)\n\n' + error_message[-500:]

            telegram_msg = f"""<b>에러 발생 통보({function_name})</b>

① 고유번호({uuid_code})
② 발생시간({date_time})
③ 부가 메시지({add_text})

<code class="language-python">{html.escape(truncated_error)}</code>"""

            requests.get(
                f"https://api.telegram.org/bot{self.telegram_config.bot_token}/sendMessage",
                params={
                    'chat_id': self.telegram_config.error_channel,
                    'text': telegram_msg,
                    'parse_mode': 'HTML'
                },
                timeout=10
            )
        except Exception as e:
            self.logger.warning(f"텔레그램 에러 알림 전송 실패: {e}")


# 글로벌 로거 인스턴스
_logger: Optional[ScraperLogger] = None


def get_logger(config: Optional[Config] = None) -> ScraperLogger:
    """싱글톤 로거 인스턴스 반환"""
    global _logger
    if _logger is None:
        if config is None:
            from config import get_config
            config = get_config()
        _logger = ScraperLogger(config)
    return _logger
