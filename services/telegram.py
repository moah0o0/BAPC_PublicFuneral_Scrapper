"""
텔레그램 서비스
알림 및 부고 메시지 전송
"""

import datetime
import html
import logging
import time
from typing import Dict, Optional
import requests

from config import TelegramConfig, DISTRICT_NAMES_KOR_TO_ENG

logger = logging.getLogger(__name__)

# 텔레그램 API 레이트 리밋: 같은 채널 초당 1개
TELEGRAM_RATE_LIMIT_DELAY = 1.0  # 초
KST = datetime.timezone(datetime.timedelta(hours=9))


class TelegramService:
    """텔레그램 메시지 전송 서비스"""

    def __init__(self, config: TelegramConfig):
        self.config = config
        self.bot_token = config.bot_token
        self.api_base = f"https://api.telegram.org/bot{config.bot_token}"
        self.templates = config.templates

    def _send_message(
        self,
        chat_id: str,
        text: str,
        parse_mode: str = "HTML",
        disable_notification: bool = False
    ) -> bool:
        """
        메시지 전송

        Returns:
            성공 여부
        """
        try:
            response = requests.get(
                f"{self.api_base}/sendMessage",
                params={
                    'chat_id': chat_id,
                    'text': text,
                    'parse_mode': parse_mode,
                    'disable_notification': disable_notification
                },
                timeout=10
            )

            # API 응답 확인
            result = response.json()
            if not result.get("ok"):
                error_code = result.get("error_code", "unknown")
                description = result.get("description", "no description")
                logger.error(
                    f"텔레그램 API 오류 (chat_id={chat_id}): "
                    f"[{error_code}] {description}"
                )
                print(f"  [TELEGRAM ERROR] chat_id={chat_id}, code={error_code}, desc={description}")
                return False

            response.raise_for_status()

            # 레이트 리밋 방지 딜레이
            time.sleep(TELEGRAM_RATE_LIMIT_DELAY)
            return True
        except requests.exceptions.RequestException as e:
            # HTTP 에러 상세 로깅
            error_detail = str(e)
            if hasattr(e, 'response') and e.response is not None:
                try:
                    api_response = e.response.json()
                    error_code = api_response.get("error_code", "unknown")
                    description = api_response.get("description", "no description")
                    error_detail = f"[{error_code}] {description}"
                except:
                    error_detail = f"HTTP {e.response.status_code}: {e.response.text[:200]}"

            logger.error(f"텔레그램 전송 실패 (chat_id={chat_id}): {error_detail}")
            print(f"  [TELEGRAM ERROR] chat_id={chat_id}, error={error_detail}")
            return False

    def send_funeral_notification(
        self,
        district_kor: str,
        url: str,
        update_count: int,
        analyzed_data: Dict[str, str]
    ) -> bool:
        """
        부고 알림 전송

        Args:
            district_kor: 구청명 (한국어, 예: "해운대구")
            url: 원본 URL
            update_count: 수정 횟수
            analyzed_data: 분석된 부고 정보

        Returns:
            성공 여부
        """
        # 채널 ID 찾기
        district_eng = DISTRICT_NAMES_KOR_TO_ENG.get(district_kor)
        if not district_eng:
            logger.warning(f"Unknown district: {district_kor}")
            return False

        channel_id = self.config.district_channels.get(district_eng)
        if not channel_id:
            logger.warning(f"No channel for district: {district_eng}")
            return False

        # 템플릿으로 메시지 생성
        title = self.templates.format_funeral_title(district_kor, update_count)

        # 정보 항목 (HTML 이스케이프 적용)
        escaped_data = {k: html.escape(str(v)) for k, v in analyzed_data.items()}
        info_text = self.templates.format_funeral_info(escaped_data)

        # 링크
        url_text = self.templates.funeral_link.format(url=url.replace('&>&', '&'))

        message = f"{title}\n{info_text}{url_text}"

        # 야간 알림 무음 처리
        is_night = self._is_night_time()

        # 구청 채널에 전송
        success1 = self._send_message(channel_id, message, disable_notification=is_night)

        # 통합 채널에도 전송
        success2 = self._send_message(
            self.config.funeral_main,
            message,
            disable_notification=is_night
        )

        return success1 and success2

    def send_general_notification(self, message: str) -> bool:
        """일반 알림 전송"""
        date_time = datetime.datetime.now(KST).strftime("%Y년 %m월 %d일 %H시 %M분 %S초")
        text = self.templates.general_notification.format(
            message=html.escape(message),
            datetime=date_time
        )

        return self._send_message(
            self.config.general_channel,
            text,
            disable_notification=True
        )

    def send_error_notification(
        self,
        function_name: str,
        error_message: str,
        uuid_code: str,
        add_text: str = ""
    ) -> bool:
        """에러 알림 전송"""
        date_time = datetime.datetime.now(KST).strftime("%Y년 %m월 %d일 %H시 %M분 %S초")

        # 에러 메시지 길이 제한
        if len(error_message) > 1000:
            error_message = error_message[:500] + '\n\n(...)\n\n' + error_message[-500:]

        text = self.templates.error_notification.format(
            function_name=html.escape(function_name),
            uuid=uuid_code,
            datetime=date_time,
            add_text=html.escape(add_text),
            error_message=html.escape(error_message)
        )

        return self._send_message(self.config.error_channel, text)

    def _is_night_time(self) -> bool:
        """야간 시간대 여부 (20:00 ~ 07:00)"""
        now = datetime.datetime.now(KST)
        t = now.time()
        return t >= datetime.time(20, 0) or t < datetime.time(7, 0)
