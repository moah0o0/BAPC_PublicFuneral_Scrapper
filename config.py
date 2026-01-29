"""
설정 관리 모듈
환경변수 기반 설정을 dataclass로 관리
"""

from dataclasses import dataclass, field
from typing import Dict, Optional
import os
from pathlib import Path

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass


# 구청명 매핑
DISTRICT_NAMES_ENG_TO_KOR = {
    "BUKGU": "북구",
    "DONGGU": "동구",
    "DONGNAE": "동래구",
    "GANGSEO": "강서구",
    "GEUMJEONG": "금정구",
    "GIJANG": "기장군",
    "HAEUNDAE": "해운대구",
    "JINGU": "부산진구",
    "JUNGGU": "중구",
    "NAMGU": "남구",
    "SAHA": "사하구",
    "SASANG": "사상구",
    "SEOGU": "서구",
    "SUYEONG": "수영구",
    "YEONGDOGU": "영도구",
    "YEONJE": "연제구"
}

DISTRICT_NAMES_KOR_TO_ENG = {v: k for k, v in DISTRICT_NAMES_ENG_TO_KOR.items()}

# 구청별 텔레그램 채널 ID
DISTRICT_CHANNEL_IDS = {
    "BUKGU": "-4130128744",
    "DONGGU": "-4165065871",
    "DONGNAE": "-4178400896",
    "GANGSEO": "-4104549484",
    "GEUMJEONG": "-4199264402",
    "GIJANG": "-4158955899",
    "HAEUNDAE": "-4158535154",
    "JINGU": "-4178316252",
    "JUNGGU": "-4178873706",
    "NAMGU": "-4155333786",
    "SAHA": "-4158043390",
    "SASANG": "-4153235935",
    "SEOGU": "-4162434773",
    "SUYEONG": "-4120218199",
    "YEONGDOGU": "-4193340992",
    "YEONJE": "-4104969591"
}

# 테스트 모드: 모든 메시지를 GENERAL_CHANNEL로 전송
TELEGRAM_TEST_MODE = os.getenv("TELEGRAM_TEST_MODE", "false").lower() == "true"
if TELEGRAM_TEST_MODE:
    print("⚠️  [TEST MODE] 모든 텔레그램 메시지가 GENERAL_CHANNEL로 전송됩니다.")
    for key in DISTRICT_CHANNEL_IDS:
        DISTRICT_CHANNEL_IDS[key] = os.getenv("TELEGRAM_TEST_CHANNEL")

# Tor 사용이 필요한 구청 (차단 이력 있음)
TOR_REQUIRED_DISTRICTS = ["HAEUNDAE", "JINGU", "GEUMJEONG", "SASANG", "JUNGGU"]


# ==================== 메시지 템플릿 ====================

@dataclass
class MessageTemplates:
    """텔레그램 메시지 템플릿"""

    # 부고 알림 - 새 게시물
    funeral_new: str = "<b>🔔 [{district}] 새로운 부고가 게시되었습니다.</b>"

    # 부고 알림 - 수정된 게시물
    funeral_updated: str = "<b>🔔 [{district}] 부고가 수정되었습니다(ver. {version})</b>"

    # 부고 정보 항목 (번호 + 항목명 + 값)
    funeral_info_item: str = "{num} {key} : {value}"

    # 부고 원문 링크
    funeral_link: str = "\n<a href='{url}'>부고 게시물 확인</a>"

    # 일반 알림
    general_notification: str = "<b>[일반 통보] ✅ {message}</b>\n-({datetime})"

    # 에러 알림
    error_notification: str = """<b>🚨 에러 발생 통보({function_name})</b>

① 고유번호({uuid})
② 발생시간({datetime})
③ 부가 메시지({add_text})

<code class="language-python">{error_message}</code>"""

    # 스케줄러 시작 알림
    scheduler_start: str = "서버 가동 시작했습니다."

    # 스케줄러 종료 알림
    scheduler_end: str = "서버 모두 종료합니다."

    # 단계별 알림
    phase_start: str = "START_{phase}/3. {name} 시작합니다."
    phase_end: str = "FINISH_{phase}/3. {name} 실행을 종료했습니다."

    # 구청 스크래핑 시도
    district_attempt: str = "{district} 시도 중"

    # 번호 매기기용 (①②③...)
    numbered_markers: tuple = ("①", "②", "③", "④", "⑤", "⑥", "⑦", "⑧", "⑨", "⑩")

    def format_funeral_title(self, district: str, update_count: int) -> str:
        """부고 제목 포맷팅"""
        if update_count == 0:
            return self.funeral_new.format(district=district)
        return self.funeral_updated.format(district=district, version=update_count)

    def format_funeral_info(self, data: Dict[str, str]) -> str:
        """부고 정보 항목들 포맷팅"""
        lines = []
        for i, (key, value) in enumerate(data.items()):
            if i < len(self.numbered_markers):
                num = self.numbered_markers[i]
                lines.append(self.funeral_info_item.format(num=num, key=key, value=value))
        return "\n".join(lines)


# 기본 메시지 템플릿 인스턴스
DEFAULT_TEMPLATES = MessageTemplates()


@dataclass
class TelegramConfig:
    """텔레그램 설정"""
    bot_token: str
    error_channel: str
    general_channel: str
    funeral_main: str
    district_channels: Dict[str, str] = field(default_factory=lambda: DISTRICT_CHANNEL_IDS.copy())
    templates: MessageTemplates = field(default_factory=MessageTemplates)


@dataclass
class TorConfig:
    """Tor 프록시 설정"""
    enabled: bool = True
    host: str = "127.0.0.1"
    port: int = 9050

    @property
    def proxy_url(self) -> str:
        return f"socks5://{self.host}:{self.port}"

    @property
    def proxies(self) -> Dict[str, str]:
        return {
            "http": self.proxy_url,
            "https": self.proxy_url
        }


@dataclass
class PocketbaseConfig:
    """Pocketbase 설정"""
    url: str
    email: str
    password: str


@dataclass
class Config:
    """전체 설정"""
    telegram: TelegramConfig
    tor: TorConfig
    pocketbase: PocketbaseConfig
    openai_api_key: str
    max_page_num: int = 1
    schedule_interval_minutes: int = 15
    log_file: str = "log.txt"
    base_dir: Path = field(default_factory=lambda: Path(__file__).parent)

    @property
    def log_path(self) -> Path:
        return self.base_dir / self.log_file


def load_config() -> Config:
    """환경변수에서 설정 로드"""
    # 테스트 모드면 funeral_main도 GENERAL_CHANNEL로
    test_mode = os.getenv("TELEGRAM_TEST_MODE", "false").lower() == "true"
    funeral_main = os.getenv("TELEGRAM_TEST_CHANNEL") if test_mode else os.getenv("TELEGRAM_FUNERAL_MAIN")
    error_channel = os.getenv("TELEGRAM_TEST_CHANNEL") if test_mode else os.getenv("TELEGRAM_ERROR_CHANNEL")

    return Config(
        telegram=TelegramConfig(
            bot_token=os.getenv("TELEGRAM_BOT_TOKEN"),
            error_channel=error_channel,
            general_channel=os.getenv("TELEGRAM_GENERAL_CHANNEL"),
            funeral_main=funeral_main,
        ),
        tor=TorConfig(
            enabled=os.getenv("TOR_ENABLED").lower() == "true",
            host=os.getenv("TOR_HOST"),
            port=int(os.getenv("TOR_PORT")),
        ),
        pocketbase=PocketbaseConfig(
            url=os.getenv("POCKETBASE_URL"),
            email=os.getenv("POCKETBASE_EMAIL"),
            password=os.getenv("POCKETBASE_PASSWORD"),
        ),
        openai_api_key=os.getenv("OPENAI_API_KEY"),
        max_page_num=int(os.getenv("MAX_PAGE_NUM")),
        schedule_interval_minutes=int(os.getenv("SCHEDULE_INTERVAL_MINUTES")),
        log_file=os.getenv("LOG_FILE", "log.txt"),
    )


# 글로벌 설정 인스턴스 (지연 로딩)
_config: Optional[Config] = None


def get_config() -> Config:
    """싱글톤 설정 인스턴스 반환"""
    global _config
    if _config is None:
        _config = load_config()
    return _config
