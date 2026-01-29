"""
HTTP 클라이언트
직접 연결 우선, 차단 시 Tor 폴백
"""

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from urllib3 import disable_warnings
from urllib3.exceptions import InsecureRequestWarning
from typing import Optional, Dict
import logging

from random_user_agent.user_agent import UserAgent
from random_user_agent.params import SoftwareName, OperatingSystem

from config import Config, TorConfig

disable_warnings(InsecureRequestWarning)
logger = logging.getLogger(__name__)


def get_random_user_agent() -> str:
    """랜덤 User-Agent 생성"""
    software_names = [SoftwareName.CHROME.value]
    operating_systems = [OperatingSystem.WINDOWS.value, OperatingSystem.LINUX.value]
    user_agent_rotator = UserAgent(
        software_names=software_names,
        operating_systems=operating_systems,
        limit=100
    )
    return user_agent_rotator.get_random_user_agent()


class HttpClient:
    """
    HTTP 클라이언트 (Tor 폴백 지원)

    기본 동작:
    1. 직접 연결 시도
    2. 차단 감지 시 (403, 429, 503, ConnectionError) Tor로 재시도
    """

    # 차단으로 간주하는 HTTP 상태 코드
    BLOCKED_STATUS_CODES = {403, 429, 503}

    def __init__(self, config: Config):
        self.config = config
        self.tor_config = config.tor
        self.session = self._create_session()

    def _create_session(self) -> requests.Session:
        """세션 생성 (재시도 로직 포함)"""
        session = requests.Session()

        retry = Retry(
            total=3,
            backoff_factor=0.5,
            status_forcelist=[500, 502, 504],
        )
        adapter = HTTPAdapter(max_retries=retry)
        session.mount('http://', adapter)
        session.mount('https://', adapter)

        session.headers.update({
            "User-Agent": get_random_user_agent(),
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
            "Accept-Language": "ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7",
        })

        return session

    def get(
        self,
        url: str,
        force_tor: bool = False,
        timeout: int = 30,
        **kwargs
    ) -> requests.Response:
        """
        GET 요청 (Tor 폴백 지원)

        Args:
            url: 요청 URL
            force_tor: True면 Tor 직접 사용, False면 직접 연결 후 필요시 폴백
            timeout: 요청 타임아웃 (초)
            **kwargs: requests.get()에 전달할 추가 인자

        Returns:
            requests.Response

        Raises:
            requests.exceptions.RequestException: 요청 실패 시
        """
        kwargs.setdefault('verify', False)
        kwargs.setdefault('timeout', timeout)

        # force_tor가 True면 바로 Tor 사용
        if force_tor:
            return self._get_with_tor(url, **kwargs)

        # 직접 연결 시도
        try:
            response = self.session.get(url, **kwargs)
            response.raise_for_status()
            return response

        except requests.exceptions.HTTPError as e:
            if self._is_blocked(e) and self.tor_config.enabled:
                logger.info(f"차단 감지 (HTTP {e.response.status_code}), Tor로 재시도: {url}")
                return self._get_with_tor(url, **kwargs)
            raise

        except requests.exceptions.ConnectionError as e:
            if self.tor_config.enabled:
                logger.info(f"연결 실패, Tor로 재시도: {url}")
                return self._get_with_tor(url, **kwargs)
            raise

    def post(
        self,
        url: str,
        data: Optional[Dict] = None,
        params: Optional[Dict] = None,
        force_tor: bool = False,
        timeout: int = 30,
        **kwargs
    ) -> requests.Response:
        """
        POST 요청 (Tor 폴백 지원)
        """
        kwargs.setdefault('verify', False)
        kwargs.setdefault('timeout', timeout)

        if force_tor:
            return self._post_with_tor(url, data=data, params=params, **kwargs)

        try:
            response = self.session.post(url, data=data, params=params, **kwargs)
            response.raise_for_status()
            return response

        except requests.exceptions.HTTPError as e:
            if self._is_blocked(e) and self.tor_config.enabled:
                logger.info(f"차단 감지 (HTTP {e.response.status_code}), Tor로 재시도: {url}")
                return self._post_with_tor(url, data=data, params=params, **kwargs)
            raise

        except requests.exceptions.ConnectionError as e:
            if self.tor_config.enabled:
                logger.info(f"연결 실패, Tor로 재시도: {url}")
                return self._post_with_tor(url, data=data, params=params, **kwargs)
            raise

    def _is_blocked(self, error: requests.exceptions.HTTPError) -> bool:
        """차단 여부 판단"""
        if error.response is not None:
            return error.response.status_code in self.BLOCKED_STATUS_CODES
        return False

    def _get_with_tor(self, url: str, **kwargs) -> requests.Response:
        """Tor를 통한 GET 요청"""
        kwargs['proxies'] = self.tor_config.proxies
        kwargs.setdefault('timeout', 60)  # Tor는 느리므로 타임아웃 증가

        response = self.session.get(url, **kwargs)
        response.raise_for_status()
        return response

    def _post_with_tor(
        self,
        url: str,
        data: Optional[Dict] = None,
        params: Optional[Dict] = None,
        **kwargs
    ) -> requests.Response:
        """Tor를 통한 POST 요청"""
        kwargs['proxies'] = self.tor_config.proxies
        kwargs.setdefault('timeout', 60)

        response = self.session.post(url, data=data, params=params, **kwargs)
        response.raise_for_status()
        return response

    def get_text(self, url: str, encoding: str = "utf-8", **kwargs) -> str:
        """GET 요청 후 텍스트 반환"""
        response = self.get(url, **kwargs)
        response.encoding = encoding
        return response.text

    def post_text(
        self,
        url: str,
        data: Optional[Dict] = None,
        params: Optional[Dict] = None,
        encoding: str = "utf-8",
        **kwargs
    ) -> str:
        """POST 요청 후 텍스트 반환 (실제로는 GET with params)"""
        # 기존 requestx.post는 실제로 GET + params를 사용했음
        response = self.get(url, params=params, **kwargs)
        response.encoding = encoding
        return response.text
