"""
HTTP 클라이언트
직접 연결 우선, 차단 시 Tor 폴백 및 헤더 동적 생성 적용
"""

import requests
import time
import random
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from urllib3 import disable_warnings
from urllib3.exceptions import InsecureRequestWarning
from typing import Optional, Dict
import logging
from urllib.parse import urlparse

from random_user_agent.user_agent import UserAgent
from random_user_agent.params import SoftwareName, OperatingSystem

from config import Config

disable_warnings(InsecureRequestWarning)
logger = logging.getLogger(__name__)


def get_random_user_agent() -> str:
    """랜덤 User-Agent 생성 (Windows/Linux Chrome 위주)"""
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
    HTTP 클라이언트 (Tor 폴백 및 동적 헤더 지원)

    기본 동작:
    1. 매 요청마다 User-Agent와 Referer를 새롭게 생성
    2. 직접 연결 시도
    3. 차단 감지 시 (403, 429, 503, ConnectionError) Tor로 재시도
    """

    # 차단으로 간주하는 HTTP 상태 코드
    BLOCKED_STATUS_CODES = {403, 429, 503}

    def __init__(self, config: Config):
        self.config = config
        self.tor_config = config.tor
        self.session = self._create_session()

    def _create_session(self) -> requests.Session:
        """기본 세션 설정"""
        session = requests.Session()

        retry = Retry(
            total=3,
            backoff_factor=1,
            status_forcelist=[500, 502, 504],
        )
        adapter = HTTPAdapter(max_retries=retry)
        session.mount('http://', adapter)
        session.mount('https://', adapter)

        # 공통 기본 헤더 (브라우저와 유사하게 설정)
        session.headers.update({
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
            "Accept-Language": "ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7",
            "Accept-Encoding": "gzip, deflate, br",
            "Connection": "keep-alive",
            "Upgrade-Insecure-Requests": "1",
            "Sec-Ch-Ua": '"Not_A Brand";v="8", "Chromium";v="120", "Google Chrome";v="120"',
            "Sec-Ch-Ua-Mobile": "?0",
            "Sec-Ch-Ua-Platform": '"Windows"',
            "Sec-Fetch-Dest": "document",
            "Sec-Fetch-Mode": "navigate",
            "Sec-Fetch-Site": "none",
            "Sec-Fetch-User": "?1",
        })

        return session

    def _get_fresh_headers(self, url: str) -> Dict[str, str]:
        """매 요청마다 새로운 브라우저 정체성(UA, Referer) 생성"""
        parsed_url = urlparse(url)
        headers = {
            "User-Agent": get_random_user_agent(),
            "Referer": f"{parsed_url.scheme}://{parsed_url.netloc}/",
            "Host": parsed_url.netloc,
        }
        return headers

    def _apply_headers(self, url: str, kwargs: dict):
        """요청 직전 헤더 병합"""
        fresh_headers = self._get_fresh_headers(url)
        if 'headers' in kwargs:
            fresh_headers.update(kwargs['headers'])
        kwargs['headers'] = fresh_headers

    def get(
        self,
        url: str,
        force_tor: bool = False,
        timeout: int = 30,
        **kwargs
    ) -> requests.Response:
        """GET 요청 (차단 시 Tor 폴백)"""
        kwargs.setdefault('verify', False)
        kwargs.setdefault('timeout', timeout)
        self._apply_headers(url, kwargs)

        if force_tor:
            return self._get_with_tor(url, 'GET', **kwargs)

        try:
            response = self.session.get(url, **kwargs)
            response.raise_for_status()
            return response

        except requests.exceptions.HTTPError as e:
            if self._is_blocked(e) and self.tor_config.enabled:
                logger.warning(f"GET 차단 감지 ({e.response.status_code}), 잠시 후 Tor로 재시도: {url}")
                time.sleep(random.uniform(2.0, 5.0))  # 인간적인 지연 시간 추가
                return self._get_with_tor(url, 'GET', **kwargs)
            raise

        except requests.exceptions.ConnectionError:
            if self.tor_config.enabled:
                logger.warning(f"GET 연결 실패, 잠시 후 Tor로 재시도: {url}")
                time.sleep(random.uniform(1.0, 3.0))
                return self._get_with_tor(url, 'GET', **kwargs)
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
        """POST 요청 (차단 시 Tor 폴백)"""
        kwargs.setdefault('verify', False)
        kwargs.setdefault('timeout', timeout)
        self._apply_headers(url, kwargs)

        if force_tor:
            return self._get_with_tor(url, 'POST', data=data, params=params, **kwargs)

        try:
            response = self.session.post(url, data=data, params=params, **kwargs)
            response.raise_for_status()
            return response

        except requests.exceptions.HTTPError as e:
            if self._is_blocked(e) and self.tor_config.enabled:
                logger.warning(f"POST 차단 감지 ({e.response.status_code}), 잠시 후 Tor로 재시도: {url}")
                time.sleep(random.uniform(2.0, 5.0))  # 즉시 재시도하지 않고 지연 발생
                return self._get_with_tor(url, 'POST', data=data, params=params, **kwargs)
            raise

        except requests.exceptions.ConnectionError:
            if self.tor_config.enabled:
                logger.warning(f"POST 연결 실패, 잠시 후 Tor로 재시도: {url}")
                time.sleep(random.uniform(1.0, 3.0))
                return self._get_with_tor(url, 'POST', data=data, params=params, **kwargs)
            raise

    def _is_blocked(self, error: requests.exceptions.HTTPError) -> bool:
        """차단 여부 판단"""
        if error.response is not None:
            return error.response.status_code in self.BLOCKED_STATUS_CODES
        return False

    def _get_with_tor(
        self, 
        url: str, 
        method: str, 
        data: Optional[Dict] = None, 
        params: Optional[Dict] = None, 
        **kwargs
    ) -> requests.Response:
        """Tor 프록시를 통한 요청 수행 및 세션 정화"""
        kwargs['proxies'] = self.tor_config.proxies
        kwargs['timeout'] = max(kwargs.get('timeout', 30), 60) # Tor는 항상 60초 이상 권장
        
        # 차단된 기존 쿠키가 Tor IP를 오염시키지 않도록 쿠키 제거
        self.session.cookies.clear()
        
        # 재시도 시 완전히 새로운 신분(Headers)으로 위장
        self._apply_headers(url, kwargs)
        # Tor 환경이므로 Referer를 Same-origin으로 좀 더 명확히 설정
        kwargs['headers']['Sec-Fetch-Site'] = 'same-origin'

        if method == 'GET':
            response = self.session.get(url, **kwargs)
        else:
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
        """
        POST 요청처럼 보이지만 실제로는 GET with params를 사용
        (일부 공공기관 서버는 POST 인터페이스 형태를 띠면서 실제로는 GET 요청을 요구함)
        """
        response = self.get(url, params=params, **kwargs)
        response.encoding = encoding
        return response.text
