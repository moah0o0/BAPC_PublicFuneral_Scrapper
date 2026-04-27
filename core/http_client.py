"""
HTTP 클라이언트
직접 연결 우선, 차단 시 Tor 폴백 및 헤더 동적 생성 적용
IP 변경 확인을 위한 로깅 기능 및 Tor 회로 갱신 지원
"""

import requests
import time
import random
import socket
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from urllib3 import disable_warnings
from urllib3.exceptions import InsecureRequestWarning
from typing import Optional, Dict
import logging
from urllib.parse import urlparse

from random_user_agent.user_agent import UserAgent
from random_user_agent.params import SoftwareName, OperatingSystem

# 주의: 실제 환경에서는 config.py가 존재해야 합니다.
try:
    from config import Config
except ImportError:
    class Config:
        class Tor:
            enabled = True
            proxies = {'http': 'socks5h://127.0.0.1:9050', 'https': 'socks5h://127.0.0.1:9050'}
            control_port = 9051  # Tor 컨트롤 포트 추가
            password = None      # Tor 컨트롤 비밀번호
        tor = Tor()

disable_warnings(InsecureRequestWarning)
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
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

    기능:
    1. 요청 전후 IP 로깅 (Tor 전환 확인)
    2. 매 요청마다 User-Agent와 Referer를 새롭게 생성
    3. 직접 연결 시도 후 차단 시 Tor 폴백 및 회로 갱신 시도
    """

    BLOCKED_STATUS_CODES = {403, 429, 503}
    IP_CHECK_URL = "https://api.ipify.org"

    def __init__(self, config: Config):
        self.config = config
        self.tor_config = config.tor
        self.session = self._create_session()
        
        # 초기 직접 연결 IP 확인
        self.origin_ip = self._get_current_ip()
        logger.info(f"HTTP 클라이언트 초기화 완료. 현재 직접 연결 IP: {self.origin_ip}")

    def _create_session(self) -> requests.Session:
        """기본 세션 설정"""
        session = requests.Session()
        retry = Retry(
            total=2,
            backoff_factor=1,
            status_forcelist=[500, 502, 504],
        )
        adapter = HTTPAdapter(max_retries=retry)
        session.mount('http://', adapter)
        session.mount('https://', adapter)

        session.headers.update({
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
            "Accept-Language": "ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7",
            "Accept-Encoding": "gzip, deflate, br",
            "Connection": "keep-alive",
            "Upgrade-Insecure-Requests": "1",
            "Sec-Ch-Ua-Mobile": "?0",
        })
        return session

    def _renew_tor_circuit(self):
        """Tor 회로를 갱신하여 새로운 IP 요청 (컨트롤 포트 필요)"""
        if not hasattr(self.tor_config, 'control_port'):
            return
        
        try:
            with socket.create_connection(("127.0.0.1", self.tor_config.control_port)) as s:
                auth_cmd = f'AUTHENTICATE "{self.tor_config.password or ""}"\r\n'
                s.send(auth_cmd.encode())
                s.recv(1024)
                s.send(b"SIGNAL NEWNYM\r\n")
                s.recv(1024)
                logger.info("Tor 회로 갱신 신호를 보냈습니다. (NEWNYM)")
                time.sleep(2) # IP가 실제로 바뀌기 위한 대기시간
        except Exception as e:
            logger.debug(f"Tor 회로 갱신 실패 (컨트롤 포트 미설정 등): {e}")

    def _get_current_ip(self, proxies: Optional[Dict] = None) -> str:
        """현재 외부 노출 IP 주소 조회"""
        try:
            resp = requests.get(self.IP_CHECK_URL, proxies=proxies, timeout=10, verify=False)
            return resp.text.strip()
        except Exception as e:
            logger.error(f"IP 주소 확인 실패: {e}")
            return "Unknown"

    def _get_fresh_headers(self, url: str) -> Dict[str, str]:
        """매 요청마다 새로운 브라우저 정체성 생성"""
        parsed_url = urlparse(url)
        ua = get_random_user_agent()
        return {
            "User-Agent": ua,
            "Referer": f"{parsed_url.scheme}://{parsed_url.netloc}/",
            "Host": parsed_url.netloc,
            "Origin": f"{parsed_url.scheme}://{parsed_url.netloc}",
        }

    def _apply_headers(self, url: str, kwargs: dict):
        """요청 직전 헤더 병합"""
        fresh_headers = self._get_fresh_headers(url)
        if 'headers' in kwargs:
            fresh_headers.update(kwargs['headers'])
        kwargs['headers'] = fresh_headers

    def get(self, url: str, force_tor: bool = False, timeout: int = 30, **kwargs) -> requests.Response:
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
        except (requests.exceptions.HTTPError, requests.exceptions.ConnectionError) as e:
            if self.tor_config.enabled and (isinstance(e, requests.exceptions.ConnectionError) or self._is_blocked(e)):
                reason = "차단 감지" if isinstance(e, requests.exceptions.HTTPError) else "연결 실패"
                logger.warning(f"GET {reason}, Tor 폴백 실행: {url}")
                return self._get_with_tor(url, 'GET', **kwargs)
            raise

    def post(self, url: str, data: Optional[Dict] = None, params: Optional[Dict] = None, force_tor: bool = False, timeout: int = 30, **kwargs) -> requests.Response:
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
        except (requests.exceptions.HTTPError, requests.exceptions.ConnectionError) as e:
            if self.tor_config.enabled and (isinstance(e, requests.exceptions.ConnectionError) or self._is_blocked(e)):
                reason = "차단 감지" if isinstance(e, requests.exceptions.HTTPError) else "연결 실패"
                logger.warning(f"POST {reason}, Tor 폴백 실행: {url}")
                return self._get_with_tor(url, 'POST', data=data, params=params, **kwargs)
            raise

    def _is_blocked(self, error: requests.exceptions.HTTPError) -> bool:
        """차단 여부 판단"""
        return error.response is not None and error.response.status_code in self.BLOCKED_STATUS_CODES

    def _get_with_tor(self, url: str, method: str, data: Optional[Dict] = None, params: Optional[Dict] = None, retry_count: int = 0, **kwargs) -> requests.Response:
        """Tor 프록시를 통한 요청 수행 및 실패 시 회로 갱신 재시도"""
        kwargs['proxies'] = self.tor_config.proxies
        kwargs['timeout'] = max(kwargs.get('timeout', 30), 60)
        
        # 세션 초기화 및 헤더 갱신
        self.session.cookies.clear()
        self._apply_headers(url, kwargs)
        kwargs['headers']['Sec-Fetch-Site'] = 'same-origin'
        kwargs['headers']['Sec-Fetch-Mode'] = 'navigate'

        # IP 로깅
        after_ip = self._get_current_ip(proxies=self.tor_config.proxies)
        logger.info(f"[IP 변경 확인] 원본 IP: {self.origin_ip} -> Tor IP: {after_ip} (시도 {retry_count + 1})")
        
        try:
            if method == 'GET':
                response = self.session.get(url, **kwargs)
            else:
                response = self.session.post(url, data=data, params=params, **kwargs)
            
            response.raise_for_status()
            return response

        except requests.exceptions.HTTPError as e:
            # Tor IP도 차단된 경우 (403), 회로를 갱신하고 최대 1회 재시도
            if self._is_blocked(e) and retry_count < 1:
                logger.warning(f"Tor IP({after_ip})도 차단됨. 회로 갱신 후 재시도합니다.")
                self._renew_tor_circuit()
                time.sleep(random.uniform(2.0, 5.0))
                return self._get_with_tor(url, method, data=data, params=params, retry_count=retry_count + 1, **kwargs)
            raise

    def get_text(self, url: str, encoding: str = "utf-8", **kwargs) -> str:
        """GET 요청 후 텍스트 반환"""
        response = self.get(url, **kwargs)
        response.encoding = encoding
        return response.text
