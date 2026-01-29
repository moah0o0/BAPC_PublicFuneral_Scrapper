"""
기본 스크래퍼 클래스
모든 구청 스크래퍼의 베이스
"""

from abc import ABC, abstractmethod
from typing import List, Dict, Optional
from bs4 import BeautifulSoup
import re
import logging

from core.http_client import HttpClient

logger = logging.getLogger(__name__)


class BaseScraper(ABC):
    """
    모든 구청 스크래퍼의 추상 베이스 클래스

    서브클래스에서 구현해야 할 메서드:
    - base_url: 기본 URL (도메인)
    - list_url_template: 목록 페이지 URL 템플릿
    - list_selector: 목록 페이지에서 링크를 찾는 CSS 셀렉터
    - content_selector: 상세 페이지에서 본문을 찾는 CSS 셀렉터
    - pagination_selector: 페이지네이션 영역 CSS 셀렉터
    """

    def __init__(self, http_client: HttpClient, district_name: str):
        self.client = http_client
        self.district = district_name
        self.force_tor = False  # 기본값: Tor 강제 사용 안 함

    @property
    @abstractmethod
    def base_url(self) -> str:
        """기본 URL (도메인)"""
        pass

    @property
    @abstractmethod
    def list_url_template(self) -> str:
        """목록 페이지 URL 템플릿 (startPage={page} 포함)"""
        pass

    @property
    @abstractmethod
    def list_selector(self) -> str:
        """목록 페이지에서 링크 영역 CSS 셀렉터"""
        pass

    @property
    @abstractmethod
    def content_selector(self) -> str:
        """상세 페이지에서 본문 영역 CSS 셀렉터"""
        pass

    @property
    @abstractmethod
    def pagination_selector(self) -> str:
        """페이지네이션 영역 CSS 셀렉터"""
        pass

    @property
    def page_param_pattern(self) -> str:
        """URL에서 페이지 번호 추출 패턴"""
        return r'startPage=([0-9]{1,5})'

    @property
    def br_tag(self) -> str:
        """줄바꿈 태그 (<br/>, <br>, <br /> 등)"""
        return "<br/>"

    def get_list_url(self, page: int) -> str:
        """목록 페이지 URL 생성"""
        return self.list_url_template.format(page=page)

    def parse_urls(self, html: str) -> List[str]:
        """목록 페이지에서 상세 URL 추출"""
        soup = BeautifulSoup(html, 'html.parser')
        container = soup.select_one(self.list_selector)
        if not container:
            logger.warning(f"{self.district}: 목록 컨테이너를 찾을 수 없음")
            return []

        links = container.find_all("a", href=True)
        urls = [self.base_url + link["href"] for link in links]
        return urls

    def parse_content(self, html: str) -> str:
        """상세 페이지에서 본문 추출"""
        # 줄바꿈 태그를 실제 줄바꿈으로 변환
        html = html.replace(self.br_tag, "\n")
        soup = BeautifulSoup(html, "html.parser")
        container = soup.select_one(self.content_selector)
        if not container:
            logger.warning(f"{self.district}: 본문 컨테이너를 찾을 수 없음")
            return ""

        return container.get_text().strip()

    def get_last_page_num(self, html: str) -> int:
        """페이지네이션에서 마지막 페이지 번호 추출"""
        soup = BeautifulSoup(html, 'html.parser')
        pagination = soup.select_one(self.pagination_selector)
        if not pagination:
            return 1

        links = pagination.find_all("a", href=True)
        if not links:
            return 1

        # 마지막 링크에서 페이지 번호 추출
        last_href = links[-1].get("href", "")
        match = re.search(self.page_param_pattern, last_href)
        if match:
            return int(match.group(1))

        return 1

    def fetch_urls(self, page: int) -> List[str]:
        """목록 페이지에서 URL 목록 가져오기"""
        url = self.get_list_url(page)
        response = self.client.get(url, force_tor=self.force_tor)
        response.encoding = "utf-8"
        return self.parse_urls(response.text)

    def fetch_content(self, url: str) -> str:
        """상세 페이지에서 본문 가져오기"""
        response = self.client.get(url, force_tor=self.force_tor)
        response.encoding = "utf-8"
        return self.parse_content(response.text)

    def scrape(self, max_page: int = 1) -> List[Dict[str, str]]:
        """
        스크래핑 실행

        Args:
            max_page: 최대 페이지 수 (기본값 1)

        Returns:
            [{"url": str, "content": str}, ...]
        """
        # 첫 페이지 로드하여 전체 페이지 수 확인
        first_url = self.get_list_url(1)
        response = self.client.get(first_url, force_tor=self.force_tor)
        response.encoding = "utf-8"

        last_page = self.get_last_page_num(response.text)
        if last_page > max_page:
            last_page = max_page

        results = []
        for page in range(1, last_page + 1):
            urls = self.fetch_urls(page)
            for url in urls:
                try:
                    content = self.fetch_content(url)
                    results.append({"url": url, "content": content})
                except Exception as e:
                    logger.error(f"{self.district}: URL 처리 실패 - {url}: {e}")

        return results


class OnClickScraper(BaseScraper):
    """
    onclick 속성에서 URL을 추출하는 스크래퍼
    (SAHA 등)
    """

    def parse_urls(self, html: str) -> List[str]:
        """onclick 속성에서 URL 추출"""
        soup = BeautifulSoup(html, 'html.parser')
        container = soup.select_one(self.list_selector)
        if not container:
            return []

        links = container.find_all("a", onclick=True)
        urls = []
        for link in links:
            url = self.extract_url_from_onclick(link["onclick"])
            if url:
                urls.append(url)
        return urls

    @abstractmethod
    def extract_url_from_onclick(self, onclick: str) -> Optional[str]:
        """onclick 속성에서 URL 추출 (서브클래스에서 구현)"""
        pass

    def get_last_page_num(self, html: str) -> int:
        """goPage() 형태의 페이지네이션에서 마지막 페이지 추출"""
        soup = BeautifulSoup(html, 'html.parser')
        pagination = soup.select_one(self.pagination_selector)
        if not pagination:
            return 1

        links = pagination.find_all("a", onclick=True)
        if not links:
            return 1

        # 마지막 onclick에서 페이지 번호 추출
        last_onclick = links[-1].get("onclick", "")
        match = re.search(r'goPage\((\d+)\)', last_onclick)
        if match:
            return int(match.group(1))

        return 1


class BlogStyleScraper(BaseScraper):
    """
    블로그 형식 스크래퍼 - 목록에서 바로 content 추출
    (SEOGU 등)
    """

    @property
    def content_class(self) -> str:
        """목록 항목 내 본문 클래스"""
        return "stxt"

    def scrape(self, max_page: int = 1) -> List[Dict[str, str]]:
        """
        블로그 형식 스크래핑 - 목록에서 바로 content 추출
        """
        first_url = self.get_list_url(1)
        response = self.client.get(first_url, force_tor=self.force_tor)
        response.encoding = "utf-8"

        last_page = self.get_last_page_num(response.text)
        if last_page > max_page:
            last_page = max_page

        results = []
        for page in range(1, last_page + 1):
            url = self.get_list_url(page)
            response = self.client.get(url, force_tor=self.force_tor)
            response.encoding = "utf-8"

            items = self.parse_list_items(response.text)
            results.extend(items)

        return results

    def parse_list_items(self, html: str) -> List[Dict[str, str]]:
        """목록에서 URL과 content 동시 추출"""
        soup = BeautifulSoup(html, 'html.parser')
        container = soup.select_one(self.list_selector)
        if not container:
            logger.warning(f"{self.district}: 목록 컨테이너를 찾을 수 없음")
            return []

        results = []
        links = container.find_all("a", href=True)
        for link in links:
            url = self.base_url + "/board/" + link["href"]
            content_elem = link.find("span", self.content_class)
            content = content_elem.get_text().strip() if content_elem else ""
            if content:
                results.append({"url": url, "content": content})

        return results


class PostMethodScraper(BaseScraper):
    """
    POST 방식 스크래퍼
    (GANGSEO, YEONJE 등)
    """

    @property
    def post_url(self) -> str:
        """POST 요청 URL"""
        return self.list_url_template

    def get_post_params(self, page: int) -> Dict:
        """POST 파라미터 생성"""
        return {"page": str(page)}

    def fetch_list_html(self, page: int) -> str:
        """POST 방식으로 목록 HTML 가져오기"""
        response = self.client.post(
            self.post_url,
            data=self.get_post_params(page),
            force_tor=self.force_tor
        )
        response.encoding = "utf-8"
        return response.text

    def scrape(self, max_page: int = 1) -> List[Dict[str, str]]:
        """POST 방식 스크래핑"""
        # 첫 페이지로 전체 페이지 수 확인
        first_html = self.fetch_list_html(1)
        last_page = self.get_last_page_num(first_html)
        if last_page > max_page:
            last_page = max_page

        results = []
        for page in range(1, last_page + 1):
            html = self.fetch_list_html(page)
            urls = self.parse_urls(html)
            for url in urls:
                try:
                    content = self.fetch_content(url)
                    results.append({"url": url, "content": content})
                except Exception as e:
                    logger.error(f"{self.district}: URL 처리 실패 - {url}: {e}")

        return results
