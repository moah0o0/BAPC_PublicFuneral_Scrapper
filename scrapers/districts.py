"""
16개 구청 스크래퍼 구현
각 구청의 웹사이트 구조에 맞는 스크래퍼 클래스
"""

from typing import List, Dict, Optional
from bs4 import BeautifulSoup
import re

from scrapers.base import BaseScraper, OnClickScraper, BlogStyleScraper, PostMethodScraper
from core.http_client import HttpClient


# ==================== 표준 스크래퍼 (직접 연결) ====================

class BukguScraper(BaseScraper):
    """북구"""

    @property
    def base_url(self) -> str:
        return "https://www.bsbukgu.go.kr"

    @property
    def list_url_template(self) -> str:
        return "https://www.bsbukgu.go.kr/board/list.bsbukgu?boardId=BBS_0000244&menuCd=DOM_000000102014000000&paging=ok&startPage={page}"

    @property
    def list_selector(self) -> str:
        return "#conts > div.board-list-wrap > table > tbody"

    @property
    def content_selector(self) -> str:
        return "#conts > div.board-view-wrap > div"

    @property
    def pagination_selector(self) -> str:
        return "#conts > div.paging-wrap"


class DongguScraper(BaseScraper):
    """동구"""

    @property
    def base_url(self) -> str:
        return "https://www.bsdonggu.go.kr"

    @property
    def list_url_template(self) -> str:
        return "https://www.bsdonggu.go.kr/welfare/board/list.donggu?boardId=BBS_0000355&menuCd=DOM_000000206010000000&paging=ok&startPage={page}"

    @property
    def list_selector(self) -> str:
        return "#contents > table"

    @property
    def content_selector(self) -> str:
        return "#contents > table > tbody > tr.bbs_content_area > td"

    @property
    def pagination_selector(self) -> str:
        return "#contents > div.paging"


class DongnaeScraper(BaseScraper):
    """동래구"""

    @property
    def base_url(self) -> str:
        return "https://www.dongnae.go.kr"

    @property
    def list_url_template(self) -> str:
        return "https://www.dongnae.go.kr/board/list.dongnae?boardId=BBS_0000363&listRow=10&listCel=1&menuCd=DOM_000000509002000000&startPage={page}"

    @property
    def list_selector(self) -> str:
        return "#contents > div > table > tbody"

    @property
    def content_selector(self) -> str:
        return "#view > table > tbody"

    @property
    def pagination_selector(self) -> str:
        return "#contents > div > div.paging2"

    def parse_content(self, html: str) -> str:
        """상세 페이지에서 본문 추출 (레거시 방식 - 조회수 셀 삭제)"""
        html = html.replace("<br/>", "\n")
        soup = BeautifulSoup(html, "html.parser")

        # 조회수 셀 삭제 (레거시 방식)
        view_count_cell = soup.select_one("#view > table > tbody > tr:nth-child(2) > td:nth-child(6)")
        if view_count_cell:
            view_count_cell.decompose()

        container = soup.select_one(self.content_selector)
        if not container:
            return ""

        return container.get_text().strip()


class GangseoScraper(PostMethodScraper):
    """강서구 (POST 방식)"""

    @property
    def base_url(self) -> str:
        return "https://www.bsgangseo.go.kr"

    @property
    def list_url_template(self) -> str:
        return "https://www.bsgangseo.go.kr/welfare/board/post/list.do?bcIdx=567&mid=0604030000"

    @property
    def list_selector(self) -> str:
        return ""  # regex로 추출

    @property
    def content_selector(self) -> str:
        return "div.view_cont"

    @property
    def pagination_selector(self) -> str:
        return ""  # regex로 추출

    def get_post_params(self, page: int) -> Dict:
        return {
            "page": str(page),
            "cancelUrl": "/welfare/board/post/list.do?bcIdx=567&mid=0604030000",
            "searchType": "0",
            "searchTxt": ""
        }

    def parse_urls(self, html: str) -> List[str]:
        """regex로 data-req-get-p-idx 추출"""
        idxs = re.findall(r'data-req-get-p-idx="(\d+)"', html)
        return [
            f"{self.base_url}/welfare/board/post/view.do?bcIdx=567&mid=0604030000&&idx={idx}"
            for idx in idxs
        ]

    def get_last_page_num(self, html: str) -> int:
        """goPage() 형태에서 마지막 페이지 추출"""
        numbers = re.findall(r'goPage\((\d+)\)', html)
        if numbers:
            return max(map(int, numbers))
        return 1


class GijangScraper(BaseScraper):
    """기장군 (레거시 방식 - 테이블 병합 처리)"""

    @property
    def base_url(self) -> str:
        return "https://www.gijang.go.kr"

    @property
    def list_url_template(self) -> str:
        return "https://www.gijang.go.kr/board/list.gijang?boardId=BBS_0000157&menuCd=DOM_000000103008001000&paging=ok&startPage={page}"

    @property
    def list_selector(self) -> str:
        return "#conts > div > table"

    @property
    def content_selector(self) -> str:
        return "#conts > div > table > tbody"

    @property
    def pagination_selector(self) -> str:
        return "#conts > div > div.pageing"

    def parse_content(self, html: str) -> str:
        """레거시 방식 - 테이블 병합 처리 후 key:value 포맷"""
        html = html.replace("<br/>", "\n")
        soup = BeautifulSoup(html, "html.parser")
        container = soup.select_one(self.content_selector)
        if not container:
            return ""

        tables = container.find_all("table")
        if not tables:
            return container.get_text().strip()

        # 마지막 테이블 사용
        table = tables[-1]
        trs = table.select("tr")

        if len(trs) not in [2, 4]:
            return container.get_text().strip()

        # 행별 셀 추출
        rr = []
        for row in trs:
            tds = row.select("td")
            rr.append([td for td in tds])

        # 4행인 경우 병합 처리 (rowspan 있는 셀 처리)
        if len(rr) == 4:
            # rowspan 없는 셀 인덱스 찾기
            a = [rr[0].index(i) for i in rr[0] if "rowspan" not in i.attrs]
            if len(a) == 1:
                idx = a[0]
                rr[0].insert(idx, rr[1][0])
                rr.remove(rr[1])
                rr[1].insert(idx, rr[2][0])
                rr.remove(rr[2])

        # key:value 포맷으로 변환
        result = ""
        for i, key_cell in enumerate(rr[0]):
            key = key_cell.get_text().strip()
            value = rr[1][i].get_text().strip() if i < len(rr[1]) else ""
            result += f"\n{key}:{value}"

        return result.strip()


class NamguScraper(BaseScraper):
    """남구 (레거시 방식 - 특수처리 없음)"""

    @property
    def base_url(self) -> str:
        return "https://www.bsnamgu.go.kr"

    @property
    def list_url_template(self) -> str:
        return "https://www.bsnamgu.go.kr/board/list.namgu?boardId=BBS_0000315&menuCd=DOM_000000105001009000&paging=ok&startPage={page}"

    @property
    def list_selector(self) -> str:
        return "#conts > table > tbody"

    @property
    def content_selector(self) -> str:
        return "#conts > div > table > tbody"

    @property
    def pagination_selector(self) -> str:
        return "#conts > div.paging"


class SeoguScraper(BlogStyleScraper):
    """서구 (블로그 형식 - 목록에서 content 추출)"""

    @property
    def base_url(self) -> str:
        return "https://www.bsseogu.go.kr"

    @property
    def list_url_template(self) -> str:
        return "https://www.bsseogu.go.kr/board/list.bsseogu?boardId=BBS_0000214&menuCd=DOM_000000103001020000&orderBy=REGISTER_DATE%20DESC&paging=ok&startPage={page}"

    @property
    def list_selector(self) -> str:
        return "#content > div.content-inner > div.content-inner > div.bloglist-wrap > ul"

    @property
    def content_selector(self) -> str:
        return ""  # 목록에서 바로 추출하므로 불필요

    @property
    def pagination_selector(self) -> str:
        return "#content > div.content-inner > div.content-inner > div.paging-wrap2"

    @property
    def content_class(self) -> str:
        return "stxt"


class SuyeongScraper(BaseScraper):
    """수영구"""

    @property
    def base_url(self) -> str:
        return "https://www.suyeong.go.kr"

    @property
    def list_url_template(self) -> str:
        return "https://www.suyeong.go.kr/city/board/list.suyeong?boardId=BBS_0000304&menuCd=DOM_000000103001015000&paging=ok&startPage={page}"

    @property
    def list_selector(self) -> str:
        return "#con_area > table > tbody"

    @property
    def content_selector(self) -> str:
        return "#con_area > div.bbs_vtype > div"

    @property
    def pagination_selector(self) -> str:
        return "#con_area > div.page"

    @property
    def br_tag(self) -> str:
        return "<br>"


class YeongdoguScraper(BaseScraper):
    """영도구 (이미지 OCR 방식)"""

    # Naver CLOVA OCR API
    OCR_API_URL = "https://o0v9dnau5o.apigw.ntruss.com/custom/v1/29701/6f0df1e6ba1e4a4de121b34920303cfafd6f1f077cf4bd15dbf6bcf12e507509/general"
    OCR_API_SECRET = "dFVFV2xvbHBJRHVBREd0YWtvRWJMc3JXUnFRSVNuZWw="

    @property
    def base_url(self) -> str:
        return "https://www.yeongdo.go.kr"

    @property
    def list_url_template(self) -> str:
        return "https://www.yeongdo.go.kr/02418/02419/04252.web?gcode=1312&cpage={page}"

    @property
    def list_selector(self) -> str:
        return "ul.lst1"

    @property
    def content_selector(self) -> str:
        return "#body_content > div > div.bbs1view1 > div.attach1 > ul > li > a.b1.download"

    @property
    def pagination_selector(self) -> str:
        return "#listForm > div:nth-child(7) > div"

    @property
    def page_param_pattern(self) -> str:
        return r'cpage=([0-9]{1,5})'

    def parse_urls(self, html: str) -> List[str]:
        """목록에서 URL 추출"""
        soup = BeautifulSoup(html, 'html.parser')
        container = soup.find("ul", "lst1")
        if not container:
            return []

        links = container.find_all("a", href=True)
        return [
            "https://www.yeongdo.go.kr/02418/02419/04252.web" + link["href"]
            for link in links
        ]

    def fetch_content(self, url: str) -> Optional[str]:
        """이미지 다운로드 후 OCR로 텍스트 추출"""
        import hashlib
        import json
        import uuid
        import time
        import tempfile
        import os

        try:
            # 상세 페이지에서 이미지 URL 추출
            response = self.client.get(url, force_tor=self.force_tor)
            response.encoding = "utf-8"
            soup = BeautifulSoup(response.text.replace("<br/>", "\n"), "html.parser")

            img_link = soup.select_one(self.content_selector)
            if not img_link or 'href' not in img_link.attrs:
                return None

            img_url = self.base_url + img_link['href']

            # 이미지 다운로드
            img_response = self.client.get(img_url, force_tor=self.force_tor)
            img_content = img_response.content

            # 임시 파일로 저장
            url_hash = hashlib.sha256(url.encode('utf-8')).hexdigest()
            tmp_path = os.path.join(tempfile.gettempdir(), f"{url_hash}.jpg")

            with open(tmp_path, "wb") as f:
                f.write(img_content)

            # OCR API 호출
            request_json = {
                'images': [{'format': 'jpg', 'name': f'{url_hash}.jpg'}],
                'requestId': str(uuid.uuid4()),
                'version': 'V1',
                'timestamp': int(round(time.time() * 1000)),
                'enableTableDetection': True
            }

            payload = {'message': json.dumps(request_json).encode('UTF-8')}

            with open(tmp_path, 'rb') as f:
                files = [('file', f)]
                headers = {'X-OCR-SECRET': self.OCR_API_SECRET}

                import requests
                ocr_response = requests.post(
                    self.OCR_API_URL,
                    headers=headers,
                    data=payload,
                    files=files,
                    timeout=30
                )

            # 임시 파일 삭제
            os.remove(tmp_path)

            # OCR 결과 파싱
            ocr_data = ocr_response.json()
            result = ""

            if 'images' in ocr_data and ocr_data['images']:
                tables = ocr_data['images'][0].get('tables', [])
                if tables:
                    for cell in tables[0].get('cells', []):
                        for line in cell.get('cellTextLines', []):
                            for word in line.get('cellWords', []):
                                result += " " + word.get('inferText', '')
                            result += "\n"

            return result.strip() if result else None

        except Exception as e:
            import logging
            logging.getLogger(__name__).error(f"영도구 OCR 처리 실패: {url} - {e}")
            return None


class YeonjeScraper(PostMethodScraper):
    """연제구 (POST + onclick 방식)"""

    @property
    def base_url(self) -> str:
        return "https://www.yeonje.go.kr"

    @property
    def list_url_template(self) -> str:
        return "https://www.yeonje.go.kr/portal/bbs/list.do?ptIdx=234&mId=0206100000"

    @property
    def list_selector(self) -> str:
        return "table.bod_list"

    @property
    def content_selector(self) -> str:
        return "#conts > div > div.bod_view > div.view_cont"

    @property
    def pagination_selector(self) -> str:
        return "div.bod_page"

    @property
    def br_tag(self) -> str:
        return "<br>"

    def parse_urls(self, html: str) -> List[str]:
        """onclick에서 URL 추출"""
        soup = BeautifulSoup(html, 'html.parser')
        table = soup.find("table", "bod_list")
        if not table:
            return []

        links = table.find_all("a", href=True)
        urls = []
        for link in links:
            onclick = link.get("onclick", "")
            if "goTo.view(" in onclick:
                # goTo.view('','bIdx','ptIdx','mId');
                cleaned = onclick.replace("goTo.view(", "").replace("'", "").replace(")", "")
                cleaned = cleaned.replace("return false;", "").replace(";", "").replace(" ", "")
                parts = cleaned.split(",")
                if len(parts) >= 4:
                    bIdx, ptIdx, mId = parts[1], parts[2], parts[3]
                    url = f"/portal/bbs/view.do?bIdx={bIdx}&ptIdx={ptIdx}&mId={mId}"
                    urls.append(self.base_url + url)
        return urls

    def get_last_page_num(self, html: str) -> int:
        """goPage() 형태에서 마지막 페이지 추출"""
        soup = BeautifulSoup(html, 'html.parser')
        pagination = soup.find("div", "bod_page")
        if not pagination:
            return 1

        links = pagination.find_all("a", onclick=True)
        if not links:
            return 1

        last_onclick = links[-1].get("onclick", "")
        match = re.search(r'goPage\((\d+)\)', last_onclick)
        if match:
            return int(match.group(1))
        return 1


# ==================== onclick 스크래퍼 ====================

class SahaScraper(OnClickScraper):
    """사하구 (onclick 방식)"""

    @property
    def base_url(self) -> str:
        return "https://www.saha.go.kr"

    @property
    def list_url_template(self) -> str:
        return "https://www.saha.go.kr/portal/bbs/list.do?ptIdx=737&mId=0505050000&page={page}"

    @property
    def list_selector(self) -> str:
        return "table.tableSt_list"

    @property
    def content_selector(self) -> str:
        return "div.cont_box"

    @property
    def pagination_selector(self) -> str:
        return "div.box_page"

    @property
    def br_tag(self) -> str:
        return "<br />"

    def extract_url_from_onclick(self, onclick: str) -> Optional[str]:
        """boardView() 형태에서 URL 추출"""
        # boardView('value1','value2','value3','bIdx','ptIdx','mId');
        cleaned = onclick.replace("boardView(", "").replace("'", "").replace(")", "")
        cleaned = cleaned.replace("return false;", "").replace(";", "").replace(" ", "")
        parts = cleaned.split(",")
        if len(parts) >= 6:
            mId, bIdx, ptIdx = parts[5], parts[3], parts[4]
            return f"/portal/bbs/view.do?mId={mId}&bIdx={bIdx}&ptIdx={ptIdx}"
        return None

    def parse_urls(self, html: str) -> List[str]:
        """onclick에서 URL 추출"""
        soup = BeautifulSoup(html, 'html.parser')
        table = soup.find("table", "tableSt_list")
        if not table:
            return []

        links = table.find_all("a", onclick=True)
        urls = []
        for link in links:
            path = self.extract_url_from_onclick(link["onclick"])
            if path:
                urls.append(self.base_url + path)
        return urls


# ==================== Tor 폴백 스크래퍼 ====================

class HaeundaeScraper(BaseScraper):
    """해운대구 (Tor 폴백)"""

    def __init__(self, http_client: HttpClient, district_name: str = "해운대구"):
        super().__init__(http_client, district_name)
        # force_tor는 False로 유지 - HttpClient가 자동으로 폴백 처리

    @property
    def base_url(self) -> str:
        return "https://www.haeundae.go.kr"

    @property
    def list_url_template(self) -> str:
        return "https://www.haeundae.go.kr/edu/board/list.do?boardId=BBS_0000465&menuCd=DOM_000000104001009000&paging=ok&startPage={page}"

    @property
    def list_selector(self) -> str:
        return "#font_size > div.table.respond > table"

    @property
    def content_selector(self) -> str:
        return "#font_size > article > table > tbody"

    @property
    def pagination_selector(self) -> str:
        return "#font_size > div.boardPage"

    @property
    def br_tag(self) -> str:
        return "<br />"


class JinguScraper(BaseScraper):
    """부산진구 (Tor 폴백)"""

    @property
    def base_url(self) -> str:
        return "https://www.busanjin.go.kr"

    @property
    def list_url_template(self) -> str:
        return "https://www.busanjin.go.kr/board/list.busanjin?boardId=BBS_0000260&menuCd=DOM_000000107005004000&paging=ok&startPage={page}"

    @property
    def list_selector(self) -> str:
        return "#sub_contentnw > div > div.board-list > table > tbody"

    @property
    def content_selector(self) -> str:
        return "#sub_contentnw > div > div.board-view > div > div.substan"

    @property
    def pagination_selector(self) -> str:
        return "#sub_contentnw > div > ul"


class GeumjeongScraper(BaseScraper):
    """금정구 (Tor 폴백)"""

    @property
    def base_url(self) -> str:
        return "https://www.geumjeong.go.kr"

    @property
    def list_url_template(self) -> str:
        return "https://www.geumjeong.go.kr/board/list.geumj?boardId=BBS_0000372&menuCd=DOM_000000126020001000&startPage={page}"

    @property
    def list_selector(self) -> str:
        return "#print > table > tbody"

    @property
    def content_selector(self) -> str:
        return "#print > table > tbody > tr:nth-child(3) > td"

    @property
    def pagination_selector(self) -> str:
        return "#print > div.page"

    def get_last_page_num(self, html: str) -> int:
        """금정구 특수 페이지네이션 처리"""
        soup = BeautifulSoup(html, 'html.parser')
        pagination = soup.select_one(self.pagination_selector)
        if not pagination:
            return 1

        text = pagination.get_text().strip()
        if text == "1":
            return 1

        links = pagination.find_all("a", href=True)
        if not links:
            return 1

        last_href = links[-1].get("href", "")
        match = re.search(self.page_param_pattern, last_href)
        if match:
            return int(match.group(1))

        return 1


class SasangScraper(BaseScraper):
    """사상구 (Tor 폴백)"""

    @property
    def base_url(self) -> str:
        return "https://www.sasang.go.kr"

    @property
    def list_url_template(self) -> str:
        return "https://www.sasang.go.kr/board/list.sasang?boardId=BBS_0000268&menuCd=DOM_000000103009000000&startPage={page}"

    @property
    def list_selector(self) -> str:
        return "#content > table"

    @property
    def content_selector(self) -> str:
        return "#content > div.bbs_vtype > div"

    @property
    def pagination_selector(self) -> str:
        return "#content > div.page"

    @property
    def br_tag(self) -> str:
        return "<br />"


class JungguScraper(BaseScraper):
    """중구 (Tor 폴백)"""

    @property
    def base_url(self) -> str:
        return "https://www.bsjunggu.go.kr"

    @property
    def list_url_template(self) -> str:
        return "https://www.bsjunggu.go.kr/board/list.junggu?boardId=BBS_0000184&menuCd=DOM_000000401006000000&paging=ok&startPage={page}"

    @property
    def list_selector(self) -> str:
        return "#content > table"

    @property
    def content_selector(self) -> str:
        return "#content > div.bbs_vtype > div"

    @property
    def pagination_selector(self) -> str:
        return "#content > div.page"

    @property
    def br_tag(self) -> str:
        return "<br />"


# ==================== 스크래퍼 팩토리 ====================

SCRAPER_CLASSES = {
    "BUKGU": BukguScraper,
    "DONGGU": DongguScraper,
    "DONGNAE": DongnaeScraper,
    "GANGSEO": GangseoScraper,
    "GEUMJEONG": GeumjeongScraper,
    "GIJANG": GijangScraper,
    "HAEUNDAE": HaeundaeScraper,
    "JINGU": JinguScraper,
    "JUNGGU": JungguScraper,
    "NAMGU": NamguScraper,
    "SAHA": SahaScraper,
    "SASANG": SasangScraper,
    "SEOGU": SeoguScraper,
    "SUYEONG": SuyeongScraper,
    "YEONGDOGU": YeongdoguScraper,
    "YEONJE": YeonjeScraper,
}

DISTRICT_KOREAN_NAMES = {
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
    "YEONJE": "연제구",
}


def create_scraper(district_code: str, http_client: HttpClient) -> BaseScraper:
    """
    구청 코드로 스크래퍼 인스턴스 생성

    Args:
        district_code: 구청 코드 (예: "BUKGU", "HAEUNDAE")
        http_client: HTTP 클라이언트

    Returns:
        BaseScraper 인스턴스
    """
    scraper_class = SCRAPER_CLASSES.get(district_code)
    if not scraper_class:
        raise ValueError(f"Unknown district code: {district_code}")

    korean_name = DISTRICT_KOREAN_NAMES.get(district_code, district_code)
    return scraper_class(http_client, korean_name)


def get_all_scrapers(http_client: HttpClient) -> List[BaseScraper]:
    """모든 구청 스크래퍼 인스턴스 생성"""
    return [create_scraper(code, http_client) for code in SCRAPER_CLASSES.keys()]
