"""
Pocketbase 클라이언트
DB 작업 추상화
"""

import hashlib
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Optional, Any
import logging
import requests

from config import PocketbaseConfig

logger = logging.getLogger(__name__)
KST = timezone(timedelta(hours=9))


class PocketbaseClient:
    """
    Pocketbase REST API 클라이언트

    Collections:
    - funeral_raw: 원본 스크래핑 데이터
    - funeral_analyzed: GPT 분석 결과
    - funeral_sent: 전송 완료 기록
    - scraper_log: 실행 로그
    - scraper_metrics: 성능 메트릭
    """

    def __init__(self, config: PocketbaseConfig):
        self.base_url = config.url.rstrip('/')
        self.email = config.email
        self.password = config.password
        self.token: Optional[str] = None

    def authenticate(self) -> bool:
        """User 인증 (is_scrapper 권한 필요)"""
        try:
            response = requests.post(
                f"{self.base_url}/api/collections/users/auth-with-password",
                json={
                    "identity": self.email,
                    "password": self.password
                },
                timeout=10
            )
            response.raise_for_status()
            data = response.json()
            self.token = data.get("token")
            logger.info("Pocketbase 인증 성공")
            return True
        except Exception as e:
            logger.error(f"Pocketbase 인증 실패: {e}")
            return False

    def _headers(self) -> Dict[str, str]:
        """인증 헤더 반환"""
        headers = {"Content-Type": "application/json"}
        if self.token:
            headers["Authorization"] = self.token
        return headers

    def _request(
        self,
        method: str,
        endpoint: str,
        data: Optional[Dict] = None,
        params: Optional[Dict] = None
    ) -> Optional[Dict]:
        """API 요청"""
        try:
            url = f"{self.base_url}/api/collections/{endpoint}"
            response = requests.request(
                method,
                url,
                headers=self._headers(),
                json=data,
                params=params,
                timeout=30
            )
            response.raise_for_status()
            return response.json()
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 401:
                # 토큰 만료 - 재인증 시도
                if self.authenticate():
                    return self._request(method, endpoint, data, params)
            logger.error(f"Pocketbase 요청 실패: {e}")
            return None
        except Exception as e:
            logger.error(f"Pocketbase 요청 오류: {e}")
            return None

    # ==================== funeral_raw ====================

    def get_raw_by_district(self, district: str) -> List[Dict]:
        """구청별 원본 데이터 조회 (페이지네이션)"""
        # 인증 확인
        if not self.token:
            self.authenticate()

        items = []
        page = 1
        while True:
            result = self._request(
                "GET",
                "funeral_raw/records",
                params={
                    "filter": f'district="{district}"',
                    "sort": "-scraped_at",
                    "perPage": 500,
                    "page": page
                }
            )
            if not result or not result.get("items"):
                break
            items.extend(result["items"])
            if page >= result.get("totalPages", 1):
                break
            page += 1

        return items

    def get_raw_urls_by_district(self, district: str) -> List[str]:
        """구청별 이미 수집된 URL 목록"""
        records = self.get_raw_by_district(district)
        return [r["url"] for r in records]

    def get_raw_contents_by_district(self, district: str) -> List[str]:
        """구청별 이미 수집된 content 목록"""
        records = self.get_raw_by_district(district)
        return [r["content"] for r in records]

    def add_raw(
        self,
        district: str,
        url: str,
        content: str,
        update_count: int = 0
    ) -> Optional[Dict]:
        """원본 데이터 추가"""
        content_hash = hashlib.sha256((url + content).encode()).hexdigest()
        return self._request(
            "POST",
            "funeral_raw/records",
            data={
                "district": district,
                "url": url,
                "content": content,
                "content_hash": content_hash,
                "update_count": update_count,
                "scraped_at": datetime.now(KST).isoformat()
            }
        )

    def raw_exists(self, content: str, district: str) -> bool:
        """동일 내용 존재 여부 확인"""
        contents = self.get_raw_contents_by_district(district)
        return content in contents

    def count_same_url(self, url: str, district: str) -> int:
        """동일 URL 레코드 수 (수정 횟수 계산용)"""
        result = self._request(
            "GET",
            "funeral_raw/records",
            params={
                "filter": f'district="{district}" && url="{url}"',
                "fields": "id"
            }
        )
        return len(result.get("items", [])) if result else 0

    # ==================== funeral_analyzed ====================

    def get_analyzed_hashes(self) -> List[str]:
        """분석 완료된 content_hash 목록 (페이지네이션)"""
        # 인증 확인
        if not self.token:
            self.authenticate()

        hashes = []
        page = 1
        while True:
            result = self._request(
                "GET",
                "funeral_analyzed/records",
                params={"fields": "content_hash", "perPage": 500, "page": page}
            )
            if not result or not result.get("items"):
                break
            hashes.extend([r["content_hash"] for r in result["items"]])
            if page >= result.get("totalPages", 1):
                break
            page += 1

        return hashes

    def get_unanalyzed_raw(self) -> List[Dict]:
        """분석되지 않은 원본 데이터 조회 (페이지네이션)"""
        # 인증 확인
        if not self.token:
            self.authenticate()

        analyzed_hashes = set(self.get_analyzed_hashes())
        print(f"  [DEBUG] analyzed_hashes: {len(analyzed_hashes)}건")

        # 모든 RAW 조회 (페이지네이션)
        all_raw = []
        page = 1
        while True:
            result = self._request(
                "GET",
                "funeral_raw/records",
                params={"perPage": 500, "page": page}
            )
            if not result or not result.get("items"):
                break
            all_raw.extend(result["items"])
            if page >= result.get("totalPages", 1):
                break
            page += 1

        print(f"  [DEBUG] all_raw: {len(all_raw)}건")

        unanalyzed = [r for r in all_raw if r.get("content_hash") not in analyzed_hashes]
        print(f"  [DEBUG] unanalyzed: {len(unanalyzed)}건")

        return unanalyzed

    def analyzed_exists(self, content_hash: str) -> bool:
        """분석 결과 존재 여부 확인"""
        result = self._request(
            "GET",
            "funeral_analyzed/records",
            params={
                "filter": f'content_hash="{content_hash}"',
                "fields": "id"
            }
        )
        return bool(result and result.get("items"))

    def add_analyzed(
        self,
        raw_id: str,
        content_hash: str,
        district: str,
        url: str,
        update_count: int,
        analyzed_data: Dict[str, Any]
    ) -> Optional[Dict]:
        """분석 결과 추가 (이미 존재하면 스킵)"""
        # 이미 존재하는지 확인
        if self.analyzed_exists(content_hash):
            logger.debug(f"분석 결과 이미 존재: {content_hash[:16]}...")
            return {"skipped": True, "content_hash": content_hash}

        return self._request(
            "POST",
            "funeral_analyzed/records",
            data={
                "raw_id": raw_id,
                "content_hash": content_hash,
                "district": district,
                "url": url,
                "update_count": update_count,
                "name": analyzed_data.get("이름", ""),
                "birth_date": analyzed_data.get("생년월일", ""),
                "residence": analyzed_data.get("거주지", ""),
                "death_datetime": analyzed_data.get("사망일시", ""),
                "death_place": analyzed_data.get("사망장소", ""),
                "funeral_schedule": analyzed_data.get("장례일정", ""),
                "funeral_place": analyzed_data.get("장례장소", ""),
                "departure_datetime": analyzed_data.get("발인일시", ""),
                "cremation_datetime": analyzed_data.get("화장일시", ""),
                "analyzed_at": datetime.now(KST).isoformat()
            }
        )

    # ==================== funeral_sent ====================

    def get_sent_hashes(self) -> List[str]:
        """전송 완료된 content_hash 목록 (페이지네이션)"""
        # 인증 확인
        if not self.token:
            self.authenticate()

        hashes = []
        page = 1
        while True:
            result = self._request(
                "GET",
                "funeral_sent/records",
                params={"fields": "content_hash", "perPage": 500, "page": page}
            )
            if not result or not result.get("items"):
                break
            hashes.extend([r["content_hash"] for r in result["items"]])
            if page >= result.get("totalPages", 1):
                break
            page += 1

        return hashes

    def get_unsent_analyzed(self) -> List[Dict]:
        """전송되지 않은 분석 데이터 조회 (페이지네이션)"""
        # 인증 확인
        if not self.token:
            self.authenticate()

        sent_hashes = set(self.get_sent_hashes())
        print(f"  [DEBUG] sent_hashes: {len(sent_hashes)}건")

        # 모든 analyzed 조회 (페이지네이션)
        all_analyzed = []
        page = 1
        while True:
            result = self._request(
                "GET",
                "funeral_analyzed/records",
                params={"perPage": 500, "page": page}
            )
            if not result or not result.get("items"):
                break
            all_analyzed.extend(result["items"])
            if page >= result.get("totalPages", 1):
                break
            page += 1

        print(f"  [DEBUG] all_analyzed: {len(all_analyzed)}건")
        unsent = [r for r in all_analyzed if r.get("content_hash") not in sent_hashes]
        print(f"  [DEBUG] unsent: {len(unsent)}건")

        return unsent

    def mark_as_sent(self, content_hash: str) -> Optional[Dict]:
        """전송 완료 기록"""
        return self._request(
            "POST",
            "funeral_sent/records",
            data={
                "content_hash": content_hash,
                "sent_at": datetime.now(KST).isoformat()
            }
        )

    def delete_sent(self, record_id: str) -> bool:
        """전송 완료 레코드 삭제"""
        try:
            url = f"{self.base_url}/api/collections/funeral_sent/records/{record_id}"
            response = requests.delete(url, headers=self._headers(), timeout=10)
            response.raise_for_status()
            return True
        except Exception as e:
            logger.error(f"전송 레코드 삭제 실패 ({record_id}): {e}")
            return False

    def cleanup_orphan_sent(self) -> int:
        """고아 전송완료 레코드 정리 (analyzed에 없는 sent 삭제)"""
        if not self.token:
            self.authenticate()

        # 모든 analyzed content_hash 조회
        analyzed_hashes = set(self.get_analyzed_hashes())
        print(f"  [DEBUG] analyzed_hashes: {len(analyzed_hashes)}건")

        # 모든 sent 레코드 조회 (페이지네이션)
        all_sent = []
        page = 1
        while True:
            result = self._request(
                "GET",
                "funeral_sent/records",
                params={"perPage": 500, "page": page}
            )
            if not result or not result.get("items"):
                break
            all_sent.extend(result["items"])
            if page >= result.get("totalPages", 1):
                break
            page += 1

        print(f"  [DEBUG] all_sent: {len(all_sent)}건")

        # 고아 레코드 찾기 (analyzed에 없는 sent)
        orphans = [s for s in all_sent if s.get("content_hash") not in analyzed_hashes]
        print(f"  [DEBUG] orphan_sent: {len(orphans)}건")

        # 삭제
        deleted = 0
        for orphan in orphans:
            if self.delete_sent(orphan["id"]):
                deleted += 1
                print(f"    삭제: {orphan['content_hash'][:16]}...")

        return deleted

    def cleanup_duplicate_sent(self) -> int:
        """중복 전송완료 레코드 정리 (같은 content_hash에 대해 최신 1개만 유지)"""
        if not self.token:
            self.authenticate()

        # 모든 sent 레코드 조회 (페이지네이션)
        all_sent = []
        page = 1
        while True:
            result = self._request(
                "GET",
                "funeral_sent/records",
                params={"perPage": 500, "page": page, "sort": "-sent_at"}
            )
            if not result or not result.get("items"):
                break
            all_sent.extend(result["items"])
            if page >= result.get("totalPages", 1):
                break
            page += 1

        print(f"  [DEBUG] all_sent: {len(all_sent)}건")

        # content_hash별로 그룹화 (최신순 정렬됨)
        seen_hashes = set()
        duplicates = []
        for sent in all_sent:
            ch = sent.get("content_hash")
            if ch in seen_hashes:
                duplicates.append(sent)
            else:
                seen_hashes.add(ch)

        print(f"  [DEBUG] unique: {len(seen_hashes)}건, duplicates: {len(duplicates)}건")

        # 중복 삭제
        deleted = 0
        for dup in duplicates:
            if self.delete_sent(dup["id"]):
                deleted += 1

        print(f"  중복 삭제 완료: {deleted}건")
        return deleted

    # ==================== scraper_metrics ====================

    def save_metrics(self, metrics_dict: Dict) -> Optional[Dict]:
        """메트릭 저장"""
        return self._request(
            "POST",
            "scraper_metrics/records",
            data=metrics_dict
        )

    # ==================== scraper_log ====================

    def save_log(
        self,
        level: str,
        message: str,
        function_name: Optional[str] = None,
        error_trace: Optional[str] = None
    ) -> Optional[Dict]:
        """로그 저장"""
        return self._request(
            "POST",
            "scraper_log/records",
            data={
                "level": level,
                "message": message,
                "function_name": function_name,
                "error_trace": error_trace,
                "logged_at": datetime.now(KST).isoformat()
            }
        )
