"""
Pocketbase 클라이언트
DB 작업 추상화
"""

import hashlib
import re
from datetime import datetime, timezone, timedelta
from typing import Callable, Dict, List, Optional, Any
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
        self._on_error: Optional[Callable[[str, str], None]] = None
        self._notified_errors: set = set()  # 중복 알림 방지

    def set_error_callback(self, callback: Callable[[str, str], None]):
        """에러 발생 시 호출할 콜백 설정 (텔레그램 등)
        callback(endpoint, error_message)
        """
        self._on_error = callback

    def _notify_error(self, endpoint: str, error_msg: str):
        """에러 콜백 호출 (동일 endpoint는 한 번만)"""
        if self._on_error and endpoint not in self._notified_errors:
            self._notified_errors.add(endpoint)
            try:
                self._on_error(endpoint, error_msg)
            except Exception:
                pass  # 콜백 실패는 무시

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
            if not self.token:
                logger.error(f"Pocketbase 인증 응답에 token 없음. 응답 키: {list(data.keys())}")
                return False
            logger.info(f"Pocketbase 인증 성공 (token: {self.token[:20]}...)")
            return True
        except Exception as e:
            logger.error(f"Pocketbase 인증 실패: {e}")
            return False

    def _headers(self) -> Dict[str, str]:
        headers = {"Content-Type": "application/json"}
        if self.token:
            headers["Authorization"] = f"Bearer {self.token}"   # ✅ 수정
        return headers

    def _request(
        self,
        method: str,
        endpoint: str,
        data: Optional[Dict] = None,
        params: Optional[Dict] = None,
        _retried: bool = False
    ) -> Optional[Dict]:
        """API 요청"""
        if not self.token:
            if not self.authenticate():
                logger.error("인증 실패로 요청 중단")
                return None
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
            # 응답 본문 로깅 (디버깅용)
            response_body = None
            try:
                response_body = e.response.json()
            except Exception:
                response_body = e.response.text[:500] if e.response.text else None

            # API rule 실패 감지: data가 비어있으면 필드 검증이 아닌 권한 문제
            is_auth_issue = e.response.status_code in (401, 403)
            is_api_rule_failure = (
                e.response.status_code == 400
                and isinstance(response_body, dict)
                and not response_body.get("data")  # 빈 data = API rule 실패
            )

            if not _retried and (is_auth_issue or is_api_rule_failure):
                logger.warning(f"인증/API rule 오류 감지 (HTTP {e.response.status_code}), 재인증 시도...")
                self.token = None  # 토큰 강제 초기화
                if self.authenticate():
                    return self._request(method, endpoint, data, params, _retried=True)

            error_detail = f"HTTP {e.response.status_code} | 응답: {response_body}"
            self._notify_error(endpoint, error_detail)
            logger.error(f"Pocketbase 요청 실패: {e} | 응답: {response_body} | 요청 데이터 키: {list(data.keys()) if data else None}")
            return None
        except Exception as e:
            self._notify_error(endpoint, str(e))
            logger.error(f"Pocketbase 요청 오류: {e}")
            return None

    # ==================== funeral_raw ====================

    def get_raw_by_district(self, district: str) -> List[Dict]:
        """구청별 원본 데이터 조회 (페이지네이션, fail-closed).

        조회 실패 시 예외를 던진다. 과거에는 실패 시 빈/부분 목록을 반환해
        이미 수집한 글을 '새 글'로 오판 → 재저장 → 재분석 → 재발송하는
        fail-open 결함이 있었다 (2026-11-03, 11-05 수영구 옛 부고 재발송).
        """
        if not self.token:
            self.authenticate()

        items = self._fetch_all_pages(
            "funeral_raw/records",
            {"filter": f'district="{district}"', "sort": "-scraped_at"}
        )
        if items is None:
            raise RuntimeError(f"{district} 기존 RAW 조회 실패(fail-closed) - 중복 저장 방지를 위해 이번 회차 건너뜀")
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
        if result is None:
            # fail-closed: 0으로 오인하면 '수정' 횟수가 틀어짐 → 해당 구 이번 회차 건너뜀
            raise RuntimeError(f"{district} 동일 URL 수 조회 실패(fail-closed)")
        return len(result.get("items", []))

    # ==================== funeral_analyzed ====================

    def get_analyzed_hashes(self) -> List[str]:
        """분석 완료된 content_hash 목록 (fail-closed: 조회 실패 시 예외)"""
        if not self.token:
            self.authenticate()
        items = self._fetch_all_pages("funeral_analyzed/records", {"fields": "content_hash"})
        if items is None:
            raise RuntimeError("분석본 해시 목록 조회 실패(fail-closed)")
        return [r["content_hash"] for r in items]

    def _fetch_all_pages(self, endpoint: str, params: Optional[Dict] = None) -> Optional[List[Dict]]:
        """페이지네이션 전체 조회 (fail-closed).

        요청이 한 번이라도 실패하면 None을 반환한다.
        호출부는 None을 '조회 실패'로 간주하고, 절대 '데이터 없음'으로 오인하지 말 것.
        (빈 컬렉션은 [] 로 정상 반환된다)
        """
        params = dict(params or {})
        params.setdefault("perPage", 500)
        items: List[Dict] = []
        page = 1
        while True:
            params["page"] = page
            result = self._request("GET", endpoint, params=params)
            if result is None:  # 요청 실패 → fail-closed
                logger.error(f"전체 조회 실패(fail-closed): {endpoint} page={page}")
                return None
            items.extend(result.get("items", []))
            if page >= result.get("totalPages", 1):
                break
            page += 1
        return items

    def get_unanalyzed_raw(self) -> Optional[List[Dict]]:
        """분석되지 않은 원본 데이터 조회 (fail-closed).

        조회 실패 시 None을 반환한다 → 호출부는 분석을 건너뛰어 재분석을 방지한다.
        """
        # 인증 확인
        if not self.token:
            self.authenticate()

        # 분석 완료된 content_hash (fail-closed)
        analyzed_records = self._fetch_all_pages(
            "funeral_analyzed/records", {"fields": "content_hash"}
        )
        if analyzed_records is None:
            logger.error("분석본 해시 조회 실패 - 재분석 방지를 위해 분석 단계 건너뜀")
            return None
        analyzed_hashes = {r.get("content_hash") for r in analyzed_records}
        print(f"  [DEBUG] analyzed_hashes: {len(analyzed_hashes)}건")

        # 모든 RAW 조회 (fail-closed)
        all_raw = self._fetch_all_pages("funeral_raw/records")
        if all_raw is None:
            logger.error("RAW 조회 실패 - 재분석 방지를 위해 분석 단계 건너뜀")
            return None
        print(f"  [DEBUG] all_raw: {len(all_raw)}건")

        unanalyzed = [r for r in all_raw if r.get("content_hash") not in analyzed_hashes]
        print(f"  [DEBUG] unanalyzed: {len(unanalyzed)}건")

        return unanalyzed

    def analyzed_exists(self, content_hash: str) -> Optional[bool]:
        """분석 결과 존재 여부 확인. 조회 실패 시 None (호출부는 '없음'으로 오인하지 말 것)"""
        result = self._request(
            "GET",
            "funeral_analyzed/records",
            params={
                "filter": f'content_hash="{content_hash}"',
                "fields": "id"
            }
        )
        if result is None:
            return None
        return bool(result.get("items"))

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
        # 이미 존재하는지 확인 (조회 실패 시 저장하지 않음 → 다음 회차 재시도)
        exists = self.analyzed_exists(content_hash)
        if exists is None:
            raise RuntimeError(f"분석본 존재 확인 실패(fail-closed): {content_hash[:16]}...")
        if exists:
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
                "is_sent": False,
                "analyzed_at": datetime.now(KST).isoformat()
            }
        )

    # ==================== funeral_sent ====================

    def get_sent_hashes(self) -> List[str]:
        """전송 완료된 content_hash 목록 (fail-closed: 조회 실패 시 예외)"""
        if not self.token:
            self.authenticate()
        items = self._fetch_all_pages("funeral_sent/records", {"fields": "content_hash"})
        if items is None:
            raise RuntimeError("전송 기록 해시 목록 조회 실패(fail-closed)")
        return [r["content_hash"] for r in items]

    def get_unsent_analyzed(self) -> Optional[List[Dict]]:
        """미전송 분석 데이터 조회 (is_sent=false 서버측 필터, fail-closed).

        전송 여부의 단일 진실원천(source of truth)은 funeral_analyzed.is_sent 이다.
        조회 실패 시 None을 반환한다 → 호출부는 전송을 건너뛰어 중복 발송을 방지한다.
        (과거: 전체 sent 목록을 내려받아 비교 → 조회 실패 시 '전부 미전송'으로
         오판하여 대량 재발송하는 fail-open 결함이 있었음)
        """
        # 인증 확인
        if not self.token:
            self.authenticate()

        unsent = self._fetch_all_pages(
            "funeral_analyzed/records", {"filter": "is_sent=false"}
        )
        if unsent is None:
            logger.error("미전송 목록 조회 실패 - 중복 발송 방지를 위해 전송 단계 건너뜀")
            return None
        print(f"  [DEBUG] unsent(is_sent=false): {len(unsent)}건")
        return unsent

    @staticmethod
    def _normalize_content(text: str) -> str:
        """본문 비교용 정규화 (공백·nbsp 차이 무시)"""
        return re.sub(r"\s+", "", (text or "").replace("\xa0", " "))

    def find_already_sent_same_notice(self, item: Dict) -> Optional[bool]:
        """같은 구청에서 '원문 본문이 같은' 부고가 이미 발송됐는지 확인 (URL/해시 무관).

        구청 사이트가 같은 글을 매번 다른 URL로 노출하면(예: 수영구는
        /board/, /library/board/, /lll/board/ ... 처럼 경로가 계속 바뀜)
        content_hash(=url+본문)가 달라져 옛 부고가 '새 부고'로 보인다.
        GPT 분석 결과는 회차마다 표기가 달라지므로 비교 기준으로 쓰지 않고,
        결정적인 원문 본문으로 비교한다.

        반환: True(이미 발송) / False(없음) / None(조회 실패 → 호출부는 발송 보류)
        """
        raw_id = item.get("raw_id")
        district = item.get("district", "")
        if not raw_id:
            return False

        raw = self._request("GET", f"funeral_raw/records/{raw_id}")
        if raw is None:
            return None
        target = self._normalize_content(raw.get("content", ""))
        if not target:
            return False

        def esc(v: str) -> str:
            return str(v or "").replace("\\", "\\\\").replace('"', '\\"')

        others = self._fetch_all_pages(
            "funeral_raw/records",
            {"filter": f'district="{esc(district)}"', "fields": "id,content_hash,content"}
        )
        if others is None:
            return None
        same_hashes = sorted({
            o.get("content_hash") for o in others
            if o.get("id") != raw_id
            and o.get("content_hash")
            and o.get("content_hash") != item.get("content_hash")
            and self._normalize_content(o.get("content", "")) == target
        })
        if not same_hashes:
            return False

        # 같은 본문의 다른 해시 중 이미 발송된 분석본이 있는지 (필터 길이 제한 대비 분할 조회)
        for i in range(0, len(same_hashes), 20):
            chunk = same_hashes[i:i + 20]
            cond = " || ".join(f'content_hash="{h}"' for h in chunk)
            result = self._request(
                "GET",
                "funeral_analyzed/records",
                params={"filter": f"is_sent=true && ({cond})", "fields": "id", "perPage": 1}
            )
            if result is None:
                return None
            if result.get("items"):
                return True
        return False

    def mark_analyzed_sent(self, analyzed_id: str) -> Optional[Dict]:
        """분석 레코드를 전송완료로 표시 (is_sent=true). 전송 dedup의 단일 진실원천."""
        return self._request(
            "PATCH",
            f"funeral_analyzed/records/{analyzed_id}",
            data={"is_sent": True}
        )

    def mark_analyzed_sent_by_hash(self, content_hash: str) -> int:
        """content_hash로 분석 레코드를 찾아 전송완료(is_sent=true) 표시.
        반환: 갱신된 레코드 수 (마이그레이션 등 id를 모를 때 사용)
        """
        result = self._request(
            "GET",
            "funeral_analyzed/records",
            params={"filter": f'content_hash="{content_hash}"', "fields": "id"}
        )
        if result is None:
            raise RuntimeError(f"분석본 조회 실패(fail-closed): {content_hash[:16]}...")
        updated = 0
        for rec in result.get("items", []):
            if self.mark_analyzed_sent(rec["id"]):
                updated += 1
        return updated

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
        """고아 전송완료 레코드 정리 (analyzed에 없는 sent 삭제)

        fail-closed: 어느 조회든 실패하면 아무것도 지우지 않고 예외.
        (과거: analyzed 조회 실패 시 빈 목록 → 모든 sent를 '고아'로 오판해 전부 삭제할 위험)
        """
        if not self.token:
            self.authenticate()

        analyzed_hashes = set(self.get_analyzed_hashes())
        print(f"  [DEBUG] analyzed_hashes: {len(analyzed_hashes)}건")

        all_sent = self._fetch_all_pages("funeral_sent/records")
        if all_sent is None:
            raise RuntimeError("전송 기록 조회 실패(fail-closed) - 정리 중단, 삭제 0건")
        print(f"  [DEBUG] all_sent: {len(all_sent)}건")

        if all_sent and not analyzed_hashes:
            raise RuntimeError("분석본 0건인데 전송 기록이 존재 - 비정상 상태로 판단해 정리 중단")

        orphans = [s for s in all_sent if s.get("content_hash") not in analyzed_hashes]
        print(f"  [DEBUG] orphan_sent: {len(orphans)}건")

        deleted = 0
        for orphan in orphans:
            if self.delete_sent(orphan["id"]):
                deleted += 1
                print(f"    삭제: {orphan['content_hash'][:16]}...")

        return deleted

    def cleanup_duplicate_sent(self) -> int:
        """중복 전송완료 레코드 정리 (같은 content_hash에 대해 최신 1개만 유지)

        fail-closed: 조회 실패(부분 목록 포함) 시 아무것도 지우지 않고 예외.
        """
        if not self.token:
            self.authenticate()

        all_sent = self._fetch_all_pages("funeral_sent/records", {"sort": "-sent_at"})
        if all_sent is None:
            raise RuntimeError("전송 기록 조회 실패(fail-closed) - 정리 중단, 삭제 0건")
        print(f"  [DEBUG] all_sent: {len(all_sent)}건")

        seen_hashes = set()
        duplicates = []
        for sent in all_sent:
            ch = sent.get("content_hash")
            if ch in seen_hashes:
                duplicates.append(sent)
            else:
                seen_hashes.add(ch)

        print(f"  [DEBUG] unique: {len(seen_hashes)}건, duplicates: {len(duplicates)}건")

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
        if error_trace and len(error_trace) > 5000:
            error_trace = error_trace[:4990] + "\n...(잘림)"
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



    
