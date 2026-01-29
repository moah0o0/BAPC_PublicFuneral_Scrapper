"""
파이프라인 모듈
3단계 파이프라인: 수집 → 분석 → 전송
"""

import traceback
import logging
from typing import List, Dict, Optional

from config import Config, DISTRICT_NAMES_ENG_TO_KOR
from core.http_client import HttpClient
from services.pocketbase import PocketbaseClient
from services.telegram import TelegramService
from services.gpt_analyzer import GPTAnalyzer, clean_analyzed_data
from scrapers.districts import SCRAPER_CLASSES, create_scraper
from utils.logger import ScraperLogger
from utils.metrics import MetricsCollector, get_collector

logger = logging.getLogger(__name__)


class Pipeline:
    """
    부고 스크래핑 파이프라인

    3단계:
    1. RAW 데이터 수집 (16개 구청 스크래핑)
    2. GPT 분석 (수집된 데이터 구조화)
    3. 텔레그램 전송 (분석 결과 알림)
    """

    def __init__(
        self,
        http_client: HttpClient,
        db: PocketbaseClient,
        telegram: TelegramService,
        gpt: GPTAnalyzer,
        config: Config,
        scraper_logger: Optional[ScraperLogger] = None
    ):
        self.http_client = http_client
        self.db = db
        self.telegram = telegram
        self.gpt = gpt
        self.config = config
        self.logger = scraper_logger
        self.metrics = get_collector()

    def run(self, skip_raw: bool = False):
        """파이프라인 전체 실행"""
        self.metrics.start_pipeline()

        try:
            self._log_general("서버 가동 시작했습니다.")

            # 테스트 모드 알림
            from config import TELEGRAM_TEST_MODE
            if TELEGRAM_TEST_MODE:
                self._log_general("⚠️ [TEST MODE] 모든 메시지가 GENERAL_CHANNEL로 전송됩니다.")

            # 1단계: RAW 데이터 수집
            if skip_raw:
                self._log_general("⏭️ RAW 수집 건너뜀 (--skip-raw)")
            else:
                self._log_general("START_1/3. RAW 수집 시작합니다.")
                with self.metrics.measure_phase("raw_collect"):
                    self._collect_raw_data()
                self._log_general("FINISH_1/3. RAW 수집 실행을 종료했습니다.")

            # 2단계: GPT 분석
            self._log_general("START_2/3. RAW 분석 시작합니다.")
            with self.metrics.measure_phase("analyze"):
                self._analyze_raw_data()
            self._log_general("FINISH_2/3. RAW 분석 실행을 종료했습니다.")

            # 3단계: 텔레그램 전송
            self._log_general("START_3/3. 분석 전송 시작합니다.")
            with self.metrics.measure_phase("send"):
                self._send_analyzed_data()
            self._log_general("FINISH_3/3. 분석 전송 실행을 종료했습니다.")

            self._log_general("서버 모두 종료합니다.")

        finally:
            self.metrics.end_pipeline()
            # 메트릭 저장
            if self.metrics.current_metrics:
                self._save_metrics()

    def _collect_raw_data(self):
        """1단계: RAW 데이터 수집"""
        collect_summary = {}  # 구청별 수집 결과

        for district_code in SCRAPER_CLASSES.keys():
            district_kor = DISTRICT_NAMES_ENG_TO_KOR.get(district_code, district_code)
            self._log_general(f"{district_kor} 시도 중")

            with self.metrics.measure_district(district_code) as result:
                try:
                    scraper = create_scraper(district_code, self.http_client)
                    scraped_data = scraper.scrape(self.config.max_page_num)

                    # DB에 저장
                    saved_count = self._save_raw_data(district_kor, scraped_data)
                    result["items"] = saved_count
                    result["success"] = True

                    # 새로 수집된 것만 기록
                    if saved_count > 0:
                        collect_summary[district_kor] = saved_count

                except Exception as e:
                    err_msg = traceback.format_exc()
                    self._log_error(
                        f"public_funeral.{district_code}",
                        err_msg,
                        f"실패(type:{type(e).__name__})"
                    )
                    result["error"] = str(e)
                    # 개별 스크래퍼 실패는 무시하고 다음 구청으로 계속 진행
                    continue

        # 수집 요약 로그
        if collect_summary:
            summary_lines = [f"{k}: {v}건" for k, v in collect_summary.items()]
            total = sum(collect_summary.values())
            self._log_general(f"📥 RAW 수집 결과: 총 {total}건 ({', '.join(summary_lines)})")
        else:
            self._log_general("📥 RAW 수집 결과: 새로 수집된 데이터 없음")

    def _save_raw_data(self, district_kor: str, scraped_data: List[Dict]) -> int:
        """스크래핑 데이터를 DB에 저장"""
        saved = 0
        existing_contents = self.db.get_raw_contents_by_district(district_kor)

        for item in scraped_data:
            url = item["url"]
            content = item["content"]

            # 이미 존재하는 내용이면 스킵
            if content in existing_contents:
                continue

            # 같은 URL의 레코드 수 (수정 횟수)
            update_count = self.db.count_same_url(url, district_kor)

            # 저장
            self.db.add_raw(
                district=district_kor,
                url=url,
                content=content,
                update_count=update_count
            )
            saved += 1

        return saved

    def _analyze_raw_data(self):
        """2단계: GPT 분석"""
        unanalyzed = self.db.get_unanalyzed_raw()
        analyze_summary = {}  # 구청별 분석 결과
        total_count = len(unanalyzed)

        for idx, raw_item in enumerate(unanalyzed, 1):
            # 진행상황 출력
            name_preview = raw_item.get("content", "")[:30].replace("\n", " ")
            print(f"  [{idx}/{total_count}] {raw_item['district']} 분석 중... ({name_preview}...)")
            try:
                # GPT 분석
                result = self.gpt.analyze_raw_data({
                    "url": raw_item["url"],
                    "content": raw_item["content"],
                    "updated": raw_item.get("update_count", 0)
                })

                # 분석 결과 저장
                self.db.add_analyzed(
                    raw_id=raw_item["id"],
                    content_hash=raw_item["content_hash"],
                    district=raw_item["district"],
                    url=raw_item["url"],
                    update_count=raw_item.get("update_count", 0),
                    analyzed_data=result.get("content", {})
                )

                # 구청별 카운트
                district = raw_item["district"]
                analyze_summary[district] = analyze_summary.get(district, 0) + 1

            except Exception as e:
                err_msg = traceback.format_exc()
                self._log_error(
                    "SECOND_RAW_ANALYZE",
                    err_msg,
                    f"type:{type(e).__name__}"
                )

        # 분석 요약 로그
        total = sum(analyze_summary.values())
        if analyze_summary:
            summary_lines = [f"{k}: {v}건" for k, v in analyze_summary.items()]
            self._log_general(f"🔍 분석 결과: 총 {total}건 ({', '.join(summary_lines)})")
        else:
            self._log_general("🔍 분석 결과: 새로 분석된 데이터 없음")

        if self.metrics.current_metrics:
            self.metrics.current_metrics.items_analyzed = total

    def _send_analyzed_data(self):
        """3단계: 텔레그램 전송"""
        unsent = self.db.get_unsent_analyzed()
        send_summary = {}  # 구청별 전송 결과

        for item in unsent:
            try:
                # 분석 데이터 정리
                cleaned = clean_analyzed_data({"content": {
                    "이름": item.get("name", ""),
                    "생년월일": item.get("birth_date", ""),
                    "거주지": item.get("residence", ""),
                    "사망일시": item.get("death_datetime", ""),
                    "사망장소": item.get("death_place", ""),
                    "장례일정": item.get("funeral_schedule", ""),
                    "장례장소": item.get("funeral_place", ""),
                    "발인일시": item.get("departure_datetime", ""),
                    "화장일시": item.get("cremation_datetime", ""),
                }})

                # 텔레그램 전송
                success = self.telegram.send_funeral_notification(
                    district_kor=item["district"],
                    url=item["url"],
                    update_count=item.get("update_count", 0),
                    analyzed_data=cleaned
                )

                if success:
                    # 전송 완료 기록
                    self.db.mark_as_sent(item["content_hash"])

                    # 구청별 카운트
                    district = item["district"]
                    send_summary[district] = send_summary.get(district, 0) + 1

            except Exception as e:
                err_msg = traceback.format_exc()
                self._log_error(
                    "THIRD_SEND_DATA",
                    err_msg,
                    f"type:{type(e).__name__}"
                )

        # 전송 요약 로그
        total = sum(send_summary.values())
        if send_summary:
            summary_lines = [f"{k}: {v}건" for k, v in send_summary.items()]
            self._log_general(f"📤 전송 결과: 총 {total}건 ({', '.join(summary_lines)})")
        else:
            self._log_general("📤 전송 결과: 새로 전송된 데이터 없음")

        if self.metrics.current_metrics:
            self.metrics.current_metrics.items_sent = total

    def _save_metrics(self):
        """메트릭 저장"""
        if self.metrics.current_metrics:
            metrics_dict = self.metrics.current_metrics.to_dict()
            self.db.save_metrics(metrics_dict)
            logger.info(self.metrics.current_metrics.summary())

    def _log_general(self, message: str):
        """일반 로그 - 즉시 텔레그램 + Pocketbase 저장"""
        if self.logger:
            self.logger.log_general(message)
        else:
            logger.info(message)
            self.telegram.send_general_notification(message)

        # Pocketbase에 즉시 저장
        self.db.save_log(level="INFO", message=message)

    def _log_error(self, function_name: str, error_message: str, add_text: str = ""):
        """에러 로그 - 즉시 텔레그램 + Pocketbase 저장"""
        if self.logger:
            self.logger.log_error(function_name, error_message, add_text)
        else:
            logger.error(f"{function_name}: {add_text}\n{error_message}")

        # Pocketbase에 즉시 저장
        self.db.save_log(
            level="ERROR",
            message=add_text,
            function_name=function_name,
            error_trace=error_message
        )
