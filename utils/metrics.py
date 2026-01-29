"""
메트릭 추적 모듈
실행 시간, 메모리 사용량, 성공/실패 카운트 등 추적
"""

import time
import tracemalloc
from dataclasses import dataclass, field
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Optional
from contextlib import contextmanager
import logging

logger = logging.getLogger(__name__)
KST = timezone(timedelta(hours=9))


@dataclass
class DistrictMetrics:
    """구청별 메트릭"""
    district: str
    success: bool = False
    error_message: Optional[str] = None
    duration_seconds: float = 0.0
    items_scraped: int = 0
    used_tor: bool = False


@dataclass
class PipelineMetrics:
    """파이프라인 전체 메트릭"""
    started_at: datetime = field(default_factory=lambda: datetime.now(KST))
    ended_at: Optional[datetime] = None

    # 단계별 소요 시간
    raw_collect_duration: float = 0.0
    analyze_duration: float = 0.0
    send_duration: float = 0.0

    # 메모리 사용량 (MB)
    peak_memory_mb: float = 0.0

    # 구청별 결과
    district_results: List[DistrictMetrics] = field(default_factory=list)

    # 분석/전송 카운트
    items_analyzed: int = 0
    items_sent: int = 0

    @property
    def total_duration(self) -> float:
        """전체 소요 시간 (초)"""
        if self.ended_at:
            return (self.ended_at - self.started_at).total_seconds()
        return (datetime.now(KST) - self.started_at).total_seconds()

    @property
    def success_count(self) -> int:
        """성공한 구청 수"""
        return sum(1 for d in self.district_results if d.success)

    @property
    def failure_count(self) -> int:
        """실패한 구청 수"""
        return sum(1 for d in self.district_results if not d.success)

    @property
    def tor_usage_count(self) -> int:
        """Tor 사용 횟수"""
        return sum(1 for d in self.district_results if d.used_tor)

    def to_dict(self) -> Dict:
        """딕셔너리 변환 (DB 저장용)"""
        return {
            "started_at": self.started_at.isoformat(),
            "ended_at": self.ended_at.isoformat() if self.ended_at else None,
            "total_duration_seconds": self.total_duration,
            "raw_collect_duration": self.raw_collect_duration,
            "analyze_duration": self.analyze_duration,
            "send_duration": self.send_duration,
            "peak_memory_mb": self.peak_memory_mb,
            "success_count": self.success_count,
            "failure_count": self.failure_count,
            "tor_usage_count": self.tor_usage_count,
            "items_analyzed": self.items_analyzed,
            "items_sent": self.items_sent,
            "district_results": [
                {
                    "district": d.district,
                    "success": d.success,
                    "error_message": d.error_message,
                    "duration_seconds": d.duration_seconds,
                    "items_scraped": d.items_scraped,
                    "used_tor": d.used_tor,
                }
                for d in self.district_results
            ]
        }

    def summary(self) -> str:
        """요약 문자열"""
        return f"""
=== 실행 결과 요약 ===
시작: {self.started_at.strftime('%Y-%m-%d %H:%M:%S')}
종료: {self.ended_at.strftime('%Y-%m-%d %H:%M:%S') if self.ended_at else '진행중'}
소요시간: {self.total_duration:.1f}초
메모리: {self.peak_memory_mb:.1f}MB

[수집] {self.success_count}/{len(self.district_results)}개 구청 성공 ({self.raw_collect_duration:.1f}초)
[분석] {self.items_analyzed}건 ({self.analyze_duration:.1f}초)
[전송] {self.items_sent}건 ({self.send_duration:.1f}초)
[Tor] {self.tor_usage_count}회 사용
"""


class MetricsCollector:
    """메트릭 수집기"""

    def __init__(self):
        self.current_metrics: Optional[PipelineMetrics] = None
        self._memory_tracking = False

    def start_pipeline(self) -> PipelineMetrics:
        """파이프라인 메트릭 수집 시작"""
        self.current_metrics = PipelineMetrics()
        self._start_memory_tracking()
        return self.current_metrics

    def end_pipeline(self):
        """파이프라인 메트릭 수집 종료"""
        if self.current_metrics:
            self.current_metrics.ended_at = datetime.now(KST)
            self._update_memory_usage()
            self._stop_memory_tracking()

    def add_district_result(
        self,
        district: str,
        success: bool,
        duration: float,
        items_count: int = 0,
        used_tor: bool = False,
        error_message: Optional[str] = None
    ):
        """구청 결과 추가"""
        if self.current_metrics:
            self.current_metrics.district_results.append(
                DistrictMetrics(
                    district=district,
                    success=success,
                    duration_seconds=duration,
                    items_scraped=items_count,
                    used_tor=used_tor,
                    error_message=error_message,
                )
            )

    @contextmanager
    def measure_phase(self, phase_name: str):
        """단계별 시간 측정 컨텍스트 매니저"""
        start = time.time()
        try:
            yield
        finally:
            duration = time.time() - start
            if self.current_metrics:
                if phase_name == "raw_collect":
                    self.current_metrics.raw_collect_duration = duration
                elif phase_name == "analyze":
                    self.current_metrics.analyze_duration = duration
                elif phase_name == "send":
                    self.current_metrics.send_duration = duration

    @contextmanager
    def measure_district(self, district: str):
        """구청별 시간 측정 컨텍스트 매니저"""
        start = time.time()
        result = {"success": False, "items": 0, "used_tor": False, "error": None}
        try:
            yield result
            result["success"] = True
        except Exception as e:
            result["error"] = str(e)
            raise
        finally:
            duration = time.time() - start
            self.add_district_result(
                district=district,
                success=result["success"],
                duration=duration,
                items_count=result.get("items", 0),
                used_tor=result.get("used_tor", False),
                error_message=result.get("error"),
            )

    def _start_memory_tracking(self):
        """메모리 추적 시작"""
        try:
            tracemalloc.start()
            self._memory_tracking = True
        except Exception:
            pass

    def _stop_memory_tracking(self):
        """메모리 추적 중지"""
        if self._memory_tracking:
            try:
                tracemalloc.stop()
            except Exception:
                pass
            self._memory_tracking = False

    def _update_memory_usage(self):
        """현재 메모리 사용량 업데이트"""
        if self._memory_tracking and self.current_metrics:
            try:
                current, peak = tracemalloc.get_traced_memory()
                self.current_metrics.peak_memory_mb = peak / 1024 / 1024
            except Exception:
                pass


# 글로벌 메트릭 수집기
_collector: Optional[MetricsCollector] = None


def get_collector() -> MetricsCollector:
    """싱글톤 메트릭 수집기 반환"""
    global _collector
    if _collector is None:
        _collector = MetricsCollector()
    return _collector
