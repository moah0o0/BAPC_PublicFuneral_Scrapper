"""
스케줄러 모듈
APScheduler 기반 내장 스케줄러
"""

import signal
import sys
import logging
from typing import Callable, Optional

from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.interval import IntervalTrigger
from apscheduler.events import EVENT_JOB_EXECUTED, EVENT_JOB_ERROR

from config import Config

logger = logging.getLogger(__name__)


class FuneralScheduler:
    """
    부고 스크래퍼 스케줄러

    특징:
    - APScheduler 기반 15분 간격 실행
    - 시작 시 즉시 1회 실행
    - Graceful shutdown 지원
    - 실행 이력 로깅
    """

    def __init__(
        self,
        config: Config,
        job_func: Callable,
        on_error: Optional[Callable] = None
    ):
        """
        Args:
            config: 설정 객체
            job_func: 실행할 작업 함수 (Pipeline.run)
            on_error: 에러 발생 시 콜백
        """
        self.config = config
        self.job_func = job_func
        self.on_error = on_error
        self.scheduler = BlockingScheduler()
        self._setup_signal_handlers()
        self._setup_listeners()

    def _setup_signal_handlers(self):
        """종료 시그널 핸들러 설정"""
        signal.signal(signal.SIGTERM, self._shutdown)
        signal.signal(signal.SIGINT, self._shutdown)

    def _setup_listeners(self):
        """이벤트 리스너 설정"""
        self.scheduler.add_listener(
            self._job_executed_listener,
            EVENT_JOB_EXECUTED
        )
        self.scheduler.add_listener(
            self._job_error_listener,
            EVENT_JOB_ERROR
        )

    def _job_executed_listener(self, event):
        """작업 완료 리스너"""
        logger.info(f"작업 완료: {event.job_id}")

    def _job_error_listener(self, event):
        """작업 에러 리스너"""
        logger.error(f"작업 실패: {event.job_id}, 에러: {event.exception}")
        if self.on_error:
            self.on_error(event.exception)

    def start(self):
        """스케줄러 시작"""
        # 주기적 작업 등록
        self.scheduler.add_job(
            self.job_func,
            trigger=IntervalTrigger(
                minutes=self.config.schedule_interval_minutes
            ),
            id='funeral_scraper_periodic',
            name='공영장례 스크래퍼 (주기)',
            replace_existing=True,
            misfire_grace_time=300,  # 5분 유예
            coalesce=True,  # 누락된 실행은 1회로 통합
            max_instances=1  # 동시 실행 방지
        )

        # 시작 시 즉시 1회 실행
        self.scheduler.add_job(
            self.job_func,
            id='funeral_scraper_initial',
            name='공영장례 스크래퍼 (초기)'
        )

        logger.info(
            f"스케줄러 시작 "
            f"(간격: {self.config.schedule_interval_minutes}분)"
        )

        try:
            self.scheduler.start()
        except (KeyboardInterrupt, SystemExit):
            logger.info("스케줄러 종료 요청")

    def _shutdown(self, signum, frame):
        """Graceful shutdown"""
        logger.info(f"종료 신호 수신 (signal={signum}), 스케줄러 중지...")
        self.scheduler.shutdown(wait=True)
        logger.info("스케줄러 정상 종료")
        sys.exit(0)

    def run_once(self):
        """1회만 실행 (테스트용)"""
        self.job_func()
