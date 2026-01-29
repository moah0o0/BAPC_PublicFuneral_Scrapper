#!/usr/bin/env python3
"""
공영장례 스크래퍼 엔트리포인트

사용법:
    python main.py              # 스케줄러 모드 (15분 간격)
    python main.py --once       # 1회만 실행
    python main.py --migrate    # JSON → Pocketbase 마이그레이션
    python main.py --cleanup    # 고아 전송완료 레코드 정리
"""

import argparse
import logging
import sys

from config import load_config, get_config
from core.http_client import HttpClient
from core.pipeline import Pipeline
from core.scheduler import FuneralScheduler
from services.pocketbase import PocketbaseClient
from services.telegram import TelegramService
from services.gpt_analyzer import GPTAnalyzer
from utils.logger import get_logger


def setup_logging():
    """로깅 설정"""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )


def create_pipeline(config) -> Pipeline:
    """파이프라인 생성"""
    http_client = HttpClient(config)
    db = PocketbaseClient(config.pocketbase)
    telegram = TelegramService(config.telegram)
    gpt = GPTAnalyzer(config.openai_api_key)
    scraper_logger = get_logger(config)

    # Pocketbase 인증
    if not db.authenticate():
        logging.warning("Pocketbase 인증 실패 - DB 저장이 작동하지 않을 수 있습니다")

    return Pipeline(
        http_client=http_client,
        db=db,
        telegram=telegram,
        gpt=gpt,
        config=config,
        scraper_logger=scraper_logger
    )


def run_scheduler(config):
    """스케줄러 모드로 실행"""
    pipeline = create_pipeline(config)

    def on_error(e):
        """에러 발생 시 텔레그램 알림"""
        try:
            telegram = TelegramService(config.telegram)
            telegram.send_error_notification(
                "Scheduler",
                str(e),
                "scheduler_error",
                "스케줄러 실행 중 에러 발생"
            )
        except Exception:
            pass

    scheduler = FuneralScheduler(
        config=config,
        job_func=pipeline.run,
        on_error=on_error
    )

    print(f"스케줄러 시작 ({config.schedule_interval_minutes}분 간격)")
    print("종료: Ctrl+C")
    scheduler.start()


def run_once(config, skip_raw: bool = False):
    """1회만 실행"""
    if skip_raw:
        print("파이프라인 1회 실행 중... (RAW 수집 건너뜀)")
    else:
        print("파이프라인 1회 실행 중...")
    pipeline = create_pipeline(config)
    pipeline.run(skip_raw=skip_raw)
    print("완료")


def run_migration(config, skip_raw: bool = False):
    """JSON → Pocketbase 마이그레이션"""
    from migration.json_to_pocketbase import migrate
    print("마이그레이션 시작...")
    migrate(config, skip_raw=skip_raw)
    print("마이그레이션 완료")


def run_cleanup(config):
    """고아/중복 레코드 정리"""
    db = PocketbaseClient(config.pocketbase)
    if not db.authenticate():
        print("Pocketbase 인증 실패")
        sys.exit(1)

    print("1. 중복 전송완료 레코드 정리 중...")
    dup_deleted = db.cleanup_duplicate_sent()

    print("\n2. 고아 전송완료 레코드 정리 중...")
    orphan_deleted = db.cleanup_orphan_sent()

    print(f"\n정리 완료: 중복 {dup_deleted}건 + 고아 {orphan_deleted}건 = 총 {dup_deleted + orphan_deleted}건 삭제됨")


def main():
    parser = argparse.ArgumentParser(
        description='공영장례 스크래퍼',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
예시:
  python main.py              스케줄러 모드 (15분 간격 자동 실행)
  python main.py --once       1회만 실행
  python main.py --migrate    JSON 데이터를 Pocketbase로 마이그레이션
  python main.py --cleanup    고아 전송완료 레코드 정리
        """
    )

    parser.add_argument(
        '--once',
        action='store_true',
        help='1회만 실행 후 종료'
    )

    parser.add_argument(
        '--migrate',
        action='store_true',
        help='JSON → Pocketbase 마이그레이션'
    )

    parser.add_argument(
        '--skip-raw',
        action='store_true',
        help='RAW 수집 단계 건너뛰기 (분석/전송만 실행)'
    )

    parser.add_argument(
        '--cleanup',
        action='store_true',
        help='고아 전송완료 레코드 정리'
    )

    args = parser.parse_args()

    setup_logging()
    config = load_config()

    # 필수 설정 확인
    if not config.telegram.bot_token:
        print("오류: TELEGRAM_BOT_TOKEN이 설정되지 않았습니다.")
        print(".env 파일을 확인하세요.")
        sys.exit(1)

    if not config.openai_api_key:
        print("오류: OPENAI_API_KEY가 설정되지 않았습니다.")
        print(".env 파일을 확인하세요.")
        sys.exit(1)

    try:
        if args.cleanup:
            run_cleanup(config)
        elif args.migrate:
            run_migration(config, skip_raw=args.skip_raw)
        elif args.once:
            run_once(config, skip_raw=args.skip_raw)
        else:
            run_scheduler(config)
    except KeyboardInterrupt:
        print("\n종료됨")
        sys.exit(0)
    except Exception as e:
        logging.exception(f"실행 중 오류 발생: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
