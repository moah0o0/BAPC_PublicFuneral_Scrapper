"""
JSON → Pocketbase 마이그레이션 스크립트

기존 JSON 파일:
- DB_RAW.json → funeral_raw
- DB_ANALYZE.json → funeral_analyzed
- DB_SENDED.json → funeral_sent
"""

import json
import hashlib
from pathlib import Path
from datetime import datetime, timezone, timedelta
import logging

from config import Config
from services.pocketbase import PocketbaseClient

logger = logging.getLogger(__name__)
KST = timezone(timedelta(hours=9))


def load_json_file(file_path: Path) -> dict:
    """JSON 파일 로드"""
    if not file_path.exists():
        logger.warning(f"파일 없음: {file_path}")
        return {}

    with open(file_path, 'r', encoding='utf-8') as f:
        return json.load(f)


def migrate_raw_data(db: PocketbaseClient, base_dir: Path) -> int:
    """
    DB_RAW.json → funeral_raw 마이그레이션

    기존 구조:
    {
        "북구": [
            {"url": "...", "content": "...", "updated": 0},
            ...
        ],
        ...
    }
    """
    file_path = base_dir / "data" / "DB_RAW.json"
    data = load_json_file(file_path)

    if not data:
        return 0

    count = 0
    for district, items in data.items():
        for item in items:
            url = item.get("url", "")
            content = item.get("content", "")
            updated = item.get("updated", 0)

            # 이미 존재하는지 확인
            if db.raw_exists(content, district):
                continue

            # 저장
            result = db.add_raw(
                district=district,
                url=url,
                content=content,
                update_count=updated
            )

            if result:
                count += 1
                logger.debug(f"RAW 마이그레이션: {district} - {url[:50]}...")

    logger.info(f"RAW 데이터 마이그레이션 완료: {count}건")
    return count


def get_raw_id_mapping(db: PocketbaseClient) -> dict:
    """
    funeral_raw에서 content_hash → id 매핑 조회
    """
    result = db._request(
        "GET",
        "funeral_raw/records",
        params={"fields": "id,content_hash", "perPage": 10000}
    )
    if not result:
        return {}

    return {r["content_hash"]: r["id"] for r in result.get("items", [])}


def migrate_analyzed_data(db: PocketbaseClient, base_dir: Path) -> int:
    """
    DB_ANALYZE.json → funeral_analyzed 마이그레이션

    기존 구조:
    {
        "data": [
            {
                "url": "...",
                "updated": 0,
                "content": {"이름": "...", "생년월일": "...", ...},
                "hash": "...",
                "goo": "북구",
                "created_at": "...",
                "filter": {...},
                "people_edited": false
            },
            ...
        ],
        "last_updated": "..."
    }
    """
    file_path = base_dir / "data" / "DB_ANALYZE.json"
    data = load_json_file(file_path)

    if not data:
        return 0

    items = data.get("data", [])
    existing_hashes = set(db.get_analyzed_hashes())

    # content_hash → raw_id 매핑 조회
    print("RAW 데이터에서 content_hash → raw_id 매핑 조회 중...")
    hash_to_raw_id = get_raw_id_mapping(db)
    print(f"매핑 조회 완료: {len(hash_to_raw_id)}건")

    # 마이그레이션 대상 필터링
    to_migrate = [item for item in items if item.get("hash", "") not in existing_hashes]
    total = len(to_migrate)
    skipped = len(items) - total
    print(f"전체: {len(items)}건, 이미 존재: {skipped}건, 마이그레이션 대상: {total}건")

    count = 0
    failed = 0
    no_raw_id = 0

    for i, item in enumerate(to_migrate):
        content_hash = item.get("hash", "")
        analyzed_content = item.get("content", {})

        # content_hash로 raw_id 찾기
        raw_id = hash_to_raw_id.get(content_hash, "")
        if not raw_id:
            no_raw_id += 1

        result = db.add_analyzed(
            raw_id=raw_id,
            content_hash=content_hash,
            district=item.get("goo", ""),
            url=item.get("url", ""),
            update_count=item.get("updated", 0),
            analyzed_data=analyzed_content
        )

        if result:
            count += 1
        else:
            failed += 1

        # 10건마다 진행 상황 출력
        if (i + 1) % 10 == 0 or (i + 1) == total:
            remaining = total - (i + 1)
            print(f"\r[analyzed] {i+1}/{total} 완료 (성공: {count}, 실패: {failed}, raw_id 없음: {no_raw_id}, 잔여: {remaining})", end="", flush=True)

    print()  # 줄바꿈
    logger.info(f"분석 데이터 마이그레이션 완료: {count}건 (raw_id 없음: {no_raw_id}건)")
    return count


def migrate_sent_data(db: PocketbaseClient, base_dir: Path) -> int:
    """
    DB_SENDED.json → funeral_sent 마이그레이션

    기존 구조:
    {
        "data": ["hash1", "hash2", ...]
    }
    """
    file_path = base_dir / "data" / "DB_SENDED.json"
    data = load_json_file(file_path)

    if not data:
        return 0

    hashes = data.get("data", [])
    existing_sent = set(db.get_sent_hashes())

    # 마이그레이션 대상 필터링
    to_migrate = [h for h in hashes if h not in existing_sent]
    total = len(to_migrate)
    skipped = len(hashes) - total
    print(f"전체: {len(hashes)}건, 이미 존재: {skipped}건, 마이그레이션 대상: {total}건")

    count = 0
    failed = 0

    for i, content_hash in enumerate(to_migrate):
        result = db.mark_as_sent(content_hash)
        if result:
            count += 1
        else:
            failed += 1

        # 50건마다 진행 상황 출력
        if (i + 1) % 50 == 0 or (i + 1) == total:
            remaining = total - (i + 1)
            print(f"\r[sent] {i+1}/{total} 완료 (성공: {count}, 실패: {failed}, 잔여: {remaining})", end="", flush=True)

    print()  # 줄바꿈
    logger.info(f"전송 기록 마이그레이션 완료: {count}건")
    return count


def migrate(config: Config, skip_raw: bool = False):
    """
    전체 마이그레이션 실행

    Args:
        config: 설정 객체
        skip_raw: True면 RAW 데이터 마이그레이션 건너뛰기
    """
    logging.basicConfig(level=logging.INFO)

    base_dir = config.base_dir
    logger.info(f"마이그레이션 시작 (소스: {base_dir})")

    # Pocketbase 연결
    db = PocketbaseClient(config.pocketbase)
    if not db.authenticate():
        logger.error("Pocketbase 인증 실패")
        return

    # 마이그레이션 실행
    raw_count = 0
    if not skip_raw:
        raw_count = migrate_raw_data(db, base_dir)
    else:
        logger.info("RAW 데이터 마이그레이션 건너뛰기")

    analyzed_count = migrate_analyzed_data(db, base_dir)
    sent_count = migrate_sent_data(db, base_dir)

    logger.info("=" * 50)
    logger.info("마이그레이션 완료 요약:")
    logger.info(f"  - RAW 데이터: {raw_count}건 {'(건너뜀)' if skip_raw else ''}")
    logger.info(f"  - 분석 데이터: {analyzed_count}건")
    logger.info(f"  - 전송 기록: {sent_count}건")
    logger.info("=" * 50)


if __name__ == "__main__":
    from config import load_config
    config = load_config()
    migrate(config)
