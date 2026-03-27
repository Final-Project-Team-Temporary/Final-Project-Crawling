"""
article_publisher.py
역할: Scrapy 크롤링 실행 → Redis Stream 발행

환경변수 목록:
  # Redis 연결
  REDIS_HOST          - Redis 호스트 주소 (필수)
  REDIS_PORT          - Redis 포트 (기본값: 6379)
  REDIS_USE_TLS       - TLS 사용 여부 "true"/"false" (기본값: "false")
  REDIS_SSL_CERT_REQS - TLS 인증서 검증 수준 "none"/"optional"/"required" (기본값: "required")

  # Redis Stream / 중복 방지
  REDIS_ARTICLE_STREAM_KEY   - 기사 발행 대상 Stream key (필수)
  REDIS_PUBLISHED_URLS_KEY   - 발행 완료 URL 저장 Set key (필수)
  REDIS_LAST_CRAWL_KEY       - 마지막 크롤링 시각 저장 key (미설정 시 증분 크롤링 비활성화)

  # 크롤러
  OUTPUT_FILE_PATH    - Scrapy 출력 파일 경로 (기본값: /tmp/output.json, Lambda는 /tmp 필수)
  MAX_ARTICLES        - 최대 크롤링 기사 수 (기본값: 10, naver_crawler.py 참조)
  MAX_CRAWL_TIME      - 크롤링 최대 시간(초) (기본값: 300, naver_crawler.py 참조)
"""

import json
import logging
import os
import subprocess
import time
from datetime import datetime
from typing import Optional

import redis as redis_lib

# ---------------------------------------------------------------------------
# 로거 설정
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
)
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# 상수
# ---------------------------------------------------------------------------
CONTENT_MAX_LEN: int = 50_000
STREAM_MAXLEN: int = 10_000
FAILED_ARTICLES_PATH: str = "/tmp/failed_articles.json"
PUBLISHED_URLS_TTL: int = 7 * 24 * 3600   # 7일 (초)
MAX_RETRIES: int = 3

# ---------------------------------------------------------------------------
# 전역 Redis 클라이언트 (Lambda warm start 재사용)
# ---------------------------------------------------------------------------
_redis_client: Optional[redis_lib.Redis] = None


# ---------------------------------------------------------------------------
# Redis 연결
# ---------------------------------------------------------------------------

def get_redis_client() -> redis_lib.Redis:
    """
    전역 Redis 클라이언트를 반환한다.
    클라이언트가 없거나 연결이 끊어진 경우 지수 백오프(1s/2s/4s)로 재연결을 시도한다.
    3회 모두 실패하면 ConnectionError를 발생시킨다.
    """
    global _redis_client

    if _redis_client is not None:
        try:
            _redis_client.ping()
            return _redis_client
        except Exception:
            _redis_client = None

    host: str = os.environ["REDIS_HOST"]
    port: int = int(os.environ.get("REDIS_PORT", "6379"))
    use_tls: bool = os.environ.get("REDIS_USE_TLS", "false").lower() == "true"
    ssl_cert_reqs: str = os.environ.get("REDIS_SSL_CERT_REQS", "required")

    last_exc: Exception = RuntimeError("알 수 없는 오류")

    for attempt in range(MAX_RETRIES):
        try:
            client = redis_lib.Redis(
                host=host,
                port=port,
                ssl=use_tls,
                ssl_cert_reqs=ssl_cert_reqs if use_tls else None,
                decode_responses=True,
                socket_connect_timeout=5,
                socket_timeout=10,
            )
            client.ping()
            _redis_client = client
            logger.info(f"Redis 연결 성공 (시도 {attempt + 1}/{MAX_RETRIES})")
            return _redis_client
        except Exception as exc:
            last_exc = exc
            wait: int = 2 ** attempt  # 1, 2, 4초
            if attempt < MAX_RETRIES - 1:
                logger.warning(
                    f"Redis 연결 실패 (시도 {attempt + 1}/{MAX_RETRIES}): {exc}. "
                    f"{wait}초 후 재시도"
                )
                time.sleep(wait)
            else:
                logger.warning(
                    f"Redis 연결 실패 (시도 {attempt + 1}/{MAX_RETRIES}): {exc}."
                )

    logger.error(f"Redis 연결 전체 실패 (최대 {MAX_RETRIES}회): {last_exc}")
    raise ConnectionError(f"Redis 연결 실패: {last_exc}") from last_exc


# ---------------------------------------------------------------------------
# 증분 크롤링 — 마지막 크롤링 시각 관리
# ---------------------------------------------------------------------------

def get_last_crawl_time(redis_client: redis_lib.Redis) -> Optional[datetime]:
    """
    Redis에서 마지막 크롤링 시각을 읽어 datetime으로 반환한다.

    REDIS_LAST_CRAWL_KEY 환경변수가 설정되지 않으면 None을 반환하여
    전체 크롤링을 수행하도록 한다.
    """
    key: str = os.environ.get("REDIS_LAST_CRAWL_KEY", "")
    if not key:
        return None
    try:
        val: Optional[str] = redis_client.get(key)
        if val:
            return datetime.fromisoformat(val)
        return None
    except Exception as exc:
        logger.warning(f"last_crawl_time 조회 실패 (전체 크롤링으로 진행): {exc}")
        return None


def update_last_crawl_time(
    redis_client: redis_lib.Redis,
    dt: datetime,
) -> bool:
    """
    Redis에 마지막 크롤링 시각을 ISO 8601 형식으로 저장한다.

    REDIS_LAST_CRAWL_KEY 환경변수가 설정되지 않으면 False를 반환하고 아무것도 하지 않는다.
    """
    key: str = os.environ.get("REDIS_LAST_CRAWL_KEY", "")
    if not key:
        return False
    try:
        redis_client.set(key, dt.isoformat())
        return True
    except Exception as exc:
        logger.warning(f"last_crawl_time 업데이트 실패: {exc}")
        return False


# ---------------------------------------------------------------------------
# 중복 체크
# ---------------------------------------------------------------------------

def load_published_urls(redis_client: redis_lib.Redis) -> set[str]:
    """
    Redis Set에서 발행 완료된 URL 목록을 전부 읽어 메모리 캐시(set)로 반환한다.
    Lambda 실행마다 호출하여 다른 인스턴스의 발행 결과도 반영한다.
    """
    key: str = os.environ["REDIS_PUBLISHED_URLS_KEY"]
    try:
        members: set[str] = redis_client.smembers(key)
        logger.info(f"발행된 URL 캐시 로드 완료: {len(members)}건")
        return set(members)
    except Exception as exc:
        logger.warning(f"발행된 URL 캐시 로드 실패 (빈 캐시로 진행): {exc}")
        return set()


def is_duplicate(url: str, cache: set[str]) -> bool:
    """메모리 캐시를 이용해 중복 여부를 확인한다."""
    return url in cache


# ---------------------------------------------------------------------------
# 기사 발행
# ---------------------------------------------------------------------------

def publish_article(
    redis_client: redis_lib.Redis,
    article: dict,
    cache: set[str],
) -> bool:
    """
    단일 기사를 Redis Stream에 발행한다.
    성공 시 Set에 URL을 추가하고, TTL을 갱신하며, 메모리 캐시도 업데이트한다.
    실패 시 False를 반환하며 예외를 전파하지 않는다.
    """
    stream_key: str = os.environ["REDIS_ARTICLE_STREAM_KEY"]
    urls_key: str = os.environ["REDIS_PUBLISHED_URLS_KEY"]
    url: str = article.get("url", "")

    message: dict = {
        "url": url,
        "title": article.get("title", ""),
        "content": (article.get("content") or "")[:CONTENT_MAX_LEN],
        "publishedAt": article.get("publishedAt") or "",
        "press": article.get("press", ""),
    }

    try:
        redis_client.xadd(stream_key, message, maxlen=STREAM_MAXLEN, approximate=True)
        redis_client.sadd(urls_key, url)
        redis_client.expire(urls_key, PUBLISHED_URLS_TTL)
        cache.add(url)
        return True
    except Exception as exc:
        logger.warning(f"기사 발행 실패 [url={url}]: {exc}")
        return False


# ---------------------------------------------------------------------------
# 크롤러 실행
# ---------------------------------------------------------------------------

def run_crawler(since_dt: Optional[datetime] = None) -> list[dict]:
    """
    naver_crawler.py를 subprocess로 실행하고, 출력된 JSONL 파일을 파싱하여
    기사 dict 목록을 반환한다.

    Args:
        since_dt: 이 시각 이후 기사만 수집하는 증분 크롤링 기준 시각.
                  None이면 전체 크롤링(CRAWL_SINCE 환경변수 제거).

    크롤러 실행 실패(비정상 종료) 시 RuntimeError를 발생시킨다.
    """
    output_path: str = os.environ.get("OUTPUT_FILE_PATH", "/tmp/output.json")

    if os.path.exists(output_path):
        os.remove(output_path)

    env = os.environ.copy()
    env["OUTPUT_FILE_PATH"] = output_path

    if since_dt is not None:
        env["CRAWL_SINCE"] = since_dt.isoformat()
    else:
        env.pop("CRAWL_SINCE", None)  # 이전 실행의 잔여 환경변수 제거

    logger.info(f"크롤링 시작: python naver_crawler.py (출력: {output_path})")

    result = subprocess.run(
        ["python", "naver_crawler.py"],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        env=env,
    )

    if result.stdout:
        logger.info(f"크롤러 stdout:\n{result.stdout.strip()}")
    if result.stderr:
        logger.warning(f"크롤러 stderr:\n{result.stderr.strip()}")

    if result.returncode != 0:
        raise RuntimeError(f"크롤링 프로세스 비정상 종료: returncode={result.returncode}")

    if not os.path.exists(output_path):
        logger.warning(f"크롤러 출력 파일 없음: {output_path}")
        return []

    articles: list[dict] = []
    with open(output_path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                articles.append(json.loads(line))
            except json.JSONDecodeError as exc:
                logger.warning(f"JSON 파싱 실패 (라인 건너뜀): {exc}")

    logger.info(f"크롤링 완료: {len(articles)}건")
    return articles


# ---------------------------------------------------------------------------
# 실패 기사 저장
# ---------------------------------------------------------------------------

def _save_failed_articles(articles: list[dict]) -> None:
    """발행 실패 기사를 /tmp/failed_articles.json에 저장한다."""
    try:
        with open(FAILED_ARTICLES_PATH, "w", encoding="utf-8") as f:
            json.dump(articles, f, ensure_ascii=False, default=str, indent=2)
        logger.warning(
            f"발행 실패 기사 {len(articles)}건을 {FAILED_ARTICLES_PATH}에 저장"
        )
    except Exception as exc:
        logger.error(f"실패 기사 파일 저장 오류: {exc}")


# ---------------------------------------------------------------------------
# 메인 오케스트레이터
# ---------------------------------------------------------------------------

def crawl_and_publish() -> dict:
    """
    크롤링을 실행하고 결과를 Redis Stream에 발행한다.

    실행 흐름:
      1. Redis 연결 시도 (증분 크롤링 기준 시각 조회 및 발행을 위해)
      2. REDIS_LAST_CRAWL_KEY 설정 시 마지막 크롤링 시각(since_dt) 조회
      3. since_dt를 전달하여 크롤러 실행 (없으면 전체 크롤링)
      4. Redis 연결 실패 시 전체 기사를 실패 파일로 저장하고 종료
      5. 중복 URL 캐시 로드 → 기사별 발행 처리
      6. 크롤링 기사가 1건 이상이면 last_crawl_time 업데이트

    반환값:
        {
            "crawled":   int,  # 크롤링된 전체 기사 수
            "published": int,  # 발행 성공 수
            "skipped":   int,  # 중복으로 skip된 수
            "failed":    int,  # 발행 실패 수
        }
    """
    crawl_start: datetime = datetime.now()

    # 1. Redis 연결 시도 (실패해도 크롤링은 계속 진행)
    redis_client: Optional[redis_lib.Redis] = None
    try:
        redis_client = get_redis_client()
    except ConnectionError as exc:
        logger.error(f"Redis 연결 불가 — 전체 크롤링으로 진행: {exc}")

    # 2. 증분 크롤링 기준 시각 조회
    since_dt: Optional[datetime] = None
    if redis_client is not None:
        since_dt = get_last_crawl_time(redis_client)
        if since_dt:
            logger.info(f"증분 크롤링 기준: {since_dt.isoformat()} 이후 기사만 수집")
        else:
            logger.info("초기 실행(last_crawl_time 없음): 전체 크롤링")

    # 3. 크롤러 실행
    articles: list[dict] = run_crawler(since_dt)
    total: int = len(articles)

    # 4. Redis 연결 실패 시 전체 실패 처리
    if redis_client is None:
        _save_failed_articles(articles)
        print(f"[FAILED_ARTICLES_COUNT] {total}")
        return {"crawled": total, "published": 0, "skipped": 0, "failed": total}

    # 5. 중복 URL 캐시 로드
    published_cache: set[str] = load_published_urls(redis_client)

    # 6. 기사별 처리
    published: int = 0
    skipped: int = 0
    failed_articles: list[dict] = []

    for article in articles:
        url: str = article.get("url", "")

        if is_duplicate(url, published_cache):
            skipped += 1
            logger.info(f"중복 skip: {url}")
            continue

        success: bool = publish_article(redis_client, article, published_cache)
        if success:
            published += 1
        else:
            failed_articles.append(article)

    failed: int = len(failed_articles)

    # 7. 실패 기사 파일 저장
    if failed_articles:
        _save_failed_articles(failed_articles)

    # 8. last_crawl_time 업데이트 (크롤링 기사가 1건 이상일 때)
    if total > 0:
        update_last_crawl_time(redis_client, crawl_start)

    # 9. 최종 요약 로그
    summary = (
        f"크롤링: {total}건, 발행성공: {published}건, "
        f"중복skip: {skipped}건, 실패: {failed}건"
    )
    logger.info(summary)
    print(f"[FAILED_ARTICLES_COUNT] {failed}")

    return {
        "crawled": total,
        "published": published,
        "skipped": skipped,
        "failed": failed,
    }
