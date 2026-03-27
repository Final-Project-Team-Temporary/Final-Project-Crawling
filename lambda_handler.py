import json
import logging
import traceback

from article_publisher import crawl_and_publish

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# Lambda 타임아웃 안전 마진 (밀리초)
# 남은 시간이 이 값 이하면 조기 종료 경고를 남긴다.
_TIMEOUT_SAFETY_MARGIN_MS: int = 15_000


def handler(event: dict, context) -> dict:
    """
    AWS Lambda handler — EventBridge 스케줄 또는 수동 호출로 진입한다.

    context.get_remaining_time_in_millis() 를 이용해 타임아웃 임박 여부를 감지하고,
    처리 결과를 응답 본문에 포함한다.
    """
    source: str = event.get("source", "manual")
    logger.info(f"Lambda 시작 — source={source}")

    # 타임아웃 임박 경고 (크롤링 시작 전 체크)
    _warn_if_timeout_near(context, phase="시작")

    try:
        result: dict = crawl_and_publish()

        # 크롤링 완료 후 남은 시간 로깅
        _warn_if_timeout_near(context, phase="완료")

        logger.info(
            f"Lambda 정상 종료 — "
            f"crawled={result['crawled']}, published={result['published']}, "
            f"skipped={result['skipped']}, failed={result['failed']}"
        )

        return {
            "statusCode": 200,
            "body": json.dumps(
                {
                    "message": "News crawling and publishing completed",
                    "source": source,
                    "result": result,
                },
                ensure_ascii=False,
            ),
        }

    except Exception as exc:
        logger.error(
            f"Lambda 실행 오류: {exc}\n{traceback.format_exc()}"
        )
        return {
            "statusCode": 500,
            "body": json.dumps(
                {
                    "message": "News crawling failed",
                    "error": str(exc),
                    "source": source,
                },
                ensure_ascii=False,
            ),
        }


def _warn_if_timeout_near(context, phase: str = "") -> None:
    """
    Lambda context가 있고 남은 실행 시간이 안전 마진 이하이면 경고를 출력한다.
    context가 None이거나 메서드가 없는 경우(로컬 실행)는 무시한다.
    """
    try:
        remaining_ms: int = context.get_remaining_time_in_millis()
        if remaining_ms <= _TIMEOUT_SAFETY_MARGIN_MS:
            logger.warning(
                f"[{phase}] Lambda 타임아웃 임박 — 남은 시간: {remaining_ms}ms "
                f"(안전 마진: {_TIMEOUT_SAFETY_MARGIN_MS}ms)"
            )
        else:
            logger.info(f"[{phase}] 남은 Lambda 실행 시간: {remaining_ms}ms")
    except (AttributeError, TypeError):
        # 로컬 테스트 등 context가 없는 환경에서는 무시
        pass
