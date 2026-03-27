"""
test_lambda_handler.py
lambda_handler 모듈의 단위 테스트 (시나리오 24~28)
"""

import json
import logging
import pytest
from unittest.mock import MagicMock

import lambda_handler


# ===========================================================================
# handler() — 시나리오 24~28
# ===========================================================================

class TestHandler:

    def _make_context(self, remaining_ms: int = 60_000) -> MagicMock:
        """테스트용 Lambda context mock 생성 헬퍼."""
        context = MagicMock()
        context.get_remaining_time_in_millis.return_value = remaining_ms
        return context

    def test_success_returns_200(self, mocker):
        """
        [24] crawl_and_publish가 정상 반환하면
        statusCode=200이고 body에 result 딕셔너리가 포함되어야 한다.
        """
        # Arrange
        mock_result = {"crawled": 3, "published": 2, "skipped": 1, "failed": 0}
        mocker.patch("lambda_handler.crawl_and_publish", return_value=mock_result)
        context = self._make_context()

        # Act
        response = lambda_handler.handler({}, context)

        # Assert
        assert response["statusCode"] == 200, "정상 종료 시 statusCode는 200이어야 함"
        body = json.loads(response["body"])
        assert "result" in body, "응답 body에 result 키가 있어야 함"
        assert body["result"] == mock_result, "result 값이 crawl_and_publish 반환값과 일치해야 함"

    def test_exception_returns_500(self, mocker):
        """
        [25] crawl_and_publish가 Exception을 던지면
        statusCode=500이고 body에 error 메시지가 포함되어야 한다.
        """
        # Arrange
        mocker.patch(
            "lambda_handler.crawl_and_publish",
            side_effect=Exception("critical crawler error"),
        )
        context = self._make_context()

        # Act
        response = lambda_handler.handler({}, context)

        # Assert
        assert response["statusCode"] == 500, "예외 발생 시 statusCode는 500이어야 함"
        body = json.loads(response["body"])
        assert "error" in body, "응답 body에 error 키가 있어야 함"
        assert "critical crawler error" in body["error"], \
            "error 메시지에 예외 내용이 포함되어야 함"

    def test_timeout_warning_logged_when_near(self, mocker, caplog):
        """
        [26] context.get_remaining_time_in_millis()가 10,000을 반환하면
        (안전 마진 15,000 이하) WARNING 레벨 로그가 출력되어야 한다.
        """
        # Arrange
        mocker.patch(
            "lambda_handler.crawl_and_publish",
            return_value={"crawled": 0, "published": 0, "skipped": 0, "failed": 0},
        )
        context = self._make_context(remaining_ms=10_000)  # 15,000ms 안전마진 미달

        # Act
        with caplog.at_level(logging.WARNING, logger="lambda_handler"):
            lambda_handler.handler({}, context)

        # Assert
        warning_messages = [r.message for r in caplog.records if r.levelno == logging.WARNING]
        assert any("타임아웃 임박" in msg for msg in warning_messages), \
            "남은 시간이 안전 마진 이하일 때 '타임아웃 임박' 경고가 출력되어야 함"

    def test_no_error_without_context(self, mocker):
        """
        [27] context=None으로 handler를 호출해도 예외 없이 실행되어야 한다.
        """
        # Arrange
        mocker.patch(
            "lambda_handler.crawl_and_publish",
            return_value={"crawled": 0, "published": 0, "skipped": 0, "failed": 0},
        )

        # Act
        response = lambda_handler.handler({}, None)

        # Assert
        assert response["statusCode"] == 200, \
            "context=None이어도 정상 응답(200)을 반환해야 함"

    def test_source_from_event_included_in_response(self, mocker):
        """
        [28] event={"source": "aws.events"}로 호출하면
        body에 source="aws.events"가 포함되어야 한다.
        """
        # Arrange
        mocker.patch(
            "lambda_handler.crawl_and_publish",
            return_value={"crawled": 0, "published": 0, "skipped": 0, "failed": 0},
        )
        context = self._make_context()

        # Act
        response = lambda_handler.handler({"source": "aws.events"}, context)

        # Assert
        body = json.loads(response["body"])
        assert body["source"] == "aws.events", \
            "이벤트의 source 값이 응답 body에 포함되어야 함"
