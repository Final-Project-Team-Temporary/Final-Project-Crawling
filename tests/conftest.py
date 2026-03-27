"""
conftest.py — 공통 pytest fixture 정의
"""

import pytest
import fakeredis
from unittest.mock import MagicMock


# ---------------------------------------------------------------------------
# 전역 상태 격리: _redis_client를 매 테스트 전 None으로 초기화
# ---------------------------------------------------------------------------

@pytest.fixture(autouse=True)
def reset_global_redis(monkeypatch):
    """
    article_publisher 모듈의 전역 _redis_client를 각 테스트 전후 None으로 보장한다.
    monkeypatch를 사용하므로 테스트 종료 시 자동 복원된다.
    """
    import article_publisher
    monkeypatch.setattr(article_publisher, "_redis_client", None)


# ---------------------------------------------------------------------------
# Redis 목(Mock) 관련 fixture
# ---------------------------------------------------------------------------

@pytest.fixture
def fake_redis():
    """
    fakeredis.FakeRedis 인스턴스 반환. 각 테스트마다 새 인스턴스를 생성한다.
    decode_responses=True로 설정해 실제 redis-py와 동일한 인터페이스를 제공한다.
    """
    return fakeredis.FakeRedis(decode_responses=True)


# ---------------------------------------------------------------------------
# 환경변수 fixture
# ---------------------------------------------------------------------------

@pytest.fixture
def env_vars(monkeypatch, tmp_path):
    """
    테스트용 환경변수를 monkeypatch로 설정한다.
    OUTPUT_FILE_PATH는 pytest tmp_path를 사용하여 실제 /tmp 경로 오염을 방지한다.
    """
    output_file = str(tmp_path / "output.json")
    vars_map = {
        "REDIS_HOST":               "localhost",
        "REDIS_PORT":               "6379",
        "REDIS_USE_TLS":            "false",
        "REDIS_SSL_CERT_REQS":      "required",
        "REDIS_ARTICLE_STREAM_KEY": "test:articles:stream",
        "REDIS_PUBLISHED_URLS_KEY": "test:published_urls",
        "OUTPUT_FILE_PATH":         output_file,
    }
    for key, value in vars_map.items():
        monkeypatch.setenv(key, value)
    return vars_map


# ---------------------------------------------------------------------------
# subprocess fixture
# ---------------------------------------------------------------------------

@pytest.fixture
def mock_subprocess_success(mocker):
    """
    article_publisher.subprocess.run을 mock하여 returncode=0을 반환한다.
    출력 파일은 별도로 생성해야 한다(side_effect 사용).
    """
    mock = mocker.patch("article_publisher.subprocess.run")
    mock.return_value = MagicMock(returncode=0, stdout="crawl done", stderr="")
    return mock


# ---------------------------------------------------------------------------
# 테스트 데이터 fixture
# ---------------------------------------------------------------------------

@pytest.fixture
def sample_articles():
    """
    테스트용 기사 dict 목록 3개.
    - [0]: 중복(skipped) 시나리오에서 사용
    - [1]: 발행 성공 시나리오에서 사용
    - [2]: 발행 실패 시나리오에서 사용
    """
    return [
        {
            "url":         "https://example.com/article/1",
            "title":       "기사 제목 1",
            "content":     "기사 본문 1",
            "publishedAt": "2025-01-01T00:00:00",
            "press":       "연합뉴스",
        },
        {
            "url":         "https://example.com/article/2",
            "title":       "기사 제목 2",
            "content":     "기사 본문 2",
            "publishedAt": "2025-01-02T00:00:00",
            "press":       "조선일보",
        },
        {
            "url":         "https://example.com/article/3",
            "title":       "기사 제목 3",
            "content":     "기사 본문 3",
            "publishedAt": "2025-01-03T00:00:00",
            "press":       "동아일보",
        },
    ]
