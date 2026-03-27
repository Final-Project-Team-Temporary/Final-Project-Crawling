"""
test_article_publisher.py
article_publisher 모듈의 단위 테스트 (시나리오 AP-01 ~ AP-35)
"""

import json
import os
from datetime import datetime

import pytest
from unittest.mock import MagicMock, call

import article_publisher


# ===========================================================================
# get_redis_client() — 시나리오 1~4
# ===========================================================================

class TestGetRedisClient:

    def test_warm_start_reuse(self, mocker, env_vars):
        """
        [1] 전역 _redis_client가 존재하고 ping이 성공하면
        Redis 생성자가 재호출되지 않아야 한다.
        """
        # Arrange
        mock_client = MagicMock()
        mock_client.ping.return_value = True
        article_publisher._redis_client = mock_client

        mock_redis_cls = mocker.patch("article_publisher.redis_lib.Redis")

        # Act
        result = article_publisher.get_redis_client()

        # Assert
        mock_redis_cls.assert_not_called(), "warm start: Redis 생성자가 호출되면 안 됨"
        assert result is mock_client, "기존 클라이언트를 그대로 반환해야 함"

    def test_reconnect_on_ping_failure(self, mocker, env_vars):
        """
        [2] 전역 _redis_client가 존재하지만 ping이 Exception을 던지면
        클라이언트를 새로 생성해야 한다.
        """
        # Arrange
        old_client = MagicMock()
        old_client.ping.side_effect = Exception("connection lost")
        article_publisher._redis_client = old_client

        new_client = MagicMock()
        new_client.ping.return_value = True
        mock_redis_cls = mocker.patch(
            "article_publisher.redis_lib.Redis", return_value=new_client
        )

        # Act
        result = article_publisher.get_redis_client()

        # Assert
        mock_redis_cls.assert_called_once(), "ping 실패 후 Redis 생성자가 1회 호출되어야 함"
        assert result is new_client, "새로 생성된 클라이언트가 반환되어야 함"

    def test_exponential_backoff_retry(self, mocker, env_vars):
        """
        [3] Redis 연결이 1,2회차 실패하고 3회차에 성공하면
        time.sleep이 1초, 2초 순서로 호출되어야 한다.
        """
        # Arrange
        fail_client = MagicMock()
        fail_client.ping.side_effect = Exception("connection refused")

        success_client = MagicMock()
        success_client.ping.return_value = True

        mocker.patch(
            "article_publisher.redis_lib.Redis",
            side_effect=[fail_client, fail_client, success_client],
        )
        mock_sleep = mocker.patch("article_publisher.time.sleep")

        # Act
        result = article_publisher.get_redis_client()

        # Assert
        assert mock_sleep.call_count == 2, "sleep은 정확히 2회(1차, 2차 실패 후) 호출되어야 함"
        assert mock_sleep.call_args_list[0] == call(1), "1차 실패 후 1초 대기해야 함"
        assert mock_sleep.call_args_list[1] == call(2), "2차 실패 후 2초 대기해야 함"
        assert result is success_client, "3회차 성공 시 해당 클라이언트가 반환되어야 함"

    def test_all_retries_failed(self, mocker, env_vars):
        """
        [4] 3회 모두 연결 실패 시 ConnectionError가 발생해야 한다.
        """
        # Arrange
        fail_client = MagicMock()
        fail_client.ping.side_effect = Exception("always fail")

        mocker.patch("article_publisher.redis_lib.Redis", return_value=fail_client)
        mocker.patch("article_publisher.time.sleep")

        # Act & Assert
        with pytest.raises(ConnectionError, match="Redis 연결 실패"):
            article_publisher.get_redis_client()


# ===========================================================================
# load_published_urls() — 시나리오 5~6
# ===========================================================================

class TestLoadPublishedUrls:

    def test_normal_load(self, fake_redis, env_vars):
        """
        [5] fakeredis Set에 URL 3개가 있으면 반환된 set의 크기가 3이어야 한다.
        """
        # Arrange
        key = env_vars["REDIS_PUBLISHED_URLS_KEY"]
        fake_redis.sadd(key, "https://a.com/1", "https://a.com/2", "https://a.com/3")

        # Act
        result = article_publisher.load_published_urls(fake_redis)

        # Assert
        assert len(result) == 3, "Redis Set의 3개 URL이 모두 로드되어야 함"
        assert "https://a.com/1" in result, "각 URL이 캐시에 포함되어야 함"

    def test_redis_error_returns_empty_set(self, mocker, env_vars):
        """
        [6] smembers가 Exception을 던지면 빈 set을 반환해야 한다 (예외 전파 금지).
        """
        # Arrange
        mock_client = MagicMock()
        mock_client.smembers.side_effect = Exception("redis unavailable")

        # Act
        result = article_publisher.load_published_urls(mock_client)

        # Assert
        assert result == set(), "Redis 오류 시 빈 set을 반환해야 함"


# ===========================================================================
# is_duplicate() — 시나리오 7~8
# ===========================================================================

class TestIsDuplicate:

    def test_url_in_cache_returns_true(self):
        """
        [7] URL이 캐시에 존재하면 True를 반환해야 한다.
        """
        # Arrange
        cache = {"https://example.com/1", "https://example.com/2"}

        # Act & Assert
        assert article_publisher.is_duplicate("https://example.com/1", cache) is True, \
            "캐시에 있는 URL은 True를 반환해야 함"

    def test_url_not_in_cache_returns_false(self):
        """
        [8] URL이 캐시에 없으면 False를 반환해야 한다.
        """
        # Arrange
        cache = {"https://example.com/1"}

        # Act & Assert
        assert article_publisher.is_duplicate("https://example.com/999", cache) is False, \
            "캐시에 없는 URL은 False를 반환해야 함"


# ===========================================================================
# publish_article() — 시나리오 9~14
# ===========================================================================

class TestPublishArticle:

    def _make_article(self, url="https://example.com/1", content="기사 본문"):
        return {
            "url":         url,
            "title":       "테스트 제목",
            "content":     content,
            "publishedAt": "2025-01-01T00:00:00",
            "press":       "연합뉴스",
        }

    def test_success_publishes_and_updates_cache(self, fake_redis, env_vars):
        """
        [9] xadd 성공 시 Stream에 메시지, Set에 URL, 메모리 캐시에 URL이 추가되어야 한다.
        """
        # Arrange
        article = self._make_article()
        cache: set = set()
        stream_key = env_vars["REDIS_ARTICLE_STREAM_KEY"]
        urls_key   = env_vars["REDIS_PUBLISHED_URLS_KEY"]

        # Act
        result = article_publisher.publish_article(fake_redis, article, cache)

        # Assert
        assert result is True, "발행 성공 시 True를 반환해야 함"
        messages = fake_redis.xrange(stream_key)
        assert len(messages) == 1, "Stream에 정확히 1개의 메시지가 추가되어야 함"
        assert fake_redis.sismember(urls_key, article["url"]), "Redis Set에 URL이 추가되어야 함"
        assert article["url"] in cache, "메모리 캐시에 URL이 추가되어야 함"

    def test_content_truncated_at_50000_chars(self, fake_redis, env_vars):
        """
        [10] 50,001자 content를 발행하면 Stream에 저장된 content가 정확히 50,000자여야 한다.
        """
        # Arrange
        article = self._make_article(content="가" * 50_001)
        cache: set = set()
        stream_key = env_vars["REDIS_ARTICLE_STREAM_KEY"]

        # Act
        article_publisher.publish_article(fake_redis, article, cache)

        # Assert
        messages = fake_redis.xrange(stream_key)
        stored_content = messages[0][1]["content"]
        assert len(stored_content) == 50_000, \
            f"content는 50,000자로 truncation되어야 함 (실제: {len(stored_content)}자)"

    def test_content_none_handled(self, fake_redis, env_vars):
        """
        [11] article["content"] = None일 때 예외 없이 빈 문자열로 발행되어야 한다.
        """
        # Arrange
        article = self._make_article(content=None)
        article["content"] = None
        cache: set = set()
        stream_key = env_vars["REDIS_ARTICLE_STREAM_KEY"]

        # Act
        result = article_publisher.publish_article(fake_redis, article, cache)

        # Assert
        assert result is True, "content=None이어도 발행에 성공해야 함"
        messages = fake_redis.xrange(stream_key)
        assert messages[0][1]["content"] == "", "content=None은 빈 문자열로 저장되어야 함"

    def test_redis_failure_returns_false_no_exception(self, mocker, env_vars):
        """
        [12] xadd가 Exception을 던져도 False를 반환하고 예외가 전파되지 않아야 한다.
        """
        # Arrange
        mock_client = MagicMock()
        mock_client.xadd.side_effect = Exception("xadd failed")
        article = self._make_article()
        cache: set = set()

        # Act
        result = article_publisher.publish_article(mock_client, article, cache)

        # Assert
        assert result is False, "xadd 실패 시 False를 반환해야 함"
        assert article["url"] not in cache, "실패 시 캐시에 URL이 추가되면 안 됨"

    def test_ttl_refreshed_on_publish(self, fake_redis, env_vars):
        """
        [13] 발행 성공 시 PUBLISHED_URLS_KEY의 TTL이 7일(604800초) 이상이어야 한다.
        """
        # Arrange
        article = self._make_article()
        cache: set = set()
        urls_key = env_vars["REDIS_PUBLISHED_URLS_KEY"]

        # Act
        article_publisher.publish_article(fake_redis, article, cache)

        # Assert
        ttl = fake_redis.ttl(urls_key)
        assert ttl >= 604_799, \
            f"TTL은 7일(604800초) 이상이어야 함 (실제: {ttl}초)"

    def test_maxlen_applied(self, mocker, fake_redis, env_vars):
        """
        [14] xadd 호출 시 maxlen=10000, approximate=True 인수가 전달되어야 한다.
        """
        # Arrange
        spy = mocker.spy(fake_redis, "xadd")
        article = self._make_article()
        cache: set = set()

        # Act
        article_publisher.publish_article(fake_redis, article, cache)

        # Assert
        spy.assert_called_once()
        call_kwargs = spy.call_args.kwargs
        assert call_kwargs.get("maxlen") == 10_000, \
            "xadd maxlen은 10000이어야 함"
        assert call_kwargs.get("approximate") is True, \
            "xadd approximate는 True여야 함"


# ===========================================================================
# run_crawler() — 시나리오 15~19
# ===========================================================================

class TestRunCrawler:

    def test_normal_execution_parses_jsonl(self, mocker, env_vars):
        """
        [15] subprocess.run이 returncode=0이고 2행 JSONL 파일이 생성되면
        article dict 목록 2개가 반환되어야 한다.
        """
        # Arrange
        output_path = env_vars["OUTPUT_FILE_PATH"]

        def create_output_file(*args, **kwargs):
            with open(output_path, "w", encoding="utf-8") as f:
                f.write('{"url": "https://example.com/1", "title": "기사 1"}\n')
                f.write('{"url": "https://example.com/2", "title": "기사 2"}\n')
            return MagicMock(returncode=0, stdout="done", stderr="")

        mocker.patch("article_publisher.subprocess.run", side_effect=create_output_file)

        # Act
        result = article_publisher.run_crawler()

        # Assert
        assert len(result) == 2, "JSONL 2행에 대응하는 기사 2건이 반환되어야 함"
        assert result[0]["url"] == "https://example.com/1", "첫 번째 기사 url이 일치해야 함"

    def test_nonzero_returncode_raises(self, mocker, env_vars):
        """
        [16] subprocess.run이 returncode=1을 반환하면 RuntimeError가 발생해야 한다.
        """
        # Arrange
        mocker.patch(
            "article_publisher.subprocess.run",
            return_value=MagicMock(returncode=1, stdout="", stderr="spider crash"),
        )

        # Act & Assert
        with pytest.raises(RuntimeError, match="returncode=1"):
            article_publisher.run_crawler()

    def test_missing_output_file_returns_empty(self, mocker, env_vars):
        """
        [17] subprocess.run이 성공했지만 출력 파일이 없으면 빈 리스트를 반환해야 한다.
        """
        # Arrange
        mocker.patch(
            "article_publisher.subprocess.run",
            return_value=MagicMock(returncode=0, stdout="", stderr=""),
        )
        # 파일을 생성하지 않음

        # Act
        result = article_publisher.run_crawler()

        # Assert
        assert result == [], "출력 파일 없을 때 빈 리스트를 반환해야 함"

    def test_invalid_json_line_skipped(self, mocker, env_vars):
        """
        [18] JSONL 파일에 잘못된 JSON 라인이 섞여 있으면
        해당 라인만 건너뛰고 나머지가 정상 파싱되어야 한다.
        """
        # Arrange
        output_path = env_vars["OUTPUT_FILE_PATH"]

        def create_mixed_output(*args, **kwargs):
            with open(output_path, "w", encoding="utf-8") as f:
                f.write('{"url": "https://example.com/1", "title": "정상1"}\n')
                f.write('INVALID JSON { broken\n')
                f.write('{"url": "https://example.com/2", "title": "정상2"}\n')
            return MagicMock(returncode=0, stdout="", stderr="")

        mocker.patch("article_publisher.subprocess.run", side_effect=create_mixed_output)

        # Act
        result = article_publisher.run_crawler()

        # Assert
        assert len(result) == 2, "잘못된 JSON 라인 1건은 건너뛰고 나머지 2건이 파싱되어야 함"
        assert all("url" in a for a in result), "정상 파싱된 기사에 url 필드가 있어야 함"

    def test_crawl_since_env_set_when_since_dt_provided(self, mocker, env_vars):
        """
        [AP-24] since_dt를 전달하면 subprocess에 CRAWL_SINCE 환경변수가
        ISO 8601 형식으로 설정되어야 한다.
        """
        # Arrange
        since = datetime(2025, 1, 15, 12, 0, 0)
        captured_envs: list[dict] = []

        def capture_env(*args, **kwargs):
            captured_envs.append(dict(kwargs.get("env", {})))
            return MagicMock(returncode=0, stdout="", stderr="")

        mocker.patch("article_publisher.subprocess.run", side_effect=capture_env)

        # Act
        article_publisher.run_crawler(since_dt=since)

        # Assert
        assert len(captured_envs) == 1
        assert "CRAWL_SINCE" in captured_envs[0], \
            "since_dt를 전달하면 subprocess 환경에 CRAWL_SINCE가 있어야 함"
        assert captured_envs[0]["CRAWL_SINCE"] == "2025-01-15T12:00:00", \
            "CRAWL_SINCE는 ISO 8601 형식이어야 함"

    def test_crawl_since_removed_when_since_dt_is_none(self, mocker, env_vars, monkeypatch):
        """
        [AP-25] since_dt=None이면 CRAWL_SINCE가 subprocess 환경에 없어야 한다.
        이전 실행에서 CRAWL_SINCE가 환경변수에 남아 있어도 제거되어야 한다.
        """
        # Arrange — 이미 CRAWL_SINCE가 환경에 존재하는 상황
        monkeypatch.setenv("CRAWL_SINCE", "2025-01-01T00:00:00")
        captured_envs: list[dict] = []

        def capture_env(*args, **kwargs):
            captured_envs.append(dict(kwargs.get("env", {})))
            return MagicMock(returncode=0, stdout="", stderr="")

        mocker.patch("article_publisher.subprocess.run", side_effect=capture_env)

        # Act
        article_publisher.run_crawler(since_dt=None)

        # Assert
        assert len(captured_envs) == 1
        assert "CRAWL_SINCE" not in captured_envs[0], \
            "since_dt=None이면 subprocess 환경에서 CRAWL_SINCE가 제거되어야 함"

    def test_previous_output_file_deleted(self, mocker, env_vars):
        """
        [AP-19] run_crawler 호출 전 OUTPUT_FILE_PATH에 파일이 존재하면
        subprocess 실행 전에 해당 파일이 삭제되어야 한다.
        """
        # Arrange
        output_path = env_vars["OUTPUT_FILE_PATH"]

        # 이전 실행의 파일 생성
        with open(output_path, "w") as f:
            f.write("old content")
        assert os.path.exists(output_path), "사전 조건: 파일이 존재해야 함"

        was_deleted_before_subprocess: list[bool] = []

        def check_deletion_then_succeed(*args, **kwargs):
            # subprocess.run이 호출되는 시점에 파일이 이미 삭제됐는지 확인
            was_deleted_before_subprocess.append(not os.path.exists(output_path))
            return MagicMock(returncode=0, stdout="", stderr="")

        mocker.patch("article_publisher.subprocess.run", side_effect=check_deletion_then_succeed)

        # Act
        article_publisher.run_crawler()

        # Assert
        assert was_deleted_before_subprocess[0] is True, \
            "subprocess 실행 전에 이전 출력 파일이 삭제되어야 함"


# ===========================================================================
# crawl_and_publish() — 시나리오 20~23
# ===========================================================================

class TestCrawlAndPublish:

    def test_normal_flow_counts(self, mocker, env_vars, sample_articles):
        """
        [20] 기사 3개 중 1개 중복, 1개 성공, 1개 실패이면
        {"crawled":3, "published":1, "skipped":1, "failed":1}을 반환해야 한다.
        """
        # Arrange
        mocker.patch("article_publisher.run_crawler", return_value=sample_articles)
        mocker.patch("article_publisher.get_redis_client", return_value=MagicMock())
        # article[0]만 캐시에 포함 → skipped
        mocker.patch(
            "article_publisher.load_published_urls",
            return_value={sample_articles[0]["url"]},
        )
        # article[1] 성공, article[2] 실패
        def publish_side_effect(client, article, cache):
            if article["url"] == sample_articles[1]["url"]:
                cache.add(article["url"])
                return True
            return False

        mocker.patch("article_publisher.publish_article", side_effect=publish_side_effect)
        mocker.patch("article_publisher._save_failed_articles")

        # Act
        result = article_publisher.crawl_and_publish()

        # Assert
        assert result == {"crawled": 3, "published": 1, "skipped": 1, "failed": 1}, \
            f"예상 카운트와 다름: {result}"

    def test_redis_connection_failure_saves_all_as_failed(
        self, mocker, env_vars, sample_articles, tmp_path
    ):
        """
        [21] get_redis_client가 ConnectionError를 던지면
        {"crawled":3, "published":0, "skipped":0, "failed":3}을 반환하고
        실패 파일이 생성되어야 한다.
        """
        # Arrange
        failed_path = str(tmp_path / "failed_articles.json")
        mocker.patch.object(article_publisher, "FAILED_ARTICLES_PATH", failed_path)
        mocker.patch("article_publisher.run_crawler", return_value=sample_articles)
        mocker.patch(
            "article_publisher.get_redis_client",
            side_effect=ConnectionError("redis unreachable"),
        )

        # Act
        result = article_publisher.crawl_and_publish()

        # Assert
        assert result == {"crawled": 3, "published": 0, "skipped": 0, "failed": 3}, \
            f"Redis 연결 실패 시 전체 실패 처리되어야 함: {result}"
        assert os.path.exists(failed_path), \
            "Redis 연결 실패 시 실패 기사 파일이 생성되어야 함"

    def test_failed_articles_saved_to_tmp(self, mocker, env_vars, tmp_path):
        """
        [22] 발행 실패 기사가 1건이면 실패 파일이 생성되고 해당 기사가 포함되어야 한다.
        """
        # Arrange
        failed_path = str(tmp_path / "failed_articles.json")
        mocker.patch.object(article_publisher, "FAILED_ARTICLES_PATH", failed_path)

        articles = [{
            "url":         "https://example.com/fail",
            "title":       "실패 기사",
            "content":     "본문",
            "publishedAt": "2025-01-01T00:00:00",
            "press":       "테스트",
        }]
        mocker.patch("article_publisher.run_crawler", return_value=articles)
        mocker.patch("article_publisher.get_redis_client", return_value=MagicMock())
        mocker.patch("article_publisher.load_published_urls", return_value=set())
        mocker.patch("article_publisher.publish_article", return_value=False)

        # Act
        article_publisher.crawl_and_publish()

        # Assert
        assert os.path.exists(failed_path), "실패 기사 파일이 생성되어야 함"
        with open(failed_path, encoding="utf-8") as f:
            saved = json.load(f)
        assert len(saved) == 1, "실패 기사 1건이 파일에 저장되어야 함"
        assert saved[0]["url"] == "https://example.com/fail", \
            "저장된 기사의 url이 일치해야 함"

    def test_no_failed_file_when_all_success(self, mocker, env_vars, tmp_path):
        """
        [AP-23] 모든 기사가 성공적으로 발행되면 실패 파일이 생성되지 않아야 한다.
        """
        # Arrange
        failed_path = str(tmp_path / "failed_articles.json")
        mocker.patch.object(article_publisher, "FAILED_ARTICLES_PATH", failed_path)

        articles = [{
            "url":         "https://example.com/success",
            "title":       "성공 기사",
            "content":     "본문",
            "publishedAt": "2025-01-01T00:00:00",
            "press":       "테스트",
        }]
        mocker.patch("article_publisher.run_crawler", return_value=articles)
        mocker.patch("article_publisher.get_redis_client", return_value=MagicMock())
        mocker.patch("article_publisher.load_published_urls", return_value=set())
        mocker.patch("article_publisher.publish_article", return_value=True)

        # Act
        article_publisher.crawl_and_publish()

        # Assert
        assert not os.path.exists(failed_path), \
            "전체 발행 성공 시 실패 파일이 생성되면 안 됨"


# ===========================================================================
# get_last_crawl_time() — 시나리오 AP-26 ~ AP-29
# ===========================================================================

class TestGetLastCrawlTime:

    def test_returns_datetime_when_key_has_value(self, fake_redis, env_vars_with_last_crawl):
        """
        [AP-26] REDIS_LAST_CRAWL_KEY에 ISO 8601 datetime이 저장돼 있으면
        해당 값을 datetime으로 반환해야 한다.
        """
        # Arrange
        key = env_vars_with_last_crawl["REDIS_LAST_CRAWL_KEY"]
        fake_redis.set(key, "2025-03-01T10:00:00")

        # Act
        result = article_publisher.get_last_crawl_time(fake_redis)

        # Assert
        assert result == datetime(2025, 3, 1, 10, 0, 0), \
            "저장된 ISO 8601 값이 datetime으로 변환되어야 함"

    def test_returns_none_when_key_has_no_value(self, fake_redis, env_vars_with_last_crawl):
        """
        [AP-27] REDIS_LAST_CRAWL_KEY가 설정됐지만 Redis에 값이 없으면 None을 반환해야 한다.
        초기 실행(첫 번째 크롤링)에 해당한다.
        """
        # Act
        result = article_publisher.get_last_crawl_time(fake_redis)

        # Assert
        assert result is None, \
            "Redis에 값이 없으면 None을 반환하여 전체 크롤링이 수행되어야 함"

    def test_returns_none_when_env_key_not_set(self, fake_redis, env_vars):
        """
        [AP-28] REDIS_LAST_CRAWL_KEY 환경변수가 없으면 None을 반환해야 한다.
        증분 크롤링이 비활성화된 것으로 간주한다.
        """
        # env_vars fixture에는 REDIS_LAST_CRAWL_KEY가 포함되지 않음
        # Act
        result = article_publisher.get_last_crawl_time(fake_redis)

        # Assert
        assert result is None, \
            "REDIS_LAST_CRAWL_KEY가 없으면 None을 반환해야 함"

    def test_returns_none_on_redis_error(self, mocker, env_vars_with_last_crawl):
        """
        [AP-29] Redis .get()이 Exception을 던지면 None을 반환하고 예외가 전파되지 않아야 한다.
        Redis 장애 시 전체 크롤링으로 자동 폴백된다.
        """
        # Arrange
        mock_client = MagicMock()
        mock_client.get.side_effect = Exception("connection lost")

        # Act
        result = article_publisher.get_last_crawl_time(mock_client)

        # Assert
        assert result is None, \
            "Redis 오류 시 None을 반환하고 예외가 전파되면 안 됨"


# ===========================================================================
# update_last_crawl_time() — 시나리오 AP-30 ~ AP-32
# ===========================================================================

class TestUpdateLastCrawlTime:

    def test_success_stores_iso_format_and_returns_true(
        self, fake_redis, env_vars_with_last_crawl
    ):
        """
        [AP-30] datetime을 전달하면 ISO 8601 형식으로 Redis에 저장하고 True를 반환해야 한다.
        """
        # Arrange
        key = env_vars_with_last_crawl["REDIS_LAST_CRAWL_KEY"]
        dt = datetime(2025, 3, 15, 9, 30, 0)

        # Act
        result = article_publisher.update_last_crawl_time(fake_redis, dt)

        # Assert
        assert result is True, "저장 성공 시 True를 반환해야 함"
        stored = fake_redis.get(key)
        assert stored == "2025-03-15T09:30:00", \
            "datetime이 ISO 8601 형식으로 저장되어야 함"

    def test_returns_false_when_env_key_not_set(self, fake_redis, env_vars):
        """
        [AP-31] REDIS_LAST_CRAWL_KEY 환경변수가 없으면 False를 반환하고
        Redis에 아무것도 저장하지 않아야 한다.
        """
        # Arrange
        dt = datetime(2025, 3, 15, 9, 30, 0)

        # Act
        result = article_publisher.update_last_crawl_time(fake_redis, dt)

        # Assert
        assert result is False, \
            "REDIS_LAST_CRAWL_KEY 없을 때 False를 반환해야 함"

    def test_returns_false_on_redis_error(self, mocker, env_vars_with_last_crawl):
        """
        [AP-32] Redis .set()이 Exception을 던지면 False를 반환하고 예외가 전파되지 않아야 한다.
        """
        # Arrange
        mock_client = MagicMock()
        mock_client.set.side_effect = Exception("redis full")
        dt = datetime(2025, 3, 15, 9, 30, 0)

        # Act
        result = article_publisher.update_last_crawl_time(mock_client, dt)

        # Assert
        assert result is False, \
            "Redis 오류 시 False를 반환하고 예외가 전파되면 안 됨"


# ===========================================================================
# crawl_and_publish() 증분 크롤링 흐름 — 시나리오 AP-33 ~ AP-35
# ===========================================================================

class TestCrawlAndPublishIncrementalFlow:

    def test_since_dt_from_redis_is_passed_to_run_crawler(
        self, mocker, env_vars_with_last_crawl, fake_redis
    ):
        """
        [AP-33] REDIS_LAST_CRAWL_KEY에 저장된 시각이 run_crawler(since_dt=...)로
        그대로 전달되어야 한다.

        검증 목적: crawl_and_publish()가 Redis에서 읽은 since_dt를 크롤러에 넘기는
        전체 데이터 흐름이 올바른지 확인한다.
        """
        # Arrange
        key = env_vars_with_last_crawl["REDIS_LAST_CRAWL_KEY"]
        since = datetime(2025, 2, 1, 0, 0, 0)
        fake_redis.set(key, since.isoformat())

        mocker.patch("article_publisher.get_redis_client", return_value=fake_redis)
        mock_run_crawler = mocker.patch("article_publisher.run_crawler", return_value=[])
        mocker.patch("article_publisher.load_published_urls", return_value=set())

        # Act
        article_publisher.crawl_and_publish()

        # Assert
        mock_run_crawler.assert_called_once_with(since), \
            "Redis에서 읽은 since_dt가 run_crawler에 전달되어야 함"

    def test_last_crawl_time_updated_when_articles_crawled(
        self, mocker, env_vars_with_last_crawl, fake_redis, sample_articles
    ):
        """
        [AP-34] 크롤링 기사가 1건 이상이면 crawl_start 시각으로
        last_crawl_time이 업데이트되어야 한다.

        검증 목적: 다음 실행 시 증분 크롤링이 올바른 기준 시각을 사용하는지 보장한다.
        """
        # Arrange
        key = env_vars_with_last_crawl["REDIS_LAST_CRAWL_KEY"]

        mocker.patch("article_publisher.get_redis_client", return_value=fake_redis)
        mocker.patch("article_publisher.run_crawler", return_value=sample_articles[:1])
        mocker.patch("article_publisher.load_published_urls", return_value=set())
        mocker.patch("article_publisher.publish_article", return_value=True)

        # Act
        article_publisher.crawl_and_publish()

        # Assert
        stored = fake_redis.get(key)
        assert stored is not None, \
            "크롤링 성공 후 last_crawl_time이 Redis에 저장되어야 함"
        assert "T" in stored, \
            "저장된 시각은 ISO 8601(T 구분자) 형식이어야 함"
        # 저장된 시각이 파싱 가능한지 검증
        parsed = datetime.fromisoformat(stored)
        assert isinstance(parsed, datetime), \
            "저장된 값이 파싱 가능한 datetime이어야 함"

    def test_last_crawl_time_not_updated_when_no_articles_crawled(
        self, mocker, env_vars_with_last_crawl, fake_redis
    ):
        """
        [AP-35] 크롤링 결과가 0건이면 last_crawl_time이 업데이트되지 않아야 한다.

        검증 목적: 새 기사가 없을 때 since_dt가 앞으로 이동하면 다음 실행에서
        기사를 놓칠 수 있으므로, 0건이면 기준 시각을 그대로 유지해야 한다.
        """
        # Arrange
        key = env_vars_with_last_crawl["REDIS_LAST_CRAWL_KEY"]

        mocker.patch("article_publisher.get_redis_client", return_value=fake_redis)
        mocker.patch("article_publisher.run_crawler", return_value=[])
        mocker.patch("article_publisher.load_published_urls", return_value=set())

        # Act
        article_publisher.crawl_and_publish()

        # Assert
        stored = fake_redis.get(key)
        assert stored is None, \
            "기사 0건 시 last_crawl_time이 저장되면 안 됨 — since_dt를 그대로 유지해야 함"
