"""
test_naver_spider.py
NaverFinanceNewsCrawler의 단위 테스트 (시나리오 NS-29~NS-39)

Scrapy의 HtmlResponse를 직접 생성하여 실제 HTTP 요청 없이 테스트한다.
parse_article()의 결과는 generator이므로 list()로 소비한다.
"""

from datetime import datetime

import pytest
from scrapy.http import HtmlResponse

from naver_crawler import NaverFinanceNewsCrawler


# ---------------------------------------------------------------------------
# 헬퍼: 테스트용 HTML → HtmlResponse 변환
# ---------------------------------------------------------------------------

def _make_response(url: str, html: str) -> HtmlResponse:
    """HTML 문자열로부터 Scrapy HtmlResponse를 생성한다."""
    return HtmlResponse(url=url, body=html, encoding="utf-8")


def _make_spider(max_articles: int = 10, count: int = 0) -> NaverFinanceNewsCrawler:
    """
    테스트용 NaverFinanceNewsCrawler 인스턴스를 반환한다.
    CrawlerProcess 없이 직접 인스턴스화한다.
    """
    spider = NaverFinanceNewsCrawler()
    spider.max_articles = max_articles
    spider.count = count
    return spider


# ---------------------------------------------------------------------------
# 공통 HTML 템플릿 헬퍼
# ---------------------------------------------------------------------------

def _article_html(
    title: str = "테스트 기사 제목",
    content: str = "기사 본문 내용입니다.",
    date_attr: str = "2025-10-23T20:37:26",
    press_block: str = "",
) -> str:
    """테스트용 기사 HTML을 생성한다."""
    date_span = (
        f'<span class="media_end_head_info_datestamp_time _ARTICLE_DATE_TIME"'
        f' data-date-time="{date_attr}"></span>'
        if date_attr
        else ""
    )
    return f"""
    <html><body>
      {press_block}
      <h2 class="media_end_head_headline">{title}</h2>
      <div class="go_trans _article_content">{content}</div>
      {date_span}
    </body></html>
    """


# ===========================================================================
# press 추출 — 시나리오 29~31
# ===========================================================================

class TestPressExtraction:

    def test_press_from_logo_alt(self):
        """
        [29] .media_end_head_top_logo img[alt="연합뉴스"]가 있으면
        press="연합뉴스"가 yield되어야 한다.
        """
        # Arrange
        press_block = '<div class="media_end_head_top_logo"><img alt="연합뉴스" src="logo.png"></div>'
        html = _article_html(press_block=press_block)
        response = _make_response("https://example.com/article/1", html)
        spider = _make_spider()

        # Act
        items = list(spider.parse_article(response))

        # Assert
        assert len(items) == 1, "정확히 1개의 기사가 yield되어야 함"
        assert items[0]["press"] == "연합뉴스", \
            "img alt에서 언론사명 '연합뉴스'가 추출되어야 함"

    def test_press_fallback_to_text(self):
        """
        [30] logo img의 alt가 없고 .media_end_head_top_logo 텍스트가 있으면
        해당 텍스트가 press로 yield되어야 한다.
        """
        # Arrange
        # img 없이 직접 텍스트 노드만 존재
        press_block = '<div class="media_end_head_top_logo">조선일보</div>'
        html = _article_html(press_block=press_block)
        response = _make_response("https://example.com/article/2", html)
        spider = _make_spider()

        # Act
        items = list(spider.parse_article(response))

        # Assert
        assert items[0]["press"] == "조선일보", \
            "logo img alt가 없을 때 div 텍스트에서 언론사명이 추출되어야 함"

    def test_press_fallback_to_unknown(self):
        """
        [31] logo img도 텍스트도 없으면 press="알 수 없음"이 yield되어야 한다.
        """
        # Arrange
        # .media_end_head_top_logo 자체가 없음
        html = _article_html(press_block="")
        response = _make_response("https://example.com/article/3", html)
        spider = _make_spider()

        # Act
        items = list(spider.parse_article(response))

        # Assert
        assert items[0]["press"] == "알 수 없음", \
            "logo가 없을 때 press는 '알 수 없음'이어야 함"


# ===========================================================================
# yield 필드 검증 — 시나리오 32~33
# ===========================================================================

class TestYieldFields:

    def test_summary_status_not_in_yield(self):
        """
        [32] yield된 딕셔너리에 "summary_status" 키가 없어야 한다.
        MongoDB 저장 시절의 잔재 필드가 제거됐는지 확인한다.
        """
        # Arrange
        html = _article_html()
        response = _make_response("https://example.com/article/4", html)
        spider = _make_spider()

        # Act
        items = list(spider.parse_article(response))

        # Assert
        assert "summary_status" not in items[0], \
            "summary_status 필드는 yield에서 제거되어야 함"

    def test_content_truncation_ready(self):
        """
        [33] spider는 60,000자 content를 그대로 yield해야 한다.
        truncation은 article_publisher.py의 책임이다.
        """
        # Arrange
        long_content = "가" * 60_000
        html = _article_html(content=long_content)
        response = _make_response("https://example.com/article/5", html)
        spider = _make_spider()

        # Act
        items = list(spider.parse_article(response))

        # Assert
        assert len(items[0]["content"]) == 60_000, \
            "spider는 content를 truncation 없이 원본 그대로 yield해야 함"


# ===========================================================================
# 날짜 변환 — 시나리오 34~35
# ===========================================================================

class TestDateHandling:

    def test_date_format_space_to_T(self):
        """
        [34] data-date-time 속성이 "2025-10-23 20:37:26" 형식(공백 구분)이면
        publishedAt이 "2025-10-23T20:37:26"(T 구분)이어야 한다.
        """
        # Arrange
        html = _article_html(date_attr="2025-10-23 20:37:26")
        response = _make_response("https://example.com/article/6", html)
        spider = _make_spider()

        # Act
        items = list(spider.parse_article(response))

        # Assert
        assert items[0]["publishedAt"] == "2025-10-23T20:37:26", \
            "공백 구분 날짜가 ISO 8601 T 구분 형식으로 변환되어야 함"

    def test_date_none_when_missing(self):
        """
        [35] data-date-time 속성이 없으면 publishedAt이 None이어야 한다.
        """
        # Arrange: date_attr="" → date_span 생략
        html = _article_html(date_attr="")
        response = _make_response("https://example.com/article/7", html)
        spider = _make_spider()

        # Act
        items = list(spider.parse_article(response))

        # Assert
        assert items[0]["publishedAt"] is None, \
            "날짜 속성이 없을 때 publishedAt은 None이어야 함"


# ===========================================================================
# 기사 개수 제한 — 시나리오 36
# ===========================================================================

class TestArticleCountLimit:

    def test_article_count_limit(self):
        """
        [36] max_articles=2이고 count가 이미 2이면
        parse_article에서 yield가 발생하지 않아야 한다.
        """
        # Arrange
        html = _article_html()
        response = _make_response("https://example.com/article/99", html)
        spider = _make_spider(max_articles=2, count=2)  # 이미 한도 도달

        # Act
        items = list(spider.parse_article(response))

        # Assert
        assert len(items) == 0, \
            "max_articles 한도에 도달했을 때 추가 yield가 발생하면 안 됨"


# ===========================================================================
# 증분 크롤링 (since_dt) — 시나리오 NS-37 ~ NS-39
# ===========================================================================

class TestIncrementalCrawling:
    """
    parse_article()이 BaseNewsSpider._should_skip_by_date()를 올바르게 호출하여
    since_dt 기준으로 기사를 필터링하는지 검증한다.
    """

    def test_article_after_since_dt_is_yielded(self):
        """
        [NS-37] since_dt=2025-01-01이고 기사 날짜가 2025-01-02이면
        기사가 yield되어야 한다.
        """
        # Arrange
        html = _article_html(date_attr="2025-01-02T10:00:00")
        response = _make_response("https://example.com/new-article", html)
        spider = _make_spider()
        spider.since_dt = datetime(2025, 1, 1, 0, 0, 0)

        # Act
        items = list(spider.parse_article(response))

        # Assert
        assert len(items) == 1, \
            "since_dt 이후 기사는 yield되어야 함"
        assert items[0]["url"] == "https://example.com/new-article"

    def test_article_before_since_dt_is_skipped(self):
        """
        [NS-38] since_dt=2025-02-01이고 기사 날짜가 2025-01-15이면
        기사가 yield되지 않아야 한다.
        """
        # Arrange
        html = _article_html(date_attr="2025-01-15T10:00:00")
        response = _make_response("https://example.com/old-article", html)
        spider = _make_spider()
        spider.since_dt = datetime(2025, 2, 1, 0, 0, 0)

        # Act
        items = list(spider.parse_article(response))

        # Assert
        assert len(items) == 0, \
            "since_dt 이전 기사는 yield 없이 skip되어야 함"

    def test_article_with_no_date_is_not_skipped_even_with_since_dt(self):
        """
        [NS-39] since_dt가 설정돼 있어도 기사 날짜가 없으면(publishedAt=None)
        기사가 yield되어야 한다 — 날짜 불명 기사는 포함 방향으로 처리한다.
        """
        # Arrange — date_attr="" → date_span 없음 → publishedAt=None
        html = _article_html(date_attr="")
        response = _make_response("https://example.com/no-date-article", html)
        spider = _make_spider()
        spider.since_dt = datetime(2025, 12, 31, 0, 0, 0)  # 아주 최근으로 설정

        # Act
        items = list(spider.parse_article(response))

        # Assert
        assert len(items) == 1, \
            "날짜 정보가 없는 기사는 since_dt와 관계없이 포함되어야 함"
        assert items[0]["publishedAt"] is None
