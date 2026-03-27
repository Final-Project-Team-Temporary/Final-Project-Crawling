"""
naver_crawler.py
네이버 금융뉴스 스파이더 (BaseNewsSpider 구현체)

크롤링 대상: https://news.naver.com/breakingnews/section/101/259

환경변수 (BaseNewsSpider 공통):
  MAX_ARTICLES   - 최대 수집 기사 수        (기본값: 10)
  MAX_CRAWL_TIME - 크롤링 제한 시간(초)     (기본값: 300)
  CRAWL_SINCE    - 이 시각 이후 기사만 수집  (ISO 8601, 증분 크롤링용)
  OUTPUT_FILE_PATH - 결과 JSONL 파일 경로   (기본값: output.json)
"""

import os
import time

import scrapy
from dotenv import load_dotenv
from scrapy.crawler import CrawlerProcess

from base_spider import BaseNewsSpider

load_dotenv()


class NaverFinanceNewsCrawler(BaseNewsSpider):
    name = "naver_news"
    source_name = "naver_finance"
    start_urls = ["https://news.naver.com/breakingnews/section/101/259"]

    def parse(self, response):
        if self._time_exceeded():
            print(f"⏰ 시간 제한({self.max_crawl_time}초) 도달, 크롤링 종료")
            return

        links = response.css(
            "ul.sa_list li.sa_item a.sa_text_title::attr(href)"
        ).getall()
        for link in links[: self.max_articles]:
            if not link.startswith("http"):
                link = "https://n.news.naver.com" + link
            yield scrapy.Request(link, callback=self.parse_article)

    def parse_article(self, response):
        if self._time_exceeded():
            print(f"⏰ 시간 제한({self.max_crawl_time}초) 도달, 크롤링 종료")
            return

        if self._count_reached():
            print(f"📊 기사 개수 제한({self.max_articles}개) 도달, 크롤링 종료")
            return

        if self.count == 0:
            print(f"\n🕒 Start: {time.strftime('%X')}")
            print(f"📊 목표: {self.max_articles}개 기사, 시간 제한: {self.max_crawl_time}초")
            if self.since_dt:
                print(f"📅 증분 크롤링: {self.since_dt.isoformat()} 이후 기사만 수집")

        # 필드 추출
        title = response.css(".media_end_head_headline").xpath("string()").get()
        content = response.css(".go_trans._article_content").xpath("string()").get()
        date = response.css(
            ".media_end_head_info_datestamp_time._ARTICLE_DATE_TIME::attr(data-date-time)"
        ).get()

        press = response.css(".media_end_head_top_logo img::attr(alt)").get()
        if not press:
            press = response.css(".media_end_head_top_logo::text").get(default="").strip()
        if not press:
            press = "알 수 없음"

        published_at = self.format_date_iso(date) if date else None

        # 증분 크롤링: 기준 시각 이전 기사 건너뜀
        if self._should_skip_by_date(published_at):
            print(f"⏭️  증분 skip (기준 이전): {published_at} — {response.url}")
            return

        print(f"\n📄 [{self.count + 1}/{self.max_articles}] {response.url}")
        print(f"언론사: {press}")
        print(f"제목: {title.strip() if title else '없음'}")
        print(f"본문 길이: {len(content.strip()) if content else 0}자")
        if published_at:
            print(f"날짜: {published_at}")

        self.count += 1

        if self.count >= self.max_articles:
            elapsed = round(time.time() - self.start_time, 3)
            print(f"\n✅ 크롤링 완료! {self.count}개 기사, 소요 시간: {elapsed}초")

        yield {
            "title": title.strip() if title else "제목 없음",
            "content": content.strip() if content else "",
            "publishedAt": published_at,
            "url": response.url,
            "press": press,
        }


if __name__ == "__main__":
    output_path = os.getenv("OUTPUT_FILE_PATH", "output.json")
    process = CrawlerProcess(
        settings={
            "USER_AGENT": (
                "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                "(KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
            ),
            "LOG_LEVEL": "INFO",
            "FEED_FORMAT": "jsonlines",
            "FEED_URI": output_path,
            "CONCURRENT_REQUESTS": 2,
            "DOWNLOAD_DELAY": 1,
            "DOWNLOAD_TIMEOUT": 10,
            "RETRY_TIMES": 2,
            "ROBOTSTXT_OBEY": False,
        }
    )
    process.crawl(NaverFinanceNewsCrawler)
    process.start()
