import scrapy
from scrapy.crawler import CrawlerProcess
import time
import os
from dotenv import load_dotenv

load_dotenv()


class NaverNewsSpider(scrapy.Spider):
    name = "naver_news"
    start_urls = ['https://news.naver.com/breakingnews/section/101/259']

    custom_settings = {
        "LOG_LEVEL": "ERROR"
    }

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.start_time = time.time()
        self.count = 0

    def parse(self, response):
        links = response.css("ul.sa_list li.sa_item a.sa_text_title::attr(href)").getall()
        for link in links[:10]:  # Lambda 환경에서 빠른 처리를 위해 10개로 제한
            if not link.startswith("http"):
                link = "https://n.news.naver.com" + link
            yield scrapy.Request(link, callback=self.parse_article)

    def parse_article(self, response):
        if self.count == 0:
            print(f"\n🕒 Start: {time.strftime('%X')}")

        title = response.css(".media_end_head_headline").xpath("string()").get()
        content = response.css(".go_trans._article_content").xpath("string()").get()
        date = response.css(".media_end_head_info_datestamp_time._ARTICLE_DATE_TIME::attr(data-date-time)").get()

        print(f"\n📄 {response.url}")
        print(f"제목: {title.strip() if title else '없음'}")
        print(f"본문 길이: {len(content.strip()) if content else 0}자")

        self.count += 1
        if self.count == 5:
            print(f"\n✅ 완료. 총 소요 시간: {round(time.time() - self.start_time, 3)}초")

        # ✅ 여기서 결과를 반환해야 Scrapy가 저장함!
        yield {
            "title": title.strip() if title else "제목 없음",
            "content": content.strip() if content else "",
            "date": date if date else "날짜 없음",
            "url": response.url,
            "summary_status": "pending"
        }


if __name__ == "__main__":
    output_path = os.getenv("OUTPUT_FILE_PATH", "output.json")
    process = CrawlerProcess(settings={
        "USER_AGENT": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
        "LOG_LEVEL": "INFO",
        "FEED_FORMAT": "jsonlines",
        "FEED_URI": output_path,
        "CONCURRENT_REQUESTS": 2,  # 동시 요청 수 제한
        "DOWNLOAD_DELAY": 1,       # 요청 간 1초 대기
        "DOWNLOAD_TIMEOUT": 10,    # 10초 타임아웃
        "RETRY_TIMES": 2,          # 재시도 2회
        "ROBOTSTXT_OBEY": False    # robots.txt 무시 (속도 향상)
    })
    process.crawl(NaverNewsSpider)
    process.start()

