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
        "LOG_LEVEL": "INFO"
    }

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.start_time = time.time()
        self.count = 0
        self.max_articles = int(os.getenv("MAX_ARTICLES", "10"))
        self.max_crawl_time = int(os.getenv("MAX_CRAWL_TIME", "300"))  # 5분

    def parse(self, response):
        # 시간 제한 확인
        if time.time() - self.start_time > self.max_crawl_time:
            print(f"⏰ 시간 제한({self.max_crawl_time}초) 도달, 크롤링 종료")
            return
            
        links = response.css("ul.sa_list li.sa_item a.sa_text_title::attr(href)").getall()
        for link in links[:self.max_articles]:
            if not link.startswith("http"):
                link = "https://n.news.naver.com" + link
            yield scrapy.Request(link, callback=self.parse_article)

    def parse_article(self, response):
        # 시간 제한 확인
        if time.time() - self.start_time > self.max_crawl_time:
            print(f"⏰ 시간 제한({self.max_crawl_time}초) 도달, 크롤링 종료")
            return
            
        # 기사 개수 제한 확인
        if self.count >= self.max_articles:
            print(f"📊 기사 개수 제한({self.max_articles}개) 도달, 크롤링 종료")
            return
        
        if self.count == 0:
            print(f"\n🕒 Start: {time.strftime('%X')}")
            print(f"📊 목표: {self.max_articles}개 기사, 시간 제한: {self.max_crawl_time}초")

        title = response.css(".media_end_head_headline").xpath("string()").get()
        content = response.css(".go_trans._article_content").xpath("string()").get()
        date = response.css(".media_end_head_info_datestamp_time._ARTICLE_DATE_TIME::attr(data-date-time)").get()

        print(f"\n📄 [{self.count + 1}/{self.max_articles}] {response.url}")
        print(f"제목: {title.strip() if title else '없음'}")
        print(f"본문 길이: {len(content.strip()) if content else 0}자")

        self.count += 1
        
        # 완료 조건 확인
        if self.count >= self.max_articles:
            elapsed_time = round(time.time() - self.start_time, 3)
            print(f"\n✅ 크롤링 완료! {self.count}개 기사, 소요 시간: {elapsed_time}초")

        # ✅ 여기서 결과를 반환해야 Scrapy가 저장함!
        yield {
            "title": title.strip() if title else "제목 없음",
            "content": content.strip() if content else "",
            "publishedAt": date if date else "날짜 없음",
            "url": response.url,
            "summary_status": "BEFORE_ENQUEUED"
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

