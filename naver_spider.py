import scrapy
from scrapy.crawler import CrawlerProcess
import time
import os
from dotenv import load_dotenv
from datetime import datetime

load_dotenv()


def format_date_for_spring(date_str):
    """
    날짜 문자열을 Spring LocalDateTime이 파싱 가능한 ISO 8601 형식으로 변환
    "2025-10-23 20:37:26" -> "2025-10-23T20:37:26"
    """
    if not date_str or date_str == "날짜 없음":
        return None
    
    try:
        # 여러 형식을 지원하도록 파싱 시도
        # "2025-10-23 20:37:26" 형식 처리
        if " " in date_str and "T" not in date_str:
            # 공백을 T로 변환
            formatted = date_str.replace(" ", "T")
            # 파싱 가능한지 검증
            datetime.fromisoformat(formatted)
            return formatted
        # 이미 ISO 형식인 경우 그대로 반환
        elif "T" in date_str:
            return date_str
        # 다른 형식인 경우 datetime으로 파싱 후 ISO 형식으로 변환
        else:
            # 다양한 형식 시도
            formats = [
                "%Y-%m-%d %H:%M:%S",
                "%Y-%m-%dT%H:%M:%S",
                "%Y-%m-%d %H:%M:%S.%f",
                "%Y-%m-%dT%H:%M:%S.%f"
            ]
            for fmt in formats:
                try:
                    dt = datetime.strptime(date_str, fmt)
                    return dt.isoformat().split('.')[0]  # 밀리초 제거하고 반환
                except ValueError:
                    continue
            # 파싱 실패 시 원본 반환
            return date_str
    except Exception as e:
        print(f"⚠️ 날짜 변환 실패: {date_str}, 오류: {e}")
        return date_str


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

        # 1. 기존 데이터 추출
        title = response.css(".media_end_head_headline").xpath("string()").get()
        content = response.css(".go_trans._article_content").xpath("string()").get()
        date = response.css(".media_end_head_info_datestamp_time._ARTICLE_DATE_TIME::attr(data-date-time)").get()
        
        # 2. 언론사(press) 정보 추출 추가
        # 기본적으로 로고 이미지의 alt 속성에서 언론사명을 가져옵니다.
        press = response.css(".media_end_head_top_logo img::attr(alt)").get()
        # 로고 이미지가 없는 경우 텍스트에서 가져오는 예외 처리
        if not press:
            press = response.css(".media_end_head_top_logo::text").get(default="").strip()
        # 그래도 없으면 기본값 설정
        if not press:
            press = "알 수 없음"

        # Spring LocalDateTime 파싱 가능한 ISO 8601 형식으로 변환
        formatted_date = format_date_for_spring(date) if date else None

        print(f"\n📄 [{self.count + 1}/{self.max_articles}] {response.url}")
        print(f"언론사: {press}")
        print(f"제목: {title.strip() if title else '없음'}")
        print(f"본문 길이: {len(content.strip()) if content else 0}자")
        if formatted_date:
            print(f"날짜: {formatted_date}")

        self.count += 1
        
        # 완료 조건 확인
        if self.count >= self.max_articles:
            elapsed_time = round(time.time() - self.start_time, 3)
            print(f"\n✅ 크롤링 완료! {self.count}개 기사, 소요 시간: {elapsed_time}초")

        # 3. yield 에 press 필드 추가
        yield {
            "title": title.strip() if title else "제목 없음",
            "content": content.strip() if content else "",
            "publishedAt": formatted_date if formatted_date else None,
            "url": response.url,
            "press": press,  # <--- 언론사 정보 추가
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

