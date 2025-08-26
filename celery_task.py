from celery import Celery
from pymongo import MongoClient
from dotenv import load_dotenv
import json
import subprocess
import os

load_dotenv()

# 1. Celery 설정
app = Celery('tasks', broker=os.getenv("REDIS_URL"))

# 2. MongoDB 연결
mongo_url = os.getenv("MONGO_URL")
client = MongoClient(mongo_url)
db = client['ArticleDatabase']
collection = db['articles']

@app.task
def crawl_and_store_articles():
    print("🚀 크롤링 시작: python naver_spider.py 실행")

    # 3. 기존 output.json 삭제 (덮어쓰기 방지)
    if os.path.exists("output.json"):

        os.remove("output.json")

    # 4. Scrapy 크롤러 실행
    result = subprocess.run(
        ['python', 'naver_spider.py'],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True
    )

    if result.returncode != 0:
        print("❌ 크롤링 실패:")
        print(result.stderr)
        return

    # 5. output.json 확인
    if not os.path.exists("output.json"):
        print("❌ output.json 파일 없음")
        return

    print("📦 output.json 확인 완료, MongoDB 저장 시작")

    # 6. JSON 라인별 로딩 및 저장
    new_count = 0
    with open("output.json", "r", encoding="utf-8") as f:
        for line in f:
            if not line.strip():
                continue
            try:
                article = json.loads(line)
                if not collection.find_one({"url": article.get("url")}):
                    collection.insert_one(article)
                    new_count += 1
            except json.JSONDecodeError as e:
                print(f"❌ JSON 디코딩 실패: {e}")


    print(f"✅ 저장 완료: {new_count}건 추가됨")

