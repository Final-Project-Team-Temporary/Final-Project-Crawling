from celery import Celery
from pymongo import MongoClient
from dotenv import load_dotenv
import json
import subprocess
import os
import requests
from datetime import datetime

load_dotenv()

# 1. Celery 설정
app = Celery('tasks', broker=os.getenv("REDIS_URL"))

# 2. MongoDB 연결
mongo_url = os.getenv("MONGO_URL")
client = MongoClient(mongo_url)
db = client['ArticleDatabase']
collection = db['articles']

def send_summarization_request(article_ids):
    """
    BE 서버의 요약 API로 새로 추가된 기사 ID들을 전송
    """
    if not article_ids:
        print("📝 요약 요청할 새 기사가 없습니다.")
        return
    
    be_server_url = os.getenv("BE_SERVER_URL")
    if not be_server_url:
        print("⚠️ BE_SERVER_URL 환경변수가 설정되지 않았습니다.")
        return
    
    # ArticleSummarizationRequest DTO 형식에 맞는 요청 데이터 생성
    request_data = {
        "articleIds": article_ids,
        "timestamp": datetime.now().isoformat()
    }
    
    try:
        print(f"📤 BE 서버로 요약 요청 전송: {len(article_ids)}개 기사")
        print(f"🔗 요청 URL: {be_server_url}/api/articles/summarization/request")
        
        response = requests.post(
            f"{be_server_url}/api/articles/summarization/request",
            json=request_data,
            headers={"Content-Type": "application/json"},
            timeout=30
        )
        
        if response.status_code == 200:
            print(f"✅ 요약 요청 성공: {response.status_code}")
            print(f"📋 응답: {response.text}")
        else:
            print(f"❌ 요약 요청 실패: {response.status_code}")
            print(f"📋 응답: {response.text}")
            
    except requests.exceptions.RequestException as e:
        print(f"❌ 요약 요청 중 오류 발생: {str(e)}")
    except Exception as e:
        print(f"❌ 예상치 못한 오류: {str(e)}")

@app.task(bind=True, time_limit=int(os.getenv("CELERY_TASK_TIMEOUT", "600")))
def crawl_and_store_articles(self):
    timeout_limit = int(os.getenv("CELERY_TASK_TIMEOUT", "600"))
    print(f"🚀 크롤링 시작: python naver_spider.py 실행 (타임아웃: {timeout_limit}초)")

    output_path = os.getenv("OUTPUT_FILE_PATH", "output.json")
    
    # 3. 기존 output 파일 삭제 (덮어쓰기 방지)
    if os.path.exists(output_path):
        os.remove(output_path)

    # 4. Scrapy 크롤러 실행
    print("📊 크롤링 프로세스 시작...")
    result = subprocess.run(
        ['python', 'naver_spider.py'],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True
    )

    # stdout 내용을 CloudWatch로 출력
    if result.stdout:
        print("📋 크롤러 출력:")
        print(result.stdout)
    
    # stderr 내용도 출력
    if result.stderr:
        print("⚠️ 크롤러 에러/경고:")
        print(result.stderr)

    if result.returncode != 0:
        print(f"❌ 크롤링 실패: 종료 코드 {result.returncode}")
        return
    else:
        print("✅ 크롤링 프로세스 완료")

    # 5. output 파일 확인
    if not os.path.exists(output_path):
        print(f"❌ {output_path} 파일 없음")
        return

    print(f"📦 {output_path} 확인 완료, MongoDB 저장 시작")

    # 6. JSON 라인별 로딩 및 저장
    new_count = 0
    new_article_ids = []  # 새로 추가된 기사 ID들을 저장할 리스트
    
    with open(output_path, "r", encoding="utf-8") as f:
        for line in f:
            if not line.strip():
                continue
            try:
                article = json.loads(line)
                if not collection.find_one({"url": article.get("url")}):
                    # 새 기사를 MongoDB에 저장하고 _id 반환
                    result = collection.insert_one(article)
                    new_article_ids.append(str(result.inserted_id))  # ObjectId를 문자열로 변환
                    new_count += 1
            except json.JSONDecodeError as e:
                print(f"❌ JSON 디코딩 실패: {e}")

    print(f"✅ 저장 완료: {new_count}건 추가됨")
    
    # 7. 새로 추가된 기사가 있으면 BE 서버로 요약 요청 전송
    if new_article_ids:
        print(f"📝 새로 추가된 기사 ID들: {new_article_ids}")
        send_summarization_request(new_article_ids)
    else:
        print("📝 새로 추가된 기사가 없어 요약 요청을 건너뜁니다.")

