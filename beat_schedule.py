from celery.schedules import crontab
from celery import Celery
from celery_task import crawl_and_store_articles
import os
from dotenv import load_dotenv

load_dotenv()

app = Celery("scrapy-beat")
# 환경변수 우선 사용, 없으면 도커 컴포즈 기본값으로 폴백
app.conf.broker_url = os.getenv("REDIS_URL", "redis://redis:6379/0")
app.config_from_object("celery")

# 주기 설정: 매 1분마다 실행 (테스트용)
app.conf.beat_schedule = {
    "crawl-and-save-every-minute": {
        "task": "celery_task.crawl_and_store_articles",
        "schedule": crontab(minute="*/60"),  # 매 1분마다
    },
}
