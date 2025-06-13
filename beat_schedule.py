from celery.schedules import crontab
from celery import Celery
from celery_task import crawl_and_store_articles

app = Celery("scrapy-beat")
app.conf.broker_url = "redis://redis:6379/0"
app.config_from_object("celery")

# 주기 설정: 매 1분마다 실행 (테스트용)
app.conf.beat_schedule = {
    "crawl-and-save-every-minute": {
        "task": "celery_task.crawl_and_store_articles",
        "schedule": crontab(minute="*/30"),  # 매 1분마다
    },
}
