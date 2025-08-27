import json
import os
from celery_task import crawl_and_store_articles


def handler(event, context):
    """
    AWS Lambda handler for news crawling
    """
    try:
        # EventBridge에서 호출되는 경우 또는 직접 호출
        source = event.get('source', 'manual')
        
        # Celery 태스크 실행 (Lambda 환경에서는 동기 실행)
        result = crawl_and_store_articles.apply()
        
        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'News crawling completed successfully',
                'source': source,
                'task_id': str(result.id) if hasattr(result, 'id') else 'sync',
                'status': 'success'
            })
        }
    
    except Exception as e:
        print(f"Lambda execution error: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps({
                'message': 'News crawling failed',
                'error': str(e),
                'status': 'error'
            })
        }