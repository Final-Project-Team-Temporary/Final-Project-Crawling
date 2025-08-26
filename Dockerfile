FROM public.ecr.aws/lambda/python:3.11

COPY requirements.txt ${LAMBDA_TASK_ROOT}
RUN pip install --no-cache-dir -r requirements.txt

COPY . ${LAMBDA_TASK_ROOT}

CMD ["lambda_handler.handler"]

# aws lambda는 docker manifest 형식이 구버전만 지원
# 따라서
# docker buildx build \
#  --platform linux/arm64 \
#  -t 503561412385.dkr.ecr.ap-northeast-2.amazonaws.com/econoeasy/econoeasy-crawler:latest \
#  --push \
#  --provenance=false \
#  --sbom=false \
#  .

