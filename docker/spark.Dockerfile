FROM apache/spark:3.5.8-java17-python3

USER root
RUN pip install --no-cache-dir pandas numpy scikit-learn joblib

# Optional: make python output unbuffered for logs
ENV PYTHONUNBUFFERED=1

USER spark
WORKDIR /opt/spark/work-dir
