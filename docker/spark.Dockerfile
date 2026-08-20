FROM python:3.11-slim-bookworm

RUN apt-get update && apt-get install -y --no-install-recommends \
    openjdk-17-jre-headless \
    ca-certificates \
    curl \
  && rm -rf /var/lib/apt/lists/* \
  && ln -s /usr/lib/jvm/java-17-openjdk-* /usr/lib/jvm/java-17-openjdk

RUN pip install --no-cache-dir \
    pyspark==3.5.3 \
    delta-spark==3.3.1 \
    pandas \
    pyarrow \
    numpy \
    scikit-learn \
    joblib

# Arch-neutral symlink created above (arm64/amd64)
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk
ENV PATH="$JAVA_HOME/bin:$PATH"
ENV PYTHONUNBUFFERED=1

WORKDIR /opt/project
