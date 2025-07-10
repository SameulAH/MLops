# Dockerfile for Astronomer Runtime with custom requirements
FROM quay.io/astronomer/astro-runtime:13.0.0
# # Upgrade pip and setuptools
RUN pip install --upgrade pip && pip install --upgrade setuptools
COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

