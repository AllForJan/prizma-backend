FROM python:3.6.3

RUN apt-get update && \
	DEBIAN_FRONTEND=noninteractive apt-get install -y \
		unzip \
		less \
		postgresql-client \
		apt-get \
		install \
		binutils \
		libproj-dev \
		gdal-bin \
		libgeos

RUN mkdir -p /app/app /app/packages
WORKDIR /app/app

COPY requirements.txt dev-requirements.txt /app/app/
RUN pip install --src /app/packages --no-cache-dir -r dev-requirements.txt

CMD ["bin/image-entrypoint.webapp.sh"]
