FROM python:3.6.3

RUN apt-get update && \
	DEBIAN_FRONTEND=noninteractive apt-get install -y \
		unzip \
		less \
		postgresql-client \
		binutils \
		libproj-dev \
		gdal-bin \
		libgeos-c1 \
		libgeos-3.4.2

RUN mkdir -p /app/app /app/packages
WORKDIR /app/app

COPY src/requirements.txt src/requirements_dev.txt ./
RUN pip install --src /app/packages -r requirements_dev.txt
EXPOSE 5000
COPY src .

CMD ["./start_app.sh"]
