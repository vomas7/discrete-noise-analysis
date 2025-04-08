FROM python:3.13

WORKDIR /noise

ENV PYTHONUNBUFFERED 1
ENV PYTHONDONTWRITEBYTECODE 1
ENV VIRTUAL_ENV=/opt/venv

RUN python3 -m venv $VIRTUAL_ENV
RUN apt-get update && apt-get install -y libpq-dev build-essential git
RUN pip install --upgrade pip

RUN	git config --global user.name "vomas7"
RUN	git config --global user.email "superjob214@yandex.ru"
RUN	git clone https://github.com/vomas7/discrete-noise-analysis.git || { echo 'Clone failed'; exit 1; }
RUN	cd discrete-noise-analysis && git checkout main || { echo 'Checkout failed'; exit 1; }
RUN	cd discrete-noise-analysis && git pull origin main || { echo 'Pull failed'; exit 1; }

RUN pip install --no-cache-dir -r discrete-noise-analysis/requirements.txt
COPY core/.env /noise/discrete-noise-analysis/core

WORKDIR /noise/discrete-noise-analysis

CMD ["uvicorn", "app:app", "--reload", "--host", "0.0.0.0", "--port", "80", "--workers", "8"]
