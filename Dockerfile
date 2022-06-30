FROM python:3.9.1

RUN pip3 install --upgrade --no-cache-dir pip
RUN pip3 install --upgrade --no-cache-dir setuptools
RUN pip3 install --upgrade --no-cache-dir numpy
RUN pip3 install --upgrade --no-cache-dir pandas
RUN pip3 install --upgrade --no-cache-dir convertdate
RUN pip3 install --upgrade --no-cache-dir lunarcalendar
RUN pip3 install --upgrade --no-cache-dir holidays
RUN pip3 install --upgrade --no-cache-dir Cython
RUN pip3 install --upgrade --no-cache-dir tqdm
RUN pip3 install --upgrade --no-cache-dir pystan
RUN pip3 install --upgrade --no-cache-dir prophet
RUN pip3 install --upgrade --no-cache-dir Flask
RUN pip3 install --upgrade --no-cache-dir gunicorn
RUN pip3 install --upgrade --no-cache-dir mariadb
RUN pip3 install --upgrade --no-cache-dir google-auth-httplib2
RUN pip3 install --upgrade --no-cache-dir google-auth-oauthlib
RUN pip3 install --upgrade --no-cache-dir google-api-python-client
RUN pip3 install --upgrade --no-cache-dir pytz


ENV PYTHONUNBUFFERED=1
ENV TZ="Europe/Luxembourg"

COPY gunicorn_config.py /deploy/gunicorn_config.py
COPY ./app /deploy/app
COPY ./logs /deploy/logs

WORKDIR /deploy/app

EXPOSE 5000

CMD gunicorn app:app --config /deploy/gunicorn_config.py --timeout 600