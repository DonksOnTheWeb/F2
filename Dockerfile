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
RUN pip3 install --upgrade --no-cache-dir apscheduler

ENV PYTHONUNBUFFERED=1
ENV TZ="Europe/Luxembourg"

WORKDIR /deploy/app

COPY ./app /deploy/app
CMD ["python", "/deploy/app/app.py"]
