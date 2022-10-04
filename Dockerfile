FROM continuumio/miniconda3
WORKDIR /usr/src/app

RUN conda install -c "conda-forge/label/cf202003" pystan


RUN pip3 install --upgrade --no-cache-dir numpy
RUN pip3 install --upgrade --no-cache-dir pandas
#RUN pip3 install --upgrade --no-cache-dir Cython
RUN pip3 install --upgrade --no-cache-dir holidays==0.13
RUN pip3 install --upgrade --no-cache-dir pystan==2.19.1.1
RUN pip3 install --upgrade --no-cache-dir prophet
#RUN pip3 install --upgrade --no-cache-dir Flask
#RUN pip3 install --upgrade --no-cache-dir gunicorn
#RUN pip3 install --upgrade --no-cache-dir mariadb
RUN pip3 install --upgrade --no-cache-dir google-auth-httplib2
RUN pip3 install --upgrade --no-cache-dir google-auth-oauthlib
RUN pip3 install --upgrade --no-cache-dir google-api-python-client
RUN pip3 install --upgrade --no-cache-dir apscheduler

ENV PYTHONUNBUFFERED=1
ENV TZ="Europe/Luxembourg"

WORKDIR /deploy/app

COPY ./app /deploy/app
CMD ["python", "/deploy/app/app.py"]