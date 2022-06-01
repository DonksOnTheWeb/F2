FROM python:3.9.1

#RUN apk --no-cache --update-cache add gcc g++ gfortran build-base wget freetype-dev libpng-dev openblas-dev python-dev python3-dev
#RUN ln -s /usr/include/locale.h /usr/include/xlocale.h

RUN pip3 install --upgrade --no-cache-dir pip
RUN pip3 install --upgrade --no-cache-dir setuptools
RUN pip3 install --upgrade --no-cache-dir numpy
RUN pip3 install --upgrade --no-cache-dir pandas
RUN pip3 install --upgrade --no-cache-dir convertdate
RUN pip3 install --upgrade --no-cache-dir lunarcalendar
RUN pip3 install --upgrade --no-cache-dir holidays
RUN pip3 install --upgrade --no-cache-dir Cython
RUN pip3 install --upgrade --no-cache-dir pystan
RUN pip3 install --upgrade --no-cache-dir prophet
RUN pip3 install --upgrade --no-cache-dir Flask
RUN pip3 install --upgrade --no-cache-dir gunicorn

COPY gunicorn_config.py /deploy/gunicorn_config.py
COPY ./app /deploy/app
WORKDIR /deploy/app

EXPOSE 5000

CMD gunicorn app:app --config /deploy/gunicorn_config.py
