FROM python:2

ARG USER_KEYTAB

ENV DEBIAN_FRONTEND=noninteractive
RUN apt-get update && apt-get install -y krb5-kdc

RUN pip install hdfs requests_kerberos

RUN mkdir source
ADD read-hdfs.py /source
ADD ${USER_KEYTAB} /source
ADD launch.sh /source