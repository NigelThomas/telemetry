# app/Dockerfile

FROM python:3.11-slim

EXPOSE 8501


RUN apt-get update && apt-get install -y \
    build-essential \
    software-properties-common && \
    #git \
    useradd -ms /bin/bash streamlit && \
    usermod -aG sudo streamlit &&\
    rm -rf /var/lib/apt/lists/*

USER streamlit
RUN  mkdir /home/streamlit/app
WORKDIR /home/streamlit/app

RUN export PATH=$PATH:/home/streamlit/.local/bin &&\
    pip3 install graphviz &&\
    pip3 install streamlit

ENTRYPOINT /home/streamlit/app/entrypoint.sh
