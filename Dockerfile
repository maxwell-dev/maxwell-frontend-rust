# syntax=docker/dockerfile:1
# escape=\

FROM ubuntu:22.04
LABEL maintainer="maxwell-dev <https://github.com/maxwell-dev>"
SHELL ["/bin/bash", "-c"]

ARG uid

RUN adduser --disabled-password --no-create-home --gecos "" --uid ${uid:-10000} maxwell

WORKDIR /maxwell-frontend
RUN mkdir -p log
COPY certificates/* certificates/
COPY config/config.template.toml config/config.toml
COPY config/log4rs.template.yaml config/log4rs.yaml
COPY target/release/maxwell-frontend .
RUN chown -R maxwell:maxwell .

USER maxwell
CMD ["./maxwell-frontend"]
