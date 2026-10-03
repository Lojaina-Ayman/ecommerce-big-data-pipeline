ARG CONNECT_BASE_IMAGE
FROM ${CONNECT_BASE_IMAGE}

ARG SPOOLDIR_VERSION

USER root

RUN confluent-hub install --no-prompt jcustenborder/kafka-connect-spooldir:${SPOOLDIR_VERSION}

USER appuser