FROM docker.io/almalinux:10

EXPOSE 5000
ARG VERSION=develop

RUN yum clean all \
 && yum -y install python3-pip python3-pip-wheel python3-devel git \
 && yum clean all

ENV \
    APP_ROOT=/opt/app        \
    VENV_PATH=/opt/venv      \
    HOME=/opt/app            \
    PYTHONUNBUFFERED=1       \
    PIP_NO_CACHE_DIR=1       \
    PATH="/opt/venv/bin:$PATH"

RUN mkdir -p ${APP_ROOT} ${VENV_PATH}

WORKDIR ${APP_ROOT}
RUN python3 -m venv ${VENV_PATH} \
 && git clone --depth 1 -b ${VERSION} https://github.com/DUNE-DAQ/connectivityserver.git \
 && ${VENV_PATH}/bin/pip install --no-cache-dir ./connectivityserver \
 && rm -rf /root/.cache ${HOME}/.cache ${VENV_PATH}/pip-selfcheck.json

COPY --chmod=755 entrypoint.sh /

ENTRYPOINT ["/entrypoint.sh"]
