FROM docker.io/almalinux:9

EXPOSE 5000
ARG VERSION=develop

RUN yum clean all \
 && yum -y install python3-pip python3-pip-wheel python3-devel git \
 && yum clean all

RUN git clone --depth 1 -b $VERSION https://github.com/DUNE-DAQ/connectivityserver.git
RUN python3 -m pip install /connectivityserver

COPY --chmod=755 entrypoint.sh /
ENTRYPOINT ["/entrypoint.sh"]
