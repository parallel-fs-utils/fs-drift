FROM docker.io/fedora:29
MAINTAINER Ben England <bengland@redhat.com>
RUN dnf install -y python2 git python2-numpy python2-pyyaml
RUN git clone https://github.com/parallel-fs-utils/fs-drift
RUN ln -sv /fs-drift/fs-drift-remote.py /usr/local/bin
COPY launch.sh /
CMD /launch.sh
