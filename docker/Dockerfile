# vim: ft=dockerfile
FROM debian:buster-slim

# Do not install recommends:
RUN echo 'APT::Install-Recommends "0";' > \
      /etc/apt/apt.conf.d/no_recommends

# Install a few things
RUN apt-get --yes update && \
    DEBIAN_FRONTEND=noninteractive \
      apt-get --yes upgrade && \
    DEBIAN_FRONTEND=noninteractive \
      apt-get --yes install \
      binutils \
      bzip2 \
      ca-certificates \
      curl \
      git \
      iputils-ping \
      less \
      librdkafka-dev \
      liblmdb-dev \
      m4 \
      make \
      ocaml-nox \
      patch \
      pkg-config \
      unzip \
      zlib1g-dev

COPY install-opam.sh /tmp
RUN echo | /tmp/install-opam.sh

RUN /usr/local/bin/opam init --no-setup --disable-sandboxing
RUN /usr/local/bin/opam update --yes
# Let's use the same version as locally:
RUN opam switch install --yes datasino 4.09.0+flambda

RUN opam update
RUN opam repo add --set-default ocalme \
      git://github.com/rixed/ocalme-opam-repository.git
RUN opam repo priority ocalme 1

# Install datasino
RUN opam install --yes datasino

# Clean up
RUN DEBIAN_FRONTEND=noninteractive \
      apt-get --yes purge \
        patch \
        pkg-config && \
    DEBIAN_FRONTEND=noninteractive \
      apt-get --yes autoremove

# Environment
ENV OPAM_SWITCH_PREFIX=/root/.opam/datasino
ENV CAML_LD_LIBRARY_PATH=/root/.opam/datasino/lib/stublibs:/root/.opam/datasino/lib/ocaml/stublibs:/root/.opam/datasino/lib/ocaml
ENV OCAML_TOPLEVEL_PATH=/root/.opam/datasino/lib/toplevel
ENV MANPATH=:/root/.opam/datasino/man
ENV PATH=/root/.opam/datasino/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin
ENV EXTRA_SEARCH_PATHS=/root/.opam/datasino/lib/datasino

# Command to run:
WORKDIR /tmp
ENTRYPOINT ["/root/.opam/datasino/bin/datasino"]
CMD []

LABEL maintainer="rixed-github@happyleptic.org"
