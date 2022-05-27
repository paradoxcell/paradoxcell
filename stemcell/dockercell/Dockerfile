# IMAGE
FROM ubuntu:latest

# LABEL
LABEL name="Juntae Song"
LABEL email="sjt0332@gmail.com"
LABEL description="Dockerfile for melody"

# VALUE
ENV LANG=C.UTF-8 LC_ALL=C.UTF-8
ENV PATH /downloads/miniconda3/bin:$PATH
ARG DEBIAN_FRONTEND=noninteractive

# APT-GET
RUN apt-get update --fix-missing && \
    apt-get upgrade -y && \
    apt-get install -y sudo vim git wget unzip curl htop locales build-essential software-properties-common && \
    apt-get clean

# SHELL
SHELL ["/bin/bash", "-c"]

# MINICONDA
RUN mkdir -p /downloads/miniconda3 && \
    wget -q https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-x86_64.sh -O /downloads/miniconda3/miniconda.sh && \
    /bin/bash /downloads/miniconda3/miniconda.sh -b -u -p /downloads/miniconda3 && \
    rm -rf /downloads/miniconda3/miniconda.sh && \
    ln -s /downloads/miniconda3/etc/profile.d/conda.sh /etc/profile.d/conda.sh && \
    echo ". /downloads/miniconda3/etc/profile.d/conda.sh" >> ~/.bashrc && \
    echo "conda activate base" >> ~/.bashrc
RUN conda update -n base -c defaults conda --force && \
    conda update -y --all
RUN conda upgrade -n base conda
RUN conda config --add channels defaults && \
    conda config --add channels r && \
    conda config --add channels bioconda && \
    conda config --add channels conda-forge

# GROUP
RUN groupadd conda && \
    chgrp -R conda /downloads/miniconda3 &&\
    chmod 770 -R /downloads/miniconda3

# USER
RUN useradd -m -g conda -s /bin/bash pgl && \
    echo ". /downloads/miniconda3/etc/profile.d/conda.sh" >> /home/pgl/.bashrc && \
    echo "conda activate base" >> /home/pgl/.bashrc
RUN usermod -G sudo -a pgl
USER pgl
RUN conda update -n base -c defaults conda --force && \
    conda update -y --all
RUN conda upgrade -n base conda
RUN conda config --add channels defaults && \
    conda config --add channels r && \
    conda config --add channels bioconda && \
    conda config --add channels conda-forge

# ROOT
USER root
RUN export LC_ALL=C.UTF-8

# WORKDIR
RUN mkdir /data
WORKDIR /data

# PORT
EXPOSE 22
EXPOSE 8888

# CMD
CMD ["bash"]
