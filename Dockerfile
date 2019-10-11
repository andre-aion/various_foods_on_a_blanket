FROM continuumio/miniconda3:latest

RUN  apt-get update \
    && apt-get install -y wget \
    && rm -rf /var/lib/apt/lists/*

# Add user ubuntu with no password, add to sudo group
RUN adduser --disabled-password --gecos '' ubuntu
RUN echo '%sudo ALL=(ALL) NOPASSWD:ALL' >> /etc/sudoers
USER ubuntu

RUN chmod a+rwx /home/ubuntu/

WORKDIR /home/ubuntu/

# set environment
COPY ./environment.yml .


#COPY ./boot.sh ./
#RUN chmod +x boot.sh
#RUN conda env create -f ./environment.yml
USER root
RUN conda update conda \
    && conda config --add channels conda-forge \
    && conda env create -n amdattds -f environment.yml \
    && rm -rf /opt/conda/pkgs/*

RUN echo "conda activate amdattds" > ~/.bashrc
ENV PATH /opt/conda/envs/amdattds/bin:$PATH


USER ubuntu
# set up the app
RUN mkdir analytics
WORKDIR /home/ubuntu/analytics/
COPY . .
ENV APP /home/ubuntu/analytics

# load up the tables
USER root
CMD ["python","main.py"]
