FROM apache/hadoop-runner
ARG HADOOP_URL=https://dlcdn.apache.org/hadoop/common/hadoop-3.3.1/hadoop-3.3.1.tar.gz
WORKDIR /opt
RUN sudo rm -rf /opt/hadoop && curl -LSs -o hadoop.tar.gz $HADOOP_URL && tar zxf hadoop.tar.gz && rm hadoop.tar.gz && mv hadoop* hadoop && rm -rf /opt/hadoop/share/doc
WORKDIR /opt/hadoop
ADD log4j.properties /opt/hadoop/etc/hadoop/log4j.properties
RUN sudo chown -R hadoop:users /opt/hadoop/etc/hadoop/*
ENV HADOOP_CONF_DIR /opt/hadoop/etc/hadoop

WORKDIR /opt
RUN sudo yum install -y gcc openssl-devel bzip2-devel libffi-devel zlib-devel make
RUN sudo wget https://www.python.org/ftp/python/3.9.10/Python-3.9.10.tgz
RUN sudo tar xzf Python-3.9.10.tgz
WORKDIR /opt/Python-3.9.10
RUN sudo ./configure --enable-optimizations
RUN sudo make altinstall
RUN sudo /usr/local/bin/pip3.9 install numpy pandas scipy

WORKDIR /opt/hadoop
