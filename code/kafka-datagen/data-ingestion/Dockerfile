FROM centos:latest

ARG FLUME_VERSION=1.9.0

RUN yum update -y && \
  yum install -y java-1.8.0-openjdk && yum clean all && rm -rf /var/cache/yum && \
  curl -L http://archive.apache.org/dist/flume/${FLUME_VERSION}/apache-flume-${FLUME_VERSION}-bin.tar.gz | tar xz && \
  mv apache-flume-${FLUME_VERSION}-bin apache-flume && \
  cp /apache-flume/conf/flume-conf.properties.template /apache-flume/conf/flume-conf.properties

ENV JAVA_HOME=/usr
ENV AGENT=agent

WORKDIR /apache-flume

# Hadoop dependencies
RUN curl -O https://www-us.apache.org/dist/hadoop/common/hadoop-3.2.0/hadoop-3.2.0.tar.gz
RUN tar -zxvf hadoop-3.2.0.tar.gz

ENV HADOOP_HOME="/apache-flume/hadoop-3.2.0"
RUN cp $HADOOP_HOME/share/hadoop/common/*.jar /apache-flume/lib
RUN cp $HADOOP_HOME/share/hadoop/common/lib/*.jar /apache-flume/lib
RUN cp $HADOOP_HOME/share/hadoop/hdfs/*.jar /apache-flume/lib
RUN cp $HADOOP_HOME/share/hadoop/hdfs/lib/*.jar /apache-flume/lib

ENV FLUME_CLASSPATH="/apache-flume/lib/"

CMD [ "sh","-c", "./bin/flume-ng agent -n ${AGENT} -c conf -f conf/flume-conf.properties -DpropertiesImplementation=org.apache.flume.node.EnvVarResolverProperties" ]
