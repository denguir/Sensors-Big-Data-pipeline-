FROM mengluo668/flume

RUN curl -O https://www-us.apache.org/dist/hadoop/common/hadoop-3.2.0/hadoop-3.2.0.tar.gz
RUN tar -zxvf hadoop-3.2.0.tar.gz

ENV HADOOP_HOME="/apache-flume/hadoop-3.2.0"
RUN cp $HADOOP_HOME/share/hadoop/common/*.jar /apache-flume/lib
RUN cp $HADOOP_HOME/share/hadoop/common/lib/*.jar /apache-flume/lib
RUN cp $HADOOP_HOME/share/hadoop/hdfs/*.jar /apache-flume/lib
RUN cp $HADOOP_HOME/share/hadoop/hdfs/lib/*.jar /apache-flume/lib

ENV FLUME_CLASSPATH="/apache-flume/lib/"

