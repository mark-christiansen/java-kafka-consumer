FROM openjdk:11-jdk-slim
MAINTAINER Platform Team (platform@jnj.com)

ARG JAR_FILE
ARG JMX_EXPORTER_JAR
ARG JMX_EXPORTER_CONFIG
ENV AGENT_JAR $JMX_EXPORTER_JAR
ENV AGENT_CONFIG $JMX_EXPORTER_CONFIG

RUN /bin/bash -c "echo $JAR_FILE; echo $JMX_EXPORTER_JAR; echo $JMX_EXPORTER_CONFIG"
COPY target/${JAR_FILE} /app/app.jar
COPY target/${JMX_EXPORTER_JAR} /app/${JMX_EXPORTER_JAR}
COPY target/${JMX_EXPORTER_CONFIG} /app/${JMX_EXPORTER_CONFIG}
# unpacked archive is slightly faster on startup than running from an unexploded archive
RUN /bin/bash -c 'cd /app; jar xf app.jar'

ENTRYPOINT exec java $JAVA_OPTS -javaagent:/app/$AGENT_JAR=9100:/app/$AGENT_CONFIG -cp /app/BOOT-INF/classes:/app/BOOT-INF/lib/* com.jnj.kafka.consumer.Application