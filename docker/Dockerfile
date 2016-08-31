FROM tomcat:8.5

# Defaulting common environment variables
ENV KAFKA_SESSION_TIMEOUT_MILLIS 30000
ENV KAFKA_CONSUMER_CLIENT_ID consumer
ENV KAFKA_PRODUCER_CLIENT_ID producer
ENV KAFKA_RPC_CLIENT_ID rpc
ENV KAFKA_RPC_RESPONSE_PORT 11111
ENV KAFKA_RPC_RESPONSE_TIMEOUT_MILLIS 100

# Set runtime environment variables
ENV CATALINA_HOME /usr/local/tomcat
COPY setenv.sh ${CATALINA_HOME}/bin

# Set work directory
WORKDIR ${CATALINA_HOME}

# Copy configurations
COPY tomcat-users.xml ${CATALINA_HOME}/conf
RUN mkdir -p ${CATALINA_HOME}/conf/Catalina/localhost
COPY kafkarpc.xml ${CATALINA_HOME}/conf/Catalina/localhost

# Copy deployment war file
RUN mkdir -p ${CATALINA_HOME}/apps
COPY kafkarpc*.war ${CATALINA_HOME}/apps
