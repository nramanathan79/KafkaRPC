FROM tomcat:8.5

# Defaulting common environment variables
ENV SESSION_TIMEOUT_MILLIS 30000
ENV CONSUMER_CLIENT_ID consumer
ENV PRODUCER_CLIENT_ID producer
ENV RPC_CLIENT_ID rpc
ENV RPC_RESPONSE_PORT 11111
ENV RPC_TIMEOUT_MILLIS 100

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
