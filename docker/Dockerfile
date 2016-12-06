FROM java:alpine

# Defaulting common environment variables
ENV KAFKA_SESSION_TIMEOUT_MILLIS 30000
ENV KAFKA_PRODUCER_CLIENT_ID producer
ENV KAFKA_RPC_CLIENT_ID rpc
ENV KAFKA_RPC_RESPONSE_HOST localhost
ENV KAFKA_RPC_RESPONSE_PORT 11111
ENV KAFKA_RPC_RESPONSE_TIMEOUT_MILLIS 10000

# Copy the JAR executable
ADD *.jar app.jar

# Execute the Spring Boot JAR command
CMD java -Djava.security.egd=file:/dev/./urandom -Dkafka.zookeeper.connect=$KAFKA_ZOOKEEPER_CONNECT -Dkafka.session.timeout.ms=$KAFKA_SESSION_TIMEOUT_MILLIS -Dkafka.producer.client.id=$KAFKA_PRODUCER_CLIENT_ID -Dkafka.rpc.client.id=$KAFKA_RPC_CLIENT_ID -Dkafka.rpc.response.host=$KAFKA_RPC_RESPONSE_HOST -Dkafka.rpc.response.port=$KAFKA_RPC_RESPONSE_PORT -Dkafka.rpc.response.timeout.ms=$KAFKA_RPC_RESPONSE_TIMEOUT_MILLIS -jar app.jar