CATALINA_OPTS="-Dzookeeper.connect=$ZOOKEEPER_CONNECT -Dsession.timeout.millis=SESSION_TIMEOUT_MILLIS -Dconsumer.client.id=$CONSUMER_CLIENT_ID -Dproducer.client.id=$PRODUCER_CLIENT_ID -Drpc.client.id=$RPC_CLIENT_ID -Drpc.response.port=$RPC_RESPONSE_PORT -Drpc.timeout.millis=$RPC_TIMEOUT_MILLIS"
