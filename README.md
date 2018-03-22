Enmasse playground: Spring Boot AMQP consumer application

Uses the Vert.x AMQP bridge library.

* The application is meant to be deployed on OpenShift.
* To deploy to OpenShift using the Fabric8 Maven plugin:
```
$ mvn fabric8:deploy -Popenshift
```
* The application expects a ConfigMap named `consumer-amqp-sb` containing an `application.properties` file.
* Example `application.properties`:
```
vertx.worker.pool.size=15
vertx.message-consumer.instances=5

amqp.host=messaging.enmasse.svc.cluster.local
amqp.port=5671
amqp.user=user
amqp.password=password
amqp.address=queue-amqp
amqp.ssl.enabled=true
amqp.ssl.trustall=false
amqp.ssl.verifyhost=true
amqp.truststore.path=/app/truststore/enmasse.jks
amqp.truststore.password=password

logging.factor=1
```
* The application expects a secret called `enmasse-truststore` containing a JKS truststore with the Enmasse messaging certificate.
* To create the truststore:
```
keytool -importcert -trustcacerts -file external-certs-messaging.crt -keystore enmasse.jks -storepass password -noprompt
```
