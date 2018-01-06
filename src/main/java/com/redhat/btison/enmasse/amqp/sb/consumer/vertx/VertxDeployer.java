package com.redhat.btison.enmasse.amqp.sb.consumer.vertx;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

import io.vertx.core.DeploymentOptions;
import io.vertx.core.Vertx;

@Component
public class VertxDeployer {

    private final Logger log = LoggerFactory.getLogger(VertxDeployer.class);

    @Autowired
    private SpringVerticleFactory springVerticleFactory;

    @Autowired
    private Vertx vertx;

    @Value("${vertx.message-consumer.instances}")
    int messageConsumerInstances;

    @EventListener
    public void deployVerticles(ApplicationReadyEvent event) {
        vertx.registerVerticleFactory(springVerticleFactory);
        CompletableFuture<Void> messageProducerDeploymentFuture = new CompletableFuture<>();
        DeploymentOptions workerDeploymentOptions = new DeploymentOptions()
                .setWorker(false)
                .setInstances(messageConsumerInstances);
        vertx.deployVerticle(springVerticleFactory.prefix() + ":" + "MessageConsumerVerticle", workerDeploymentOptions, ar -> {
            if (ar.failed()) {
                messageProducerDeploymentFuture.completeExceptionally(ar.cause());
            } else {
                messageProducerDeploymentFuture.complete(null);
            }
        });
        try {
            messageProducerDeploymentFuture.get(10, TimeUnit.SECONDS);
            log.info("Verticles deployed sucessfully");
        } catch (Exception e) {
            log.error("Error deploying verticles", e);
            throw new RuntimeException("Error deploying verticles");
        }
    }

}
