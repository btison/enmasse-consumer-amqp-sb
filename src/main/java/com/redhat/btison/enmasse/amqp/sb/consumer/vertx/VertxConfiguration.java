package com.redhat.btison.enmasse.amqp.sb.consumer.vertx;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;

@Configuration
public class VertxConfiguration {

    @Value("${vertx.worker.pool.size}")
    int workerPoolSize;

    @Bean
    public Vertx vertx() {
        return Vertx.vertx(new VertxOptions().setWorkerPoolSize(workerPoolSize));
    }

}
