package com.redhat.btison.enmasse.amqp.sb.consumer.service;

import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Component
public class MessageCounterService {

    private static final Logger log = LoggerFactory.getLogger(MessageCounterService.class);

    private AtomicInteger counter = new AtomicInteger(0);

    @Value("${logging.factor}")
    private int factor;

    public void countMessage(String msg) {
        int count = counter.incrementAndGet();
        if ( count % factor == 0 ) {
            log.info("Consumed messages:" + count + ". Last message: " + msg);
        }
    }

    public void resetCounter() {
        counter.set(0);
    }

}
