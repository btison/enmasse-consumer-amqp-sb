package com.redhat.btison.enmasse.amqp.sb.consumer.vertx;

import java.io.UnsupportedEncodingException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.redhat.btison.enmasse.amqp.sb.consumer.AmqpConfigurationProperties;
import com.redhat.btison.enmasse.amqp.sb.consumer.service.MessageCounterService;

import io.vertx.amqpbridge.AmqpBridge;
import io.vertx.amqpbridge.AmqpBridgeOptions;
import io.vertx.amqpbridge.AmqpConstants;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.eventbus.MessageConsumer;
import io.vertx.core.eventbus.MessageProducer;
import io.vertx.core.json.JsonObject;
import io.vertx.core.net.JksOptions;

@Component("MessageConsumerVerticle")
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class MessageConsumerVerticle extends AbstractVerticle {

    private final Logger log = LoggerFactory.getLogger(MessageConsumerVerticle.class);

    @Autowired
    private AmqpConfigurationProperties properties;

    @Autowired
    private MessageCounterService messageCounterService;

    private AmqpBridge bridge;

    private MessageProducer<JsonObject> producer;

    private MessageConsumer<JsonObject> consumer;

    @Override
    public void start(Future<Void> startFuture) throws Exception {
        AmqpBridgeOptions bridgeOptions = new AmqpBridgeOptions();
        bridgeOptions.setSsl(properties.getSsl().isEnabled());
        bridgeOptions.setTrustAll(properties.getSsl().isTrustAll());
        bridgeOptions.setHostnameVerificationAlgorithm(properties.getSsl().isVerifyHost() == false ? "" : "HTTPS");
        if (!bridgeOptions.isTrustAll()) {
            JksOptions jksOptions = new JksOptions()
                    .setPath(properties.getTruststore().getPath())
                    .setPassword(properties.getTruststore().getPassword());
            bridgeOptions.setTrustStoreOptions(jksOptions);
        }
        bridge = AmqpBridge.create(vertx, bridgeOptions);
        String host = properties.getHost();
        int port = properties.getPort();
        String username = properties.getUser();
        String password = properties.getPassword();
        bridge.start(host, port, username, password, ar -> {
            if (ar.failed()) {
                startFuture.fail(ar.cause());
            } else {
                bridgeStarted();
                startFuture.complete();
            }
        });
    }

    private void bridgeStarted() {
        consumer = bridge.<JsonObject>createConsumer(properties.getAddress())
                .exceptionHandler(t -> t.printStackTrace());
        consumer.handler(msg -> {
            JsonObject jsonObject = msg.body();
            if (AmqpConstants.BODY_TYPE_DATA.equals(jsonObject.getString(AmqpConstants.BODY_TYPE))) {
                String text = "";
                try {
                    text = new String(jsonObject.getBinary(AmqpConstants.BODY, new byte[] {}), "UTF-8");
                } catch (UnsupportedEncodingException ignore) {}
                messageCounterService.countMessage(text);
            } else {
                log.info("Message data type " + jsonObject.getString(AmqpConstants.BODY_TYPE) + " not supported");
            }
        });
    }

    @Override
    public void stop(Future<Void> stopFuture) throws Exception {
        if (producer != null) {
            producer.close();
        }
        if (bridge != null) {
            bridge.close(ar -> {});
        }
    }
}
