package com.redhat.btison.enmasse.amqp.sb.consumer.vertx;

import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.stereotype.Component;

import io.vertx.core.Verticle;
import io.vertx.core.spi.VerticleFactory;

@Component
public class SpringVerticleFactory implements VerticleFactory, ApplicationContextAware {

    private ApplicationContext applicationContext;

    @Override
    public boolean blockingCreate() {
      return true;
  }

    @Override
    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
        this.applicationContext = applicationContext;
    }

    @Override
    public String prefix() {
        return "spring-app";
    }

    @Override
    public Verticle createVerticle(String verticleName, ClassLoader classLoader) throws Exception {
        String clazz = VerticleFactory.removePrefix(verticleName);
        return (Verticle) applicationContext.getBean(clazz);
    }

}
