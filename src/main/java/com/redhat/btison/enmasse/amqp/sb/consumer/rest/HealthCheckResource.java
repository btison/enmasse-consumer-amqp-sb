package com.redhat.btison.enmasse.amqp.sb.consumer.rest;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

import org.springframework.boot.actuate.health.Health;
import org.springframework.stereotype.Component;

@Component
@Path("/")
public class HealthCheckResource {

    @GET
    @Path("/health")
    @Produces(MediaType.APPLICATION_JSON)
    public Health getHealth() {
        return Health.up().build();
    }

}
