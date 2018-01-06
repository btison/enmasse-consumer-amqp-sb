package com.redhat.btison.enmasse.amqp.sb.consumer;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

@Component
@ConfigurationProperties(prefix="amqp")
public class AmqpConfigurationProperties {

    private String host;

    private int port;

    private String user;

    private String password;

    private String address;

    private Ssl ssl = new Ssl();

    private TrustStore truststore = new TrustStore();

    public Ssl getSsl() {
        return ssl;
    }

    public TrustStore getTruststore() {
        return truststore;
    }

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public String getUser() {
        return user;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public String getAddress() {
        return address;
    }

    public void setAddress(String address) {
        this.address = address;
    }

    public class TrustStore {

        private String path;

        private String password;

        public String getPath() {
            return path;
        }

        public void setPath(String path) {
            this.path = path;
        }

        public String getPassword() {
            return password;
        }

        public void setPassword(String password) {
            this.password = password;
        }
    }

    public class Ssl {

        private boolean enabled;

        private boolean verifyHost;

        private boolean trustAll;

        public boolean isEnabled() {
            return enabled;
        }

        public void setEnabled(boolean enabled) {
            this.enabled = enabled;
        }

        public boolean isVerifyHost() {
            return verifyHost;
        }

        public void setVerifyHost(boolean verifyHost) {
            this.verifyHost = verifyHost;
        }

        public boolean isTrustAll() {
            return trustAll;
        }

        public void setTrustAll(boolean trustAll) {
            this.trustAll = trustAll;
        }

    }
}
