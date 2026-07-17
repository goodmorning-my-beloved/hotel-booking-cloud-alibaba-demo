package com.example.hotel.sentinel.bff.config;

import com.alibaba.csp.sentinel.cluster.ClusterStateManager;
import com.alibaba.csp.sentinel.cluster.client.config.ClusterClientAssignConfig;
import com.alibaba.csp.sentinel.cluster.client.config.ClusterClientConfig;
import com.alibaba.csp.sentinel.cluster.client.config.ClusterClientConfigManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.event.EventListener;

import java.util.Locale;

@Configuration
public class SentinelClusterConfig {

    private static final Logger log = LoggerFactory.getLogger(SentinelClusterConfig.class);

    @Value("${sentinel.cluster.mode:off}")
    private String mode;

    @Value("${sentinel.cluster.server.host:127.0.0.1}")
    private String serverHost;

    @Value("${sentinel.cluster.server.port:18730}")
    private int serverPort;

    @Value("${sentinel.cluster.client.request-timeout:20}")
    private int requestTimeout;

    @EventListener(ApplicationReadyEvent.class)
    public void configureClusterMode() {
        String normalizedMode = mode.trim().toLowerCase(Locale.ROOT);
        if ("client".equals(normalizedMode)) {
            startTokenClient();
            return;
        }
        if ("server".equals(normalizedMode)) {
            log.warn("SENTINEL_CLUSTER_MODE=server is no longer supported in sentinel-bff-service. "
                    + "Start sentinel-token-server independently and run BFF with SENTINEL_CLUSTER_MODE=client.");
            return;
        }

        log.info("Sentinel cluster flow control client is disabled. Set SENTINEL_CLUSTER_MODE=client to enable it.");
    }

    private void startTokenClient() {
        ClusterClientConfigManager.applyNewAssignConfig(new ClusterClientAssignConfig(serverHost, serverPort));
        ClusterClientConfigManager.applyNewConfig(new ClusterClientConfig().setRequestTimeout(requestTimeout));

        boolean started = ClusterStateManager.setToClient();
        log.info("Sentinel cluster token client mode={}, server={}:{}, requestTimeoutMs={}, started={}",
                ClusterStateManager.getMode(), serverHost, serverPort, requestTimeout, started);
    }
}
