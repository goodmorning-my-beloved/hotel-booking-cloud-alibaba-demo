package com.example.hotel.sentinel.token.config;

import com.alibaba.csp.sentinel.cluster.ClusterStateManager;
import com.alibaba.csp.sentinel.cluster.flow.rule.ClusterFlowRuleManager;
import com.alibaba.csp.sentinel.cluster.server.config.ClusterServerConfigManager;
import com.alibaba.csp.sentinel.cluster.server.config.ServerFlowConfig;
import com.alibaba.csp.sentinel.cluster.server.config.ServerTransportConfig;
import com.alibaba.csp.sentinel.datasource.nacos.NacosDataSource;
import com.alibaba.csp.sentinel.property.PropertyListener;
import com.alibaba.csp.sentinel.slots.block.flow.FlowRule;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.event.EventListener;
import org.springframework.util.StringUtils;

import java.util.Collections;
import java.util.List;
import java.util.Set;

@Configuration
public class SentinelTokenServerConfig {

    private static final Logger log = LoggerFactory.getLogger(SentinelTokenServerConfig.class);
    private static final TypeReference<List<FlowRule>> FLOW_RULE_LIST = new TypeReference<>() {
    };

    private final ObjectMapper objectMapper;

    @Value("${sentinel.token-server.namespace}")
    private String namespace;

    @Value("${sentinel.token-server.port}")
    private int tokenServerPort;

    @Value("${sentinel.token-server.idle-seconds}")
    private int idleSeconds;

    @Value("${sentinel.token-server.max-allowed-qps}")
    private double maxAllowedQps;

    @Value("${sentinel.token-server.nacos.server-addr}")
    private String nacosServerAddr;

    @Value("${sentinel.token-server.nacos.group-id}")
    private String nacosGroupId;

    @Value("${sentinel.token-server.nacos.data-id}")
    private String nacosDataId;

    public SentinelTokenServerConfig(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    @EventListener(ApplicationReadyEvent.class)
    public void startTokenServer() throws Exception {
        ClusterServerConfigManager.setEmbedded(false);
        ClusterServerConfigManager.loadGlobalTransportConfig(new ServerTransportConfig(tokenServerPort, idleSeconds));
        ClusterServerConfigManager.loadServerNamespaceSet(Set.of(namespace));
        ClusterServerConfigManager.loadGlobalFlowConfig(new ServerFlowConfig().setMaxAllowedQps(maxAllowedQps));
        ClusterFlowRuleManager.registerPropertyIfAbsent(namespace);

        subscribeFlowRulesFromNacos();

        boolean started = ClusterStateManager.setToServer();
        log.info("Sentinel independent token server mode={}, namespace={}, tokenPort={}, nacos={}/{}, started={}",
                ClusterStateManager.getMode(), namespace, tokenServerPort, nacosGroupId, nacosDataId, started);
    }

    private void subscribeFlowRulesFromNacos() throws Exception {
        NacosDataSource<List<FlowRule>> dataSource = new NacosDataSource<>(
                nacosServerAddr, nacosGroupId, nacosDataId, this::parseFlowRules);

        dataSource.getProperty().addListener(new PropertyListener<>() {
            @Override
            public void configUpdate(List<FlowRule> rules) {
                loadClusterRules(rules, "updated");
            }

            @Override
            public void configLoad(List<FlowRule> rules) {
                loadClusterRules(rules, "loaded");
            }
        });

        loadClusterRules(dataSource.loadConfig(), "initial");
    }

    private List<FlowRule> parseFlowRules(String source) {
        if (!StringUtils.hasText(source)) {
            return Collections.emptyList();
        }
        try {
            return objectMapper.readValue(source, FLOW_RULE_LIST);
        } catch (Exception ex) {
            throw new IllegalArgumentException("Failed to parse Sentinel flow rules from Nacos dataId=" + nacosDataId, ex);
        }
    }

    private void loadClusterRules(List<FlowRule> rules, String phase) {
        List<FlowRule> safeRules = rules == null ? Collections.emptyList() : rules;
        ClusterFlowRuleManager.loadRules(namespace, safeRules);
        log.info("Sentinel cluster flow rules {} for namespace={}, count={}", phase, namespace, safeRules.size());
    }
}
