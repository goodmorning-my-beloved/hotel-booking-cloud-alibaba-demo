package com.example.hotel.sentinel.token.config;

import com.alibaba.csp.sentinel.cluster.ClusterStateManager;
import com.alibaba.csp.sentinel.cluster.flow.rule.ClusterFlowRuleManager;
import com.alibaba.csp.sentinel.cluster.server.config.ClusterServerConfigManager;
import com.alibaba.csp.sentinel.cluster.server.config.ServerFlowConfig;
import com.alibaba.csp.sentinel.cluster.server.config.ServerTransportConfig;
import com.alibaba.csp.sentinel.datasource.nacos.NacosDataSource;
import com.alibaba.csp.sentinel.property.PropertyListener;
import com.alibaba.csp.sentinel.slots.block.flow.FlowRule;
import com.alibaba.nacos.api.PropertyKeyConst;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.event.EventListener;
import org.springframework.util.StringUtils;

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Properties;

@Configuration
public class SentinelTokenServerConfig {

    private static final Logger log = LoggerFactory.getLogger(SentinelTokenServerConfig.class);
    private static final TypeReference<List<FlowRule>> FLOW_RULE_LIST = new TypeReference<>() {
    };

    private final ObjectMapper objectMapper;

    @Value("${sentinel.token-server.namespaces}")
    private String namespaces;

    @Value("${sentinel.token-server.port}")
    private int tokenServerPort;

    @Value("${sentinel.token-server.idle-seconds}")
    private int idleSeconds;

    @Value("${sentinel.token-server.max-allowed-qps}")
    private double maxAllowedQps;

    @Value("${sentinel.token-server.nacos.server-addr}")
    private String nacosServerAddr;

    @Value("${sentinel.token-server.nacos.namespace:}")
    private String nacosNamespace;

    @Value("${sentinel.token-server.nacos.group-id}")
    private String nacosGroupId;

    @Value("${sentinel.token-server.nacos.flow-rule-data-ids}")
    private String nacosDataIds;

    public SentinelTokenServerConfig(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    @EventListener(ApplicationReadyEvent.class)
    public void startTokenServer() throws Exception {
        List<String> namespaceList = splitCommaSeparated(namespaces);
        List<String> dataIdList = splitCommaSeparated(nacosDataIds);
        if (namespaceList.isEmpty()) {
            throw new IllegalStateException("sentinel.token-server.namespaces must not be empty");
        }
        if (namespaceList.size() != dataIdList.size()) {
            throw new IllegalStateException("sentinel.token-server.namespaces count must match "
                    + "sentinel.token-server.nacos.flow-rule-data-ids count");
        }

        ClusterServerConfigManager.setEmbedded(false);
        ClusterServerConfigManager.loadGlobalTransportConfig(new ServerTransportConfig(tokenServerPort, idleSeconds));
        ClusterServerConfigManager.loadServerNamespaceSet(new LinkedHashSet<>(namespaceList));
        ClusterServerConfigManager.loadGlobalFlowConfig(new ServerFlowConfig().setMaxAllowedQps(maxAllowedQps));
        namespaceList.forEach(ClusterFlowRuleManager::registerPropertyIfAbsent);

        for (int i = 0; i < namespaceList.size(); i++) {
            subscribeFlowRulesFromNacos(namespaceList.get(i), dataIdList.get(i));
        }

        boolean started = ClusterStateManager.setToServer();
        log.info("Sentinel independent token server mode={}, namespaces={}, tokenPort={}, nacosNamespace={}, "
                        + "nacosGroup={}, dataIds={}, started={}",
                ClusterStateManager.getMode(), namespaceList, tokenServerPort, nacosNamespace,
                nacosGroupId, dataIdList, started);
    }

    private void subscribeFlowRulesFromNacos(String namespace, String dataId) throws Exception {
        NacosDataSource<List<FlowRule>> dataSource = new NacosDataSource<>(
                nacosProperties(), nacosGroupId, dataId, source -> parseFlowRules(source, dataId));

        dataSource.getProperty().addListener(new PropertyListener<>() {
            @Override
            public void configUpdate(List<FlowRule> rules) {
                loadClusterRules(namespace, rules, "updated");
            }

            @Override
            public void configLoad(List<FlowRule> rules) {
                loadClusterRules(namespace, rules, "loaded");
            }
        });

        loadClusterRules(namespace, dataSource.loadConfig(), "initial");
    }

    private Properties nacosProperties() {
        Properties properties = new Properties();
        properties.setProperty(PropertyKeyConst.SERVER_ADDR, nacosServerAddr);
        if (StringUtils.hasText(nacosNamespace)) {
            properties.setProperty(PropertyKeyConst.NAMESPACE, nacosNamespace);
        }
        return properties;
    }

    private List<FlowRule> parseFlowRules(String source, String dataId) {
        if (!StringUtils.hasText(source)) {
            return Collections.emptyList();
        }
        try {
            return objectMapper.readValue(source, FLOW_RULE_LIST);
        } catch (Exception ex) {
            throw new IllegalArgumentException("Failed to parse Sentinel flow rules from Nacos dataId=" + dataId, ex);
        }
    }

    private void loadClusterRules(String namespace, List<FlowRule> rules, String phase) {
        List<FlowRule> safeRules = rules == null ? Collections.emptyList() : rules;
        ClusterFlowRuleManager.loadRules(namespace, safeRules);
        log.info("Sentinel cluster flow rules {} for namespace={}, count={}", phase, namespace, safeRules.size());
    }

    private List<String> splitCommaSeparated(String value) {
        if (!StringUtils.hasText(value)) {
            return Collections.emptyList();
        }
        return Arrays.stream(value.split(","))
                .map(String::trim)
                .filter(StringUtils::hasText)
                .toList();
    }
}
