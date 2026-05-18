package com.example.github.connector;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.source.SourceConnector;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class GithubEventsSourceConnector extends SourceConnector {

    private Map<String, String> configProps;

    @Override
    public void start(Map<String, String> props) {
        new GithubEventsConnectorConfig(props);
        this.configProps = new HashMap<>(props);
    }

    @Override
    public Class<? extends Task> taskClass() {
        return GithubEventsSourceTask.class;
    }

    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        List<Map<String, String>> configs = new ArrayList<>();
        configs.add(configProps);
        return configs;
    }

    @Override
    public void stop() {
        // Nothing to clean up in MVP
    }

    @Override
    public ConfigDef config() {
        return GithubEventsConnectorConfig.CONFIG_DEF;
    }

    @Override
    public String version() {
        return "1.0.0";
    }
}
