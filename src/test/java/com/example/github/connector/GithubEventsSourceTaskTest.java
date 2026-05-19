package com.example.github.connector;

import com.example.github.connector.GithubEventsConnectorConfig;
import com.example.github.connector.GithubEventsSourceConnector;
import com.example.github.connector.GithubEventsSourceTask;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

class GithubEventsSourceTaskTest {

    @Test
    void configShouldReadRequiredProperties() {
        Map<String, String> props = new HashMap<>();
        props.put(GithubEventsConnectorConfig.GITHUB_OWNER, "apache");
        props.put(GithubEventsConnectorConfig.GITHUB_REPO, "kafka");
        props.put(GithubEventsConnectorConfig.TOPIC, "github.events");
        props.put(GithubEventsConnectorConfig.POLL_INTERVAL_MS, "10000");

        GithubEventsConnectorConfig config = new GithubEventsConnectorConfig(props);

        assertEquals("apache", config.getString(GithubEventsConnectorConfig.GITHUB_OWNER));
        assertEquals("kafka", config.getString(GithubEventsConnectorConfig.GITHUB_REPO));
        assertEquals("github.events", config.getString(GithubEventsConnectorConfig.TOPIC));
        assertEquals(10000L, config.getLong(GithubEventsConnectorConfig.POLL_INTERVAL_MS));
    }

    @Test
    void connectorShouldReturnTaskClass() {
        GithubEventsSourceConnector connector = new GithubEventsSourceConnector();
        assertNotNull(connector.taskClass());
        assertEquals(GithubEventsSourceTask.class, connector.taskClass());
    }
}
