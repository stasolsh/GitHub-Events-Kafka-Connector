package com.example.github.connector;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

public class GithubEventsSourceConnectorTest {

    @Test
    public void shouldReturnCorrectTaskClass() {
        GithubEventsSourceConnector connector = new GithubEventsSourceConnector();

        assertEquals(GithubEventsSourceTask.class, connector.taskClass());
    }

    @Test
    public void shouldCreateTaskConfigs() {
        GithubEventsSourceConnector connector = new GithubEventsSourceConnector();

        Map<String, String> props = Map.of(
                "github.owner", "apache",
                "github.repo", "kafka",
                "topic", "github.events"
        );

        connector.start(props);

        List<Map<String, String>> taskConfigs = connector.taskConfigs(1);

        assertEquals(1, taskConfigs.size());
        assertEquals("apache", taskConfigs.get(0).get("github.owner"));
        assertEquals("kafka", taskConfigs.get(0).get("github.repo"));
    }

    @Test
    public void shouldReturnVersion() {
        GithubEventsSourceConnector connector = new GithubEventsSourceConnector();

        assertEquals("1.0.0", connector.version());
    }
}