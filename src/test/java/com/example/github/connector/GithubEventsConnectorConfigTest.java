package com.example.github.connector;

import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class GithubEventsConnectorConfigTest {

    @Test
    public void shouldReadRequiredConfigValues() {
        GithubEventsConnectorConfig config = new GithubEventsConnectorConfig(Map.of(
                "github.owner", "apache",
                "github.repo", "kafka",
                "topic", "github.events",
                "poll.interval.ms", "100"
        ));

        assertEquals("apache", config.getString(GithubEventsConnectorConfig.GITHUB_OWNER));
        assertEquals("kafka", config.getString(GithubEventsConnectorConfig.GITHUB_REPO));
        assertEquals("github.events", config.getString(GithubEventsConnectorConfig.TOPIC));
        assertEquals(100L, config.getLong(GithubEventsConnectorConfig.POLL_INTERVAL_MS));
    }

    @Test
    public void shouldUseDefaultTopicAndPollInterval() {
        GithubEventsConnectorConfig config = new GithubEventsConnectorConfig(Map.of(
                "github.owner", "apache",
                "github.repo", "kafka"
        ));

        assertEquals("github.events", config.getString(GithubEventsConnectorConfig.TOPIC));
        assertEquals(30_000L, config.getLong(GithubEventsConnectorConfig.POLL_INTERVAL_MS));
    }
}