package com.example.github.connector;
import com.example.github.connector.dto.GithubEvent;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTaskContext;
import org.apache.kafka.connect.storage.OffsetStorageReader;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

public class GithubEventsSourceTaskTest {
    private final ObjectMapper objectMapper = new ObjectMapper();

    @Test
    public void configShouldReadRequiredProperties() {
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
    public void connectorShouldReturnTaskClass() {
        GithubEventsSourceConnector connector = new GithubEventsSourceConnector();
        assertNotNull(connector.taskClass());
        assertEquals(GithubEventsSourceTask.class, connector.taskClass());
    }

    @Test
    public void shouldReturnEmptyListWhenGithubReturnsNoEvents() throws Exception {
        GithubApiClient githubApiClient = mock(GithubApiClient.class);
        when(githubApiClient.fetchEvents()).thenReturn(Collections.emptyList());

        GithubEventsSourceTask task = new GithubEventsSourceTask(githubApiClient);
        task.initialize(mockContextWithoutOffset());
        task.start(testProps());

        List<SourceRecord> records = task.poll();

        assertTrue(records.isEmpty());
    }

    @Test
    public void shouldReturnEmptyListWhenGithubApiFails() throws Exception {
        GithubApiClient githubApiClient = mock(GithubApiClient.class);
        when(githubApiClient.fetchEvents()).thenThrow(new IOException("GitHub API error"));

        GithubEventsSourceTask task = new GithubEventsSourceTask(githubApiClient);
        task.initialize(mockContextWithoutOffset());
        task.start(testProps());

        List<SourceRecord> records = task.poll();

        assertTrue(records.isEmpty());
    }

    @Test
    public void shouldSkipAlreadyProcessedEventsBasedOnOffset() throws Exception {
        GithubApiClient githubApiClient = mock(GithubApiClient.class);

        GithubEvent newEvent = new GithubEvent(
                "1002",
                "PullRequestEvent",
                "stasolsh/github-events-kafka-connector",
                "stasolsh",
                "2026-05-20T10:05:00Z",
                objectMapper.readTree("{\"action\":\"opened\"}")
        );

        GithubEvent oldEvent = new GithubEvent(
                "1001",
                "PushEvent",
                "stasolsh/github-events-kafka-connector",
                "stasolsh",
                "2026-05-20T10:00:00Z",
                objectMapper.readTree("{\"ref\":\"refs/heads/main\"}")
        );

        when(githubApiClient.fetchEvents()).thenReturn(List.of(newEvent, oldEvent));

        GithubEventsSourceTask task = new GithubEventsSourceTask(githubApiClient);
        task.initialize(mockContextWithOffset("1001"));
        task.start(testProps());

        List<SourceRecord> records = task.poll();

        assertEquals(1, records.size());
        assertEquals("1002", records.get(0).key());
        assertEquals(Map.of("last_event_id", "1002"), records.get(0).sourceOffset());
    }

    private Map<String, String> testProps() {
        return Map.of(
                "github.owner", "stasolsh",
                "github.repo", "github-events-kafka-connector",
                "github.token", "",
                "topic", "github.events",
                "poll.interval.ms", "1"
        );
    }

    private SourceTaskContext mockContextWithoutOffset() {
        SourceTaskContext context = mock(SourceTaskContext.class);
        OffsetStorageReader offsetStorageReader = mock(OffsetStorageReader.class);

        when(context.offsetStorageReader()).thenReturn(offsetStorageReader);
        when(offsetStorageReader.offset(anyMap())).thenReturn(null);

        return context;
    }

    private SourceTaskContext mockContextWithOffset(String lastEventId) {
        SourceTaskContext context = mock(SourceTaskContext.class);
        OffsetStorageReader offsetStorageReader = mock(OffsetStorageReader.class);

        when(context.offsetStorageReader()).thenReturn(offsetStorageReader);
        when(offsetStorageReader.offset(anyMap())).thenReturn(Map.of("last_event_id", lastEventId));

        return context;
    }
}
