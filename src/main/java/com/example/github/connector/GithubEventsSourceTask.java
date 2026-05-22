package com.example.github.connector;

import com.example.github.connector.dto.GithubEvent;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;

public class GithubEventsSourceTask extends SourceTask {
    public GithubEventsSourceTask() {
    }

    GithubEventsSourceTask(GithubApiClient githubApiClient) {
        this.githubApiClient = githubApiClient;
    }

    private static final Logger log = LoggerFactory.getLogger(GithubEventsSourceTask.class);

    private static final String OFFSET_FIELD = "last_event_id";
    private static final String PARTITION_FIELD = "repo";

    private GithubEventsConnectorConfig config;
    private GithubApiClient githubApiClient;
    private ObjectMapper objectMapper;
    private String topic;
    private String repoFullName;
    private long pollIntervalMs;
    private String lastEventId;

    @Override
    public String version() {
        return "1.0.0";
    }

    @Override
    public void start(Map<String, String> props) {
        this.config = new GithubEventsConnectorConfig(props);

        String owner = config.getString(GithubEventsConnectorConfig.GITHUB_OWNER);
        String repo = config.getString(GithubEventsConnectorConfig.GITHUB_REPO);
        String token = config.getPassword(GithubEventsConnectorConfig.GITHUB_TOKEN).value();

        this.topic = config.getString(GithubEventsConnectorConfig.TOPIC);
        this.pollIntervalMs = config.getLong(GithubEventsConnectorConfig.POLL_INTERVAL_MS);

        this.repoFullName = owner + "/" + repo;
        this.objectMapper = new ObjectMapper();
        if (this.githubApiClient == null) {
            this.githubApiClient = new GithubApiClient(owner, repo, token);
        }

        Map<String, Object> offset = context.offsetStorageReader().offset(sourcePartition());
        if (offset != null && offset.get(OFFSET_FIELD) != null) {
            this.lastEventId = offset.get(OFFSET_FIELD).toString();
            log.info("Loaded last GitHub event offset: {}", lastEventId);
        } else {
            log.info("No previous GitHub event offset found. Starting fresh.");
        }
    }

    @Override
    public List<SourceRecord> poll() throws InterruptedException {
        Thread.sleep(pollIntervalMs);

        List<GithubEvent> events;
        try {
            events = githubApiClient.fetchEvents();
        } catch (IOException e) {
            log.error("Failed to fetch GitHub events", e);
            return Collections.emptyList();
        }

        List<GithubEvent> newEvents = filterNewEvents(events);

        if (newEvents.isEmpty()) {
            return Collections.emptyList();
        }

        // GitHub returns newest first. For Kafka, publish oldest first.
        newEvents.sort(Comparator.comparing(GithubEvent::getId));

        return getSourceRecords(newEvents);
    }

    private List<SourceRecord> getSourceRecords(List<GithubEvent> newEvents) {
        List<SourceRecord> records = new ArrayList<>();

        for (GithubEvent event : newEvents) {
            SourceRecord record = new SourceRecord(
                    sourcePartition(),
                    sourceOffset(event.getId()),
                    topic,
                    null,
                    Schema.STRING_SCHEMA,
                    event.getId(),
                    Schema.STRING_SCHEMA,
                    toJson(event)
            );

            records.add(record);
            lastEventId = event.getId();
        }

        log.info("Produced {} GitHub events to topic {}", records.size(), topic);
        return records;
    }


    private List<GithubEvent> filterNewEvents(List<GithubEvent> events) {
        if (lastEventId == null || lastEventId.isBlank()) {
            return events;
        }

        List<GithubEvent> result = new ArrayList<>();
        for (GithubEvent event : events) {
            if (event.getId().equals(lastEventId)) {
                break;
            }
            result.add(event);
        }
        return result;
    }

    private String toJson(GithubEvent event) {
        Map<String, Object> json = new HashMap<>();
        json.put("github_event_id", event.getId());
        json.put("type", event.getType());
        json.put("repo", event.getRepoName());
        json.put("actor", event.getActorLogin());
        json.put("created_at", event.getCreatedAt());
        json.put("payload", event.getPayload());

        try {
            return objectMapper.writeValueAsString(json);
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Failed to serialize GitHub event", e);
        }
    }

    private Map<String, String> sourcePartition() {
        return Collections.singletonMap(PARTITION_FIELD, repoFullName);
    }

    private Map<String, Object> sourceOffset(String eventId) {
        return Collections.singletonMap(OFFSET_FIELD, eventId);
    }

    @Override
    public void stop() {
        log.info("Stopping GitHub Events Source Task");
    }
}
