package com.example.github.connector;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;

import java.util.Map;

public class GithubEventsConnectorConfig extends AbstractConfig {

    public static final String GITHUB_OWNER = "github.owner";
    public static final String GITHUB_REPO = "github.repo";
    public static final String GITHUB_TOKEN = "github.token";
    public static final String TOPIC = "topic";
    public static final String POLL_INTERVAL_MS = "poll.interval.ms";
    public static final String RETRY_MAX_ATTEMPTS = "retry.max.attempts";
    public static final String RETRY_INITIAL_BACKOFF_MS = "retry.initial.backoff.ms";
    public static final String RETRY_MAX_BACKOFF_MS = "retry.max.backoff.ms";

    public static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(
                    GITHUB_OWNER,
                    ConfigDef.Type.STRING,
                    ConfigDef.Importance.HIGH,
                    "GitHub repository owner"
            )
            .define(
                    GITHUB_REPO,
                    ConfigDef.Type.STRING,
                    ConfigDef.Importance.HIGH,
                    "GitHub repository name"
            )
            .define(
                    GITHUB_TOKEN,
                    ConfigDef.Type.PASSWORD,
                    "",
                    ConfigDef.Importance.MEDIUM,
                    "GitHub personal access token. Optional for public repositories, but recommended because of rate limits."
            )
            .define(
                    TOPIC,
                    ConfigDef.Type.STRING,
                    "github.events",
                    ConfigDef.Importance.HIGH,
                    "Kafka topic where GitHub events will be published"
            )
            .define(
                    POLL_INTERVAL_MS,
                    ConfigDef.Type.LONG,
                    30_000L,
                    ConfigDef.Importance.MEDIUM,
                    "Polling interval in milliseconds"
            )
            .define(
                    RETRY_MAX_ATTEMPTS,
                    ConfigDef.Type.INT,
                    3,
                    ConfigDef.Importance.MEDIUM,
                    "Max retry attempts"
            )
            .define(
                    RETRY_INITIAL_BACKOFF_MS,
                    ConfigDef.Type.LONG,
                    1000L,
                    ConfigDef.Importance.MEDIUM,
                    "Initial retry backoff"
            )
            .define(
                    RETRY_MAX_BACKOFF_MS,
                    ConfigDef.Type.LONG,
                    10000L,
                    ConfigDef.Importance.MEDIUM,
                    "Max retry backoff"
            );

    public GithubEventsConnectorConfig(Map<String, String> props) {
        super(CONFIG_DEF, props);
    }
}
