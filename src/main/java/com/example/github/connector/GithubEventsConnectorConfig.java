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
            );

    public GithubEventsConnectorConfig(Map<String, String> props) {
        super(CONFIG_DEF, props);
    }
}
