package com.example.github.connector;

import com.example.github.connector.dto.GithubEvent;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

public class GithubApiClientTest {

    @Test
    @Disabled("Calls real GitHub API. Enable manually for smoke testing.")
    public void shouldFetchGithubEventsFromPublicRepository() throws Exception {
        GithubApiClient client = new GithubApiClient("apache", "kafka", "");

        List<GithubEvent> events = client.fetchEvents();

        assertFalse(events.isEmpty());
        assertNotNull(events.get(0).getId());
        assertNotNull(events.get(0).getType());
    }
}