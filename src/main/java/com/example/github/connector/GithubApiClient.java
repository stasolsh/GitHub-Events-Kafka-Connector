package com.example.github.connector;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

public class GithubApiClient {

    private final HttpClient httpClient;
    private final ObjectMapper objectMapper;
    private final String owner;
    private final String repo;
    private final String token;

    public GithubApiClient(String owner, String repo, String token) {
        this.httpClient = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(10))
                .build();
        this.objectMapper = new ObjectMapper();
        this.owner = owner;
        this.repo = repo;
        this.token = token;
    }

    public List<GithubEvent> fetchEvents() throws IOException, InterruptedException {
        String url = "https://api.github.com/repos/" + owner + "/" + repo + "/events";

        HttpRequest.Builder requestBuilder = HttpRequest.newBuilder()
                .uri(URI.create(url))
                .timeout(Duration.ofSeconds(20))
                .header("Accept", "application/vnd.github+json")
                .header("User-Agent", "github-events-kafka-connector");

        if (token != null && !token.isBlank()) {
            requestBuilder.header("Authorization", "Bearer " + token);
        }

        HttpRequest request = requestBuilder.GET().build();
        HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());

        if (response.statusCode() < 200 || response.statusCode() >= 300) {
            throw new IOException("GitHub API returned status " + response.statusCode() + ": " + response.body());
        }

        JsonNode root = objectMapper.readTree(response.body());
        List<GithubEvent> events = new ArrayList<>();

        for (JsonNode node : root) {
            String id = node.path("id").asText();
            String type = node.path("type").asText();
            String repoName = node.path("repo").path("name").asText();
            String actorLogin = node.path("actor").path("login").asText();
            String createdAt = node.path("created_at").asText();
            JsonNode payload = node.path("payload");

            events.add(new GithubEvent(id, type, repoName, actorLogin, createdAt, payload));
        }

        return events;
    }
}
