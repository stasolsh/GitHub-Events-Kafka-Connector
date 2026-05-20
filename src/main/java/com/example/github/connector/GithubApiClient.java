package com.example.github.connector;

import com.example.github.connector.dto.GithubApiResponse;
import com.example.github.connector.dto.GithubEvent;
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

        GithubApiResponse[] responses =
                objectMapper.readValue(response.body(), GithubApiResponse[].class);
        List<GithubEvent> events = new ArrayList<>();

        for (GithubApiResponse responseItem : responses) {

            events.add(new GithubEvent(
                    responseItem.getId(),
                    responseItem.getType(),
                    responseItem.getRepo() != null ? responseItem.getRepo().getName() : null,
                    responseItem.getActor() != null ? responseItem.getActor().getLogin() : null,
                    responseItem.getCreatedAt(),
                    responseItem.getPayload()
            ));
        }

        return events;
    }
}
