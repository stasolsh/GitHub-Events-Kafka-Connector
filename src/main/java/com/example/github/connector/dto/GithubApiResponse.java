package com.example.github.connector.dto;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
public class GithubApiResponse {

    private String id;
    private String type;

    private Repo repo;
    private Actor actor;

    @JsonProperty("created_at")
    private String createdAt;

    private Object payload;

    public String getId() {
        return id;
    }

    public String getType() {
        return type;
    }

    public Repo getRepo() {
        return repo;
    }

    public Actor getActor() {
        return actor;
    }

    public String getCreatedAt() {
        return createdAt;
    }

    public Object getPayload() {
        return payload;
    }
}
