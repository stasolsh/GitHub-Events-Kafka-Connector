package com.example.github.connector;

import com.fasterxml.jackson.databind.JsonNode;

public class GithubEvent {
    private String id;
    private String type;
    private String repoName;
    private String actorLogin;
    private String createdAt;
    private JsonNode payload;

    public GithubEvent() {
    }

    public GithubEvent(String id, String type, String repoName, String actorLogin, String createdAt, JsonNode payload) {
        this.id = id;
        this.type = type;
        this.repoName = repoName;
        this.actorLogin = actorLogin;
        this.createdAt = createdAt;
        this.payload = payload;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getRepoName() {
        return repoName;
    }

    public void setRepoName(String repoName) {
        this.repoName = repoName;
    }

    public String getActorLogin() {
        return actorLogin;
    }

    public void setActorLogin(String actorLogin) {
        this.actorLogin = actorLogin;
    }

    public String getCreatedAt() {
        return createdAt;
    }

    public void setCreatedAt(String createdAt) {
        this.createdAt = createdAt;
    }

    public JsonNode getPayload() {
        return payload;
    }

    public void setPayload(JsonNode payload) {
        this.payload = payload;
    }
}
