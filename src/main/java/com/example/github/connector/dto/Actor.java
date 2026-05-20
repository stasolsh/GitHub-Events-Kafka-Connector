package com.example.github.connector.dto;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

@JsonIgnoreProperties(ignoreUnknown = true)
public class Actor {

    private String login;

    public String getLogin() {
        return login;
    }
}
