package com.example.github.connector.dto;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

@JsonIgnoreProperties(ignoreUnknown = true)
public class Repo {

    private String name;

    public String getName() {
        return name;
    }
}
