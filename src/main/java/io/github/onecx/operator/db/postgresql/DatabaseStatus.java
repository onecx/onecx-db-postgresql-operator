package io.github.onecx.operator.db.postgresql;

import com.fasterxml.jackson.annotation.JsonProperty;

import io.javaoperatorsdk.operator.api.ObservedGenerationAwareStatus;

public class DatabaseStatus extends ObservedGenerationAwareStatus {

    @JsonProperty("url")
    private String url;

    @JsonProperty("status")
    private String status;

    @JsonProperty("user")
    private String user;

    @JsonProperty("password-secrets")
    private String passwordSecrets;

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public String getUser() {
        return user;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public String getPasswordSecrets() {
        return passwordSecrets;
    }

    public void setPasswordSecrets(String passwordSecrets) {
        this.passwordSecrets = passwordSecrets;
    }
}
