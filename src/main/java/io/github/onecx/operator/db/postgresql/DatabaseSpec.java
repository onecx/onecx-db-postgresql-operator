package io.github.onecx.operator.db.postgresql;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonInclude(JsonInclude.Include.NON_NULL)
public class DatabaseSpec {

    @JsonProperty("host")
    private String host;

    @JsonProperty("user")
    private String user;

    @JsonProperty("name")
    private String name;

    @JsonProperty("password-secrets")
    private String passwordSecrets;

    @JsonProperty("password-key")
    private String passwordKey;

    @JsonProperty("schema")
    private String schema;

    @JsonProperty("extensions")
    private List<String> extensions;

    @JsonProperty("user-search-path")
    private String userSearchPath;

    public String getUser() {
        return user;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getPasswordSecrets() {
        return passwordSecrets;
    }

    public void setPasswordSecrets(String passwordSecrets) {
        this.passwordSecrets = passwordSecrets;
    }

    public String getPasswordKey() {
        return passwordKey;
    }

    public void setPasswordKey(String passwordKey) {
        this.passwordKey = passwordKey;
    }

    public String getSchema() {
        return schema;
    }

    public void setSchema(String schema) {
        this.schema = schema;
    }

    public List<String> getExtensions() {
        return extensions;
    }

    public void setExtensions(List<String> extensions) {
        this.extensions = extensions;
    }

    public String getUserSearchPath() {
        return userSearchPath;
    }

    public void setUserSearchPath(String userSearchPath) {
        this.userSearchPath = userSearchPath;
    }

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    @Override
    public String toString() {
        return "DatabaseSpec{" +
                "user=" + user +
                ", name=" + name +
                ", host=" + host +
                ", password-secrets=" + passwordSecrets +
                ", password-key=" + passwordKey +
                ", schema=" + schema +
                ", extensions=" + extensions +
                ", user-search-path='" + userSearchPath +
                '}';
    }
}
