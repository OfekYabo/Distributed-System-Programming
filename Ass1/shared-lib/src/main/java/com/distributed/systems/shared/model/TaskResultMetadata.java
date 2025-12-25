package com.distributed.systems.shared.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Metadata for a completed task, stored as JSON in S3.
 * This allows the Manager to be stateless regarding result storage.
 */
public class TaskResultMetadata {
    private final String originalUrl;
    private final String parsingMethod;
    private final String outputS3Url;
    private final boolean success;
    private final String errorMessage;

    @JsonCreator
    public TaskResultMetadata(
            @JsonProperty("originalUrl") String originalUrl,
            @JsonProperty("parsingMethod") String parsingMethod,
            @JsonProperty("outputS3Url") String outputS3Url,
            @JsonProperty("success") boolean success,
            @JsonProperty("errorMessage") String errorMessage) {
        this.originalUrl = originalUrl;
        this.parsingMethod = parsingMethod;
        this.outputS3Url = outputS3Url;
        this.success = success;
        this.errorMessage = errorMessage;
    }

    public String getOriginalUrl() {
        return originalUrl;
    }

    public String getParsingMethod() {
        return parsingMethod;
    }

    public String getOutputS3Url() {
        return outputS3Url;
    }

    public boolean isSuccess() {
        return success;
    }

    public String getErrorMessage() {
        return errorMessage;
    }
}
