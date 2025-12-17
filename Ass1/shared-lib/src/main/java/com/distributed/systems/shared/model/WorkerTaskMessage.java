package com.distributed.systems.shared.model;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Message sent from Manager to Worker
 */
public class WorkerTaskMessage {

    public static final String TYPE_URL_PARSE_REQUEST = "urlParseRequest";

    @JsonProperty("type")
    private String type;

    @JsonProperty("data")
    private TaskData data;

    public WorkerTaskMessage() {
    }

    public WorkerTaskMessage(String type, TaskData data) {
        this.type = type;
        this.data = data;
    }

    public static WorkerTaskMessage create(String url, String parsingMethod) {
        return new WorkerTaskMessage(TYPE_URL_PARSE_REQUEST, new TaskData(url, parsingMethod));
    }

    public static WorkerTaskMessage createWithRetry(String url, String parsingMethod, int retryCount) {
        return new WorkerTaskMessage(TYPE_URL_PARSE_REQUEST, new TaskData(url, parsingMethod, retryCount));
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public TaskData getData() {
        return data;
    }

    public void setData(TaskData data) {
        this.data = data;
    }

    public static class TaskData {
        public static final int MAX_RETRIES = 3;

        @JsonProperty("parsingMethod")
        private String parsingMethod; // POS, CONSTITUENCY, or DEPENDENCY

        @JsonProperty("url")
        private String url;

        @JsonProperty("retryCount")
        private int retryCount;

        public TaskData() {
            this.retryCount = 0;
        }

        public TaskData(String url, String parsingMethod) {
            this.parsingMethod = parsingMethod;
            this.url = url;
            this.retryCount = 0;
        }

        public TaskData(String url, String parsingMethod, int retryCount) {
            this.parsingMethod = parsingMethod;
            this.url = url;
            this.retryCount = retryCount;
        }

        public String getParsingMethod() {
            return parsingMethod;
        }

        public void setParsingMethod(String parsingMethod) {
            this.parsingMethod = parsingMethod;
        }

        public String getUrl() {
            return url;
        }

        public void setUrl(String url) {
            this.url = url;
        }

        public int getRetryCount() {
            return retryCount;
        }

        public void setRetryCount(int retryCount) {
            this.retryCount = retryCount;
        }

        public boolean hasExceededMaxRetries() {
            return retryCount >= MAX_RETRIES;
        }

        public TaskData withIncrementedRetry() {
            return new TaskData(this.url, this.parsingMethod, this.retryCount + 1);
        }

        @Override
        public String toString() {
            return "TaskData{" +
                    "parsingMethod='" + parsingMethod + '\'' +
                    ", url='" + url + '\'' +
                    ", retryCount=" + retryCount +
                    '}';
        }
    }

    @Override
    public String toString() {
        return "WorkerTaskMessage{" +
                "type='" + type + '\'' +
                ", data=" + data +
                '}';
    }
}
