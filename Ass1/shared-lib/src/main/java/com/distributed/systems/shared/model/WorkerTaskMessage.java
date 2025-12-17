package com.distributed.systems.shared.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonCreator;

/**
 * Message sent from Manager to Worker
 */
public class WorkerTaskMessage {

    public static final String TYPE_URL_PARSE_REQUEST = "urlParseRequest";
    public static final String TYPE_TERMINATE = "terminate";

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

    public static WorkerTaskMessage createTask(String parsingMethod, String url, String jobId) {
        return new WorkerTaskMessage(TYPE_URL_PARSE_REQUEST, new TaskData(parsingMethod, url, jobId));
    }

    public static WorkerTaskMessage createTerminate() {
        return new WorkerTaskMessage(TYPE_TERMINATE, null);
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
        @JsonProperty("parsingMethod")
        private String parsingMethod; // POS, CONSTITUENCY, or DEPENDENCY

        @JsonProperty("url")
        private String url;

        @JsonProperty("jobId")
        private String jobId;

        public TaskData() {
        }

        @JsonCreator
        public TaskData(
                @JsonProperty("parsingMethod") String parsingMethod,
                @JsonProperty("url") String url,
                @JsonProperty("jobId") String jobId) {
            this.parsingMethod = parsingMethod;
            this.url = url;
            this.jobId = jobId;
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

        public String getJobId() {
            return jobId;
        }

        public void setJobId(String jobId) {
            this.jobId = jobId;
        }

        @Override
        public String toString() {
            return "TaskData{" +
                    "parsingMethod='" + parsingMethod + '\'' +
                    ", url='" + url + '\'' +
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
