package com.distributed.systems.shared.model;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Message received from Manager to Local Application
 */
public class LocalAppResponse {

    public static final String TYPE_TASK_COMPLETE = "taskComplete";

    @JsonProperty("type")
    private String type;

    @JsonProperty("data")
    private ResponseData data;

    public LocalAppResponse() {
    }

    public LocalAppResponse(String type, ResponseData data) {
        this.type = type;
        this.data = data;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public ResponseData getData() {
        return data;
    }

    public void setData(ResponseData data) {
        this.data = data;
    }

    public boolean isTaskComplete() {
        return TYPE_TASK_COMPLETE.equals(type);
    }

    public static LocalAppResponse taskComplete(String jobId, String summaryS3Key) {
        return new LocalAppResponse(TYPE_TASK_COMPLETE, new ResponseData(jobId, summaryS3Key));
    }

    public static class ResponseData {
        @JsonProperty("jobId")
        private String jobId;

        @JsonProperty("summaryS3Key")
        private String summaryS3Key;

        public ResponseData() {
        }

        public ResponseData(String jobId, String summaryS3Key) {
            this.jobId = jobId;
            this.summaryS3Key = summaryS3Key;
        }

        public String getJobId() {
            return jobId;
        }

        public void setJobId(String jobId) {
            this.jobId = jobId;
        }

        public String getSummaryS3Key() {
            return summaryS3Key;
        }

        public void setSummaryS3Key(String summaryS3Key) {
            this.summaryS3Key = summaryS3Key;
        }

        @Override
        public String toString() {
            return "ResponseData{" +
                    "jobId='" + jobId + '\'' +
                    ", summaryS3Key='" + summaryS3Key + '\'' +
                    '}';
        }
    }

    @Override
    public String toString() {
        return "LocalAppResponse{" +
                "type='" + type + '\'' +
                ", data=" + data +
                '}';
    }
}
