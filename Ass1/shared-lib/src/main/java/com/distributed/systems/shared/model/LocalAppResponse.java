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

    public static class ResponseData {
        @JsonProperty("inputFileS3Key")
        private String inputFileS3Key;

        @JsonProperty("summaryS3Key")
        private String summaryS3Key;

        public ResponseData() {
        }

        public ResponseData(String inputFileS3Key, String summaryS3Key) {
            this.inputFileS3Key = inputFileS3Key;
            this.summaryS3Key = summaryS3Key;
        }

        public String getInputFileS3Key() {
            return inputFileS3Key;
        }

        public void setInputFileS3Key(String inputFileS3Key) {
            this.inputFileS3Key = inputFileS3Key;
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
                    "inputFileS3Key='" + inputFileS3Key + '\'' +
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
