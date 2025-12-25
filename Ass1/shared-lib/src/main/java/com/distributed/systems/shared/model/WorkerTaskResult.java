package com.distributed.systems.shared.model;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Success/Error response from Worker to Manager
 */
public class WorkerTaskResult {

    public static final String TYPE_URL_PARSE_RESPONSE = "urlParseResponse";

    @JsonProperty("type")
    private String type;

    @JsonProperty("data")
    private ResultData data;

    public WorkerTaskResult() {
    }

    public WorkerTaskResult(String type, ResultData data) {
        this.type = type;
        this.data = data;
    }

    public static WorkerTaskResult createSuccess(String jobId) {
        return new WorkerTaskResult(TYPE_URL_PARSE_RESPONSE,
                new ResultData(true, jobId));
    }

    public static WorkerTaskResult createError(String jobId) {
        return new WorkerTaskResult(TYPE_URL_PARSE_RESPONSE,
                new ResultData(false, jobId));
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public ResultData getData() {
        return data;
    }

    public void setData(ResultData data) {
        this.data = data;
    }

    public static class ResultData {
        @JsonProperty("success")
        private boolean success;

        @JsonProperty("jobId")
        private String jobId;

        public ResultData() {
        }

        public ResultData(boolean success, String jobId) {
            this.success = success;
            this.jobId = jobId;
        }

        public boolean isSuccess() {
            return success;
        }

        public void setSuccess(boolean success) {
            this.success = success;
        }

        public String getJobId() {
            return jobId;
        }

        public void setJobId(String jobId) {
            this.jobId = jobId;
        }

        @Override
        public String toString() {
            return "ResultData{" +
                    "success=" + success +
                    ", jobId='" + jobId + '\'' +
                    '}';
        }
    }

    @Override
    public String toString() {
        return "WorkerTaskResult{" +
                "type='" + type + '\'' +
                ", data=" + data +
                '}';
    }
}
