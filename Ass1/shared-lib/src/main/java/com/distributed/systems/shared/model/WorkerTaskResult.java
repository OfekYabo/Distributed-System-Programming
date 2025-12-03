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

    public static WorkerTaskResult createSuccess(String fileUrl, String outputUrl, String parsingMethod) {
        return new WorkerTaskResult(TYPE_URL_PARSE_RESPONSE,
                new ResultData(fileUrl, outputUrl, parsingMethod, true, null));
    }

    public static WorkerTaskResult createError(String fileUrl, String parsingMethod, String errorMessage) {
        return new WorkerTaskResult(TYPE_URL_PARSE_RESPONSE,
                new ResultData(fileUrl, null, parsingMethod, false, errorMessage));
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
        @JsonProperty("fileUrl")
        private String fileUrl;

        @JsonProperty("outputUrl")
        private String outputUrl;

        @JsonProperty("parsingMethod")
        private String parsingMethod;

        @JsonProperty("success")
        private boolean success;

        @JsonProperty("errorMessage")
        private String errorMessage;

        public ResultData() {
        }

        public ResultData(String fileUrl, String outputUrl, String parsingMethod, boolean success,
                String errorMessage) {
            this.fileUrl = fileUrl;
            this.outputUrl = outputUrl;
            this.parsingMethod = parsingMethod;
            this.success = success;
            this.errorMessage = errorMessage;
        }

        public String getFileUrl() {
            return fileUrl;
        }

        public void setFileUrl(String fileUrl) {
            this.fileUrl = fileUrl;
        }

        public String getOutputUrl() {
            return outputUrl;
        }

        public void setOutputUrl(String outputUrl) {
            this.outputUrl = outputUrl;
        }

        public String getParsingMethod() {
            return parsingMethod;
        }

        public void setParsingMethod(String parsingMethod) {
            this.parsingMethod = parsingMethod;
        }

        public boolean isSuccess() {
            return success;
        }

        public void setSuccess(boolean success) {
            this.success = success;
        }

        public String getErrorMessage() {
            return errorMessage;
        }

        public void setErrorMessage(String errorMessage) {
            this.errorMessage = errorMessage;
        }

        @Override
        public String toString() {
            return "ResultData{" +
                    "fileUrl='" + fileUrl + '\'' +
                    ", outputUrl='" + outputUrl + '\'' +
                    ", parsingMethod='" + parsingMethod + '\'' +
                    ", success=" + success +
                    ", errorMessage='" + errorMessage + '\'' +
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
