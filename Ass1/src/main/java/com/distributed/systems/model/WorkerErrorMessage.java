package com.distributed.systems.model;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Error message sent from Worker to Manager when processing fails
 * Example JSON:
 * {
 *   "type": "urlParseError",
 *   "data": {
 *     "fileUrl": "http://example.com/input.txt",
 *     "parsingMethod": "POS",
 *     "error": "Failed to download file: Connection timeout"
 *   }
 * }
 */
public class WorkerErrorMessage {
    
    @JsonProperty("type")
    private String type;
    
    @JsonProperty("data")
    private ErrorData data;
    
    public WorkerErrorMessage() {
    }
    
    public WorkerErrorMessage(String type, ErrorData data) {
        this.type = type;
        this.data = data;
    }
    
    public String getType() {
        return type;
    }
    
    public void setType(String type) {
        this.type = type;
    }
    
    public ErrorData getData() {
        return data;
    }
    
    public void setData(ErrorData data) {
        this.data = data;
    }
    
    public static class ErrorData {
        @JsonProperty("fileUrl")
        private String fileUrl;
        
        @JsonProperty("parsingMethod")
        private String parsingMethod;
        
        @JsonProperty("error")
        private String error;
        
        public ErrorData() {
        }
        
        public ErrorData(String fileUrl, String parsingMethod, String error) {
            this.fileUrl = fileUrl;
            this.parsingMethod = parsingMethod;
            this.error = error;
        }
        
        public String getFileUrl() {
            return fileUrl;
        }
        
        public void setFileUrl(String fileUrl) {
            this.fileUrl = fileUrl;
        }
        
        public String getParsingMethod() {
            return parsingMethod;
        }
        
        public void setParsingMethod(String parsingMethod) {
            this.parsingMethod = parsingMethod;
        }
        
        public String getError() {
            return error;
        }
        
        public void setError(String error) {
            this.error = error;
        }
        
        @Override
        public String toString() {
            return "ErrorData{" +
                    "fileUrl='" + fileUrl + '\'' +
                    ", parsingMethod='" + parsingMethod + '\'' +
                    ", error='" + error + '\'' +
                    '}';
        }
    }
    
    @Override
    public String toString() {
        return "WorkerErrorMessage{" +
                "type='" + type + '\'' +
                ", data=" + data +
                '}';
    }
}

