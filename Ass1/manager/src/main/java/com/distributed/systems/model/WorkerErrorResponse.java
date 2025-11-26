package com.distributed.systems.model;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Error response from Worker to Manager
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
public class WorkerErrorResponse {
    
    @JsonProperty("type")
    private String type;
    
    @JsonProperty("data")
    private ErrorData data;
    
    public WorkerErrorResponse() {
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
        return "WorkerErrorResponse{" +
                "type='" + type + '\'' +
                ", data=" + data +
                '}';
    }
}


