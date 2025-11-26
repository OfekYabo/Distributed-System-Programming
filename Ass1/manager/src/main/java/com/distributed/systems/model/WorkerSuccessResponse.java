package com.distributed.systems.model;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Success response from Worker to Manager
 * Example JSON:
 * {
 *   "type": "urlParseResponse",
 *   "data": {
 *     "fileUrl": "http://example.com/input.txt",
 *     "outputUrl": "s3://bucket/results/output.txt",
 *     "parsingMethod": "POS"
 *   }
 * }
 */
public class WorkerSuccessResponse {
    
    @JsonProperty("type")
    private String type;
    
    @JsonProperty("data")
    private ResponseData data;
    
    public WorkerSuccessResponse() {
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
    
    public static class ResponseData {
        @JsonProperty("fileUrl")
        private String fileUrl;
        
        @JsonProperty("outputUrl")
        private String outputUrl;
        
        @JsonProperty("parsingMethod")
        private String parsingMethod;
        
        public ResponseData() {
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
        
        @Override
        public String toString() {
            return "ResponseData{" +
                    "fileUrl='" + fileUrl + '\'' +
                    ", outputUrl='" + outputUrl + '\'' +
                    ", parsingMethod='" + parsingMethod + '\'' +
                    '}';
        }
    }
    
    @Override
    public String toString() {
        return "WorkerSuccessResponse{" +
                "type='" + type + '\'' +
                ", data=" + data +
                '}';
    }
}


