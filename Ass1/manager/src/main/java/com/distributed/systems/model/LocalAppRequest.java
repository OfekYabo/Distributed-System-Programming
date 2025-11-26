package com.distributed.systems.model;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Message received from Local Application to Manager
 * 
 * New Task Example:
 * {
 *   "type": "newTask",
 *   "data": {
 *     "inputFileS3Key": "inputs/client123/input.txt",
 *     "n": 5
 *   }
 * }
 * 
 * Terminate Example:
 * {
 *   "type": "terminate"
 * }
 */
public class LocalAppRequest {
    
    public static final String TYPE_NEW_TASK = "newTask";
    public static final String TYPE_TERMINATE = "terminate";
    
    @JsonProperty("type")
    private String type;
    
    @JsonProperty("data")
    private RequestData data;
    
    public LocalAppRequest() {
    }
    
    public LocalAppRequest(String type, RequestData data) {
        this.type = type;
        this.data = data;
    }
    
    public String getType() {
        return type;
    }
    
    public void setType(String type) {
        this.type = type;
    }
    
    public RequestData getData() {
        return data;
    }
    
    public void setData(RequestData data) {
        this.data = data;
    }
    
    public boolean isNewTask() {
        return TYPE_NEW_TASK.equals(type);
    }
    
    public boolean isTerminate() {
        return TYPE_TERMINATE.equals(type);
    }
    
    public static class RequestData {
        @JsonProperty("inputFileS3Key")
        private String inputFileS3Key;
        
        @JsonProperty("n")
        private int n; // files per worker ratio
        
        public RequestData() {
        }
        
        public RequestData(String inputFileS3Key, int n) {
            this.inputFileS3Key = inputFileS3Key;
            this.n = n;
        }
        
        public String getInputFileS3Key() {
            return inputFileS3Key;
        }
        
        public void setInputFileS3Key(String inputFileS3Key) {
            this.inputFileS3Key = inputFileS3Key;
        }
        
        public int getN() {
            return n;
        }
        
        public void setN(int n) {
            this.n = n;
        }
        
        @Override
        public String toString() {
            return "RequestData{" +
                    "inputFileS3Key='" + inputFileS3Key + '\'' +
                    ", n=" + n +
                    '}';
        }
    }
    
    @Override
    public String toString() {
        return "LocalAppRequest{" +
                "type='" + type + '\'' +
                ", data=" + data +
                '}';
    }
}


