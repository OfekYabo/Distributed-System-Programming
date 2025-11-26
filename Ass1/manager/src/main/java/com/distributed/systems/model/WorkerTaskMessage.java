package com.distributed.systems.model;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Message sent from Manager to Worker
 * Example JSON:
 * {
 *   "type": "urlParseRequest",
 *   "data": {
 *     "parsingMethod": "POS",
 *     "url": "http://example.com/text.txt"
 *   }
 * }
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
    
    public static WorkerTaskMessage create(String parsingMethod, String url) {
        return new WorkerTaskMessage(TYPE_URL_PARSE_REQUEST, new TaskData(parsingMethod, url));
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
        
        public TaskData() {
        }
        
        public TaskData(String parsingMethod, String url) {
            this.parsingMethod = parsingMethod;
            this.url = url;
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


