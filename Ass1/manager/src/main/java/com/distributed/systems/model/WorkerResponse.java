package com.distributed.systems.model;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Base class for Worker responses (both success and error)
 * Used for initial parsing to determine type
 */
public class WorkerResponse {
    
    public static final String TYPE_SUCCESS = "urlParseResponse";
    public static final String TYPE_ERROR = "urlParseError";
    
    @JsonProperty("type")
    private String type;
    
    public WorkerResponse() {
    }
    
    public String getType() {
        return type;
    }
    
    public void setType(String type) {
        this.type = type;
    }
    
    public boolean isSuccess() {
        return TYPE_SUCCESS.equals(type);
    }
    
    public boolean isError() {
        return TYPE_ERROR.equals(type);
    }
}


