package com.laoyang.mongo;  

public class DocKey {  
    private long start;

    public DocKey() {  

    }  

    public long getKey() {
        return this.start;
    }

    public String toString() {  
        return "DocKey [start=" + start; 
    }
}   
