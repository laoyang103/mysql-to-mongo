package com.laoyang.mongo;  

public class Communication {  

    private long starttime;
    private long endtime;
    private long srcip;
    private long srcport;
    private String proto;
    private long srcsent;
    private long dstsent;

    public Communication() {  

    }  

    public Communication(Communication comm) {  
        this.starttime = comm.starttime + 10; 
        this.endtime   = comm.endtime + 10;
        this.srcip     = comm.srcip;
        this.srcport   = comm.srcport;
        this.proto     = comm.proto;
        this.srcsent   = comm.srcsent + 10;
        this.dstsent   = comm.dstsent + 10;
    }  

    public String toString() {  
        return "Communication [starttime=" + starttime + 
            ", endtime=" + endtime + 
            ", srcip=" + srcip + 
            ", srcport=" + srcport + 
            ", proto=" + proto + 
            ", srcsent=" + srcsent + 
            ", dstsent=" + dstsent + "]";  
    }  
}   
