package com.dremio.metrics;

/**
 * Objects from this class will be used to track the statistics through the project
 */
public class ReaderStat {

    private final String name;
    private final long startTime;
    private long endTime;

    public ReaderStat(String name, long startTime, long endTime) {
        this.name = name;
        this.startTime = startTime;
        this.endTime = endTime;
    }

    public ReaderStat(String name, long startTime) {
        this.name = name;
        this.startTime = startTime;
    }

    public void setEndTime(long endTime) {
        this.endTime = endTime;
    }

    public String getName() {
        return name;
    }

    public long getStartTime() {
        return startTime;
    }

    public long getEndTime() {
        return endTime;
    }
}
