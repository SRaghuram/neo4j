/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.model.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

import java.util.HashMap;
import java.util.Map;

public class Job
{
    public static Job fromMap( Map<String,Object> map )
    {
        return new Job( (String) map.get( "id" ),
                        (Long) map.get( "queued_at" ),
                        (Long) map.get( "run_at" ),
                        (Long) map.get( "done_at" ),
                        (String) map.get( "status_reason" ),
                        (String) map.get( "log_stream_name" ) );
    }

    private final String id;
    private final Long queuedAt;
    private final Long runAt;
    private final Long doneAt;
    private final String statusReason;
    private final String logStreamName;

    /**
     * @param id job identifier
     * @param queuedAt epoch seconds when job was queued
     * @param runAt epoch seconds when job was started
     * @param doneAt epoch seconds when job was done (either failed or succeeded)
     * @param statusReason job status reason which contains information why job failed
     * @param logStreamName AWS CloudWatch log stream name
     */
    @JsonCreator
    public Job(
            @JsonProperty( "id" ) String id,
            @JsonProperty( "queued_at" ) Long queuedAt,
            @JsonProperty( "run_at" ) Long runAt,
            @JsonProperty( "done_at" ) Long doneAt,
            @JsonProperty( "status_reason" ) String statusReason,
            @JsonProperty( "log_stream_name" ) String logStreamName )
    {
        this.id = id;
        this.queuedAt = queuedAt;
        this.runAt = runAt;
        this.doneAt = doneAt;
        this.statusReason = statusReason;
        this.logStreamName = logStreamName;
    }

    public String id()
    {
        return id;
    }

    public Long queuedAt()
    {
        return queuedAt;
    }

    public Long runAt()
    {
        return runAt;
    }

    public Long doneAt()
    {
        return doneAt;
    }

    public String statusReason()
    {
        return statusReason;
    }

    public String logStreamName()
    {
        return logStreamName;
    }

    @Override
    public boolean equals( Object o )
    {
        return EqualsBuilder.reflectionEquals( this, o );
    }

    @Override
    public int hashCode()
    {
        return HashCodeBuilder.reflectionHashCode( this );
    }

    public Map<String,Object> asMap()
    {
        Map<String,Object> map = new HashMap<>();
        map.put( "id", id );
        map.put( "queued_at", queuedAt );
        map.put( "run_at", runAt );
        map.put( "done_at", doneAt );
        map.put( "status_reason", statusReason );
        map.put( "log_stream_name", logStreamName );
        return map;
    }
}
