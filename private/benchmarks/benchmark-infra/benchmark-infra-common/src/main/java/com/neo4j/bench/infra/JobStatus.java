/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.infra;

import com.neo4j.bench.infra.aws.AWSBatchJobLogs;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;

import java.util.Optional;

import static java.lang.String.format;

public class JobStatus
{
    private final JobId jobId;
    private final String status;
    private final String logStreamName;
    private final String statusReason;

    public JobStatus( JobId jobId, String status, String logStreamName, String statusReason )
    {
        super();
        this.jobId = jobId;
        this.status = status;
        this.logStreamName = logStreamName;
        this.statusReason = statusReason;
    }

    public JobId jobId()
    {
        return jobId;
    }

    public String status()
    {
        return status;
    }

    public boolean isDone()
    {
        return "SUCCEEDED".equals( status ) || "FAILED".equals( status );
    }

    public boolean isWaiting()
    {
        return !isDone();
    }

    public boolean isFailed()
    {
        return "FAILED".equals( status );
    }

    public Optional<String> logStreamURL( String region )
    {
        return Optional.ofNullable( logStreamName ).map( streamName -> AWSBatchJobLogs.getLogStreamURL( region, streamName ) );
    }

    public Optional<String> statusReason()
    {
        return Optional.ofNullable( statusReason );
    }

    public String logStreamName()
    {
        return logStreamName;
    }

    public String toStatusLine( String region )
    {
        return format( "job %s is %s (%s), find log stream at <%s>",
                       jobId,
                       status,
                       statusReason().orElse( "UNKNOWN" ),
                       logStreamURL( region ).orElse( "UNAVAILABLE" ) );
    }

    @Override
    public int hashCode()
    {
        return HashCodeBuilder.reflectionHashCode( this );
    }

    @Override
    public boolean equals( Object obj )
    {
        return EqualsBuilder.reflectionEquals( this, obj );
    }

    @Override
    public String toString()
    {
        return ToStringBuilder.reflectionToString( this );
    }
}
