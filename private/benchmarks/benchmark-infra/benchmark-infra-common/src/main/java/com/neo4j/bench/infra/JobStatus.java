/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.infra;

import com.neo4j.bench.infra.aws.AWSBatchJobLogs;

import java.util.Objects;
import java.util.Optional;

import static java.lang.String.format;

public class JobStatus
{
    private final JobId jobId;
    private final String status;
    private final String logStreamName;

    public JobStatus( JobId jobId, String status, String logStreamName )
    {
        super();
        this.jobId = jobId;
        this.status = status;
        this.logStreamName = logStreamName;
    }

    public JobId getJobId()
    {
        return jobId;
    }

    public String getStatus()
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

    public String toStatusLine( String region )
    {
        return format( "job %s is %s, find log stream at <%s>", jobId, status, logStreamURL( region ).orElse( "UNAVAILABLE" ) );
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( jobId, status, logStreamName );
    }

    @Override
    public boolean equals( Object obj )
    {
        if ( this == obj )
        {
            return true;
        }
        if ( obj == null )
        {
            return false;
        }
        if ( getClass() != obj.getClass() )
        {
            return false;
        }
        JobStatus other = (JobStatus) obj;
        return Objects.equals( jobId, other.jobId )
               && Objects.equals( status, other.status )
               && Objects.equals( logStreamName, other.logStreamName );
    }

    @Override
    public String toString()
    {
        StringBuilder builder = new StringBuilder();
        builder.append( "JobStatus [jobId=" ).append( jobId )
               .append( ", status=" ).append( status )
               .append( ", logStreamName=" ).append( logStreamName )
               .append( "]" );
        return builder.toString();
    }
}
