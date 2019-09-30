/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.infra;

import java.util.Objects;

public class JobStatus
{
    private final String jobId;
    private final String status;

    public JobStatus( String jobId, String status )
    {
        super();
        this.jobId = jobId;
        this.status = status;
    }

    public String getJobId()
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

    @Override
    public int hashCode()
    {
        return Objects.hash( jobId, status );
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
        return Objects.equals( jobId, other.jobId ) && Objects.equals( status, other.status );
    }

    @Override
    public String toString()
    {
        StringBuilder builder = new StringBuilder();
        builder.append( "JobStatus [jobId=" ).append( jobId ).append( ", status=" ).append( status ).append( "]" );
        return builder.toString();
    }

}
