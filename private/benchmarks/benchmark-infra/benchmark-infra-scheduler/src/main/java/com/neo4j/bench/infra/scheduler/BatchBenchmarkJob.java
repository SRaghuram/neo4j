/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.infra.scheduler;

import com.neo4j.bench.infra.JobStatus;
import com.neo4j.bench.model.model.Job;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Clock;
import java.time.ZonedDateTime;
import java.time.chrono.ChronoZonedDateTime;

import static java.util.Objects.requireNonNull;
import static java.util.Optional.ofNullable;

class BatchBenchmarkJob
{
    private static final Logger LOG = LoggerFactory.getLogger( BatchBenchmarkJob.class );

    private final String jobName;
    private final String testRunId;
    private final JobStatus lastJobStatus;
    private final ZonedDateTime queuedAt;
    private final ZonedDateTime runAt;
    private final ZonedDateTime doneAt;

    private BatchBenchmarkJob( String jobName,
                               String testRunId,
                               JobStatus lastJobStatus,
                               ZonedDateTime queuedAt,
                               ZonedDateTime runAt,
                               ZonedDateTime doneAt )
    {
        this.jobName = jobName;
        this.testRunId = testRunId;
        this.lastJobStatus = lastJobStatus;
        this.queuedAt = requireNonNull( queuedAt );
        this.runAt = runAt;
        this.doneAt = doneAt;
    }

    static BatchBenchmarkJob newJob( String jobName,
                                     String testRunId,
                                     JobStatus jobStatus,
                                     Clock clock )
    {
        return new BatchBenchmarkJob( jobName,
                                      testRunId,
                                      jobStatus,
                                      ZonedDateTime.now( clock ),
                                      null,
                                      null );
    }

    Job toJob()
    {
        return new Job( lastJobStatus().jobId().id(),
                        queuedAt().toEpochSecond(),
                        ofNullable( runAt() ).map( ChronoZonedDateTime::toEpochSecond ).orElse( null ),
                        ofNullable( doneAt() ).map( ChronoZonedDateTime::toEpochSecond ).orElse( null ),
                        lastJobStatus().logStreamName(),
                        lastJobStatus().statusReason().orElse( null ) );
    }

    JobStatus lastJobStatus()
    {
        return lastJobStatus;
    }

    ZonedDateTime queuedAt()
    {
        return queuedAt;
    }

    ZonedDateTime runAt()
    {
        return runAt;
    }

    ZonedDateTime doneAt()
    {
        return doneAt;
    }

    String jobName()
    {
        return jobName;
    }

    String testRunId()
    {
        return testRunId;
    }

    BatchBenchmarkJob copyWith( JobStatus jobStatus, Clock clock )
    {

        LOG.info( "updating batch job {} with status {}", this, jobStatus );

        ZonedDateTime runningTimestamp = runAt;
        ZonedDateTime doneTimestamp = doneAt;

        if ( "RUNNING".equals( jobStatus.status() ) && runningTimestamp == null )
        {
            runningTimestamp = ZonedDateTime.now( clock );
        }
        else if ( ("FAILED".equals( jobStatus.status() ) || "SUCCEEDED".equals( jobStatus.status() ))
                  && doneTimestamp == null )
        {
            doneTimestamp = ZonedDateTime.now( clock );
        }

        return new BatchBenchmarkJob( this.jobName,
                                      this.testRunId,
                                      jobStatus,
                                      this.queuedAt,
                                      runningTimestamp,
                                      doneTimestamp );
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

