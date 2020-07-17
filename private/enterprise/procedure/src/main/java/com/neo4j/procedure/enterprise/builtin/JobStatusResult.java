/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.procedure.enterprise.builtin;

import java.time.ZoneId;

import org.neo4j.procedure.builtin.ProceduresTimeFormatHelper;
import org.neo4j.scheduler.MonitoredJobInfo;

import static org.apache.commons.lang3.StringUtils.EMPTY;
import static org.apache.commons.lang3.StringUtils.defaultString;
import static org.neo4j.procedure.builtin.ProceduresTimeFormatHelper.formatTime;

public class JobStatusResult
{
    private static final String ID_PREFIX = "job-";

    public final String jobId;
    public final String group;
    public final String submitted;
    public final String database;
    public final String submitter;
    public final String description;
    public final String type;
    public final String scheduledAt;
    public final String period;
    public final String state;
    public final String currentStateDescription;

    JobStatusResult( MonitoredJobInfo job, ZoneId zoneId )
    {
        jobId = serialiseJobId( job.getId() );
        group = job.getGroup().groupName();
        submitted = formatTime( job.getSubmitted(), zoneId );
        submitter = job.getSubmitter().describe();
        database = defaultString( job.getTargetDatabaseName() );
        description = job.getDescription();
        type = job.getType().name();
        state = job.getState().name();
        scheduledAt = job.getNextDeadline() != null ? formatTime( job.getNextDeadline(), zoneId ) : EMPTY;
        period = job.getPeriod() != null ? ProceduresTimeFormatHelper.formatInterval( job.getPeriod().toMillis() ) : EMPTY;
        currentStateDescription = job.getCurrentStateDescription() != null ? job.getCurrentStateDescription() : EMPTY;
    }

    static String serialiseJobId( long jobId )
    {
        return ID_PREFIX + jobId;
    }
}
