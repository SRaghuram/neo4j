/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.procedure.enterprise.builtin;

import java.time.ZoneId;

import org.neo4j.procedure.builtin.ProceduresTimeFormatHelper;
import org.neo4j.scheduler.MonitoredJobInfo;

public class JobStatusResult
{
    public final String group;
    public final String submitted;
    public final String database;
    public final String submitter;
    public final String description;
    public final String type;
    public final String scheduledAt;
    public final String period;
    public final String state;

    JobStatusResult( MonitoredJobInfo job, ZoneId zoneId )
    {
        group = job.getGroup().groupName();
        submitted = ProceduresTimeFormatHelper.formatTime( job.getSubmitted().toEpochMilli(), zoneId );
        submitter = job.getSubmitter() != null ? job.getSubmitter() : "";
        database = job.getTargetDatabaseName() != null ? job.getTargetDatabaseName() : "";
        description = job.getDescription();
        type = job.getType().name();
        state = job.getState().name();
        scheduledAt = job.getNextDeadline() != null ? ProceduresTimeFormatHelper.formatTime( job.getNextDeadline().toEpochMilli(), zoneId ) : "";
        period = job.getPeriod() != null ? ProceduresTimeFormatHelper.formatInterval( job.getPeriod().toMillis() ) : "";
    }
}
