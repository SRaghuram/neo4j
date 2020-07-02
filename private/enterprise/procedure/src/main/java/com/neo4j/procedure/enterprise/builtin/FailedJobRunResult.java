/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.procedure.enterprise.builtin;

import java.time.ZoneId;

import org.neo4j.procedure.builtin.ProceduresTimeFormatHelper;
import org.neo4j.scheduler.FailedJobRun;

import static org.apache.commons.lang3.StringUtils.defaultString;

public class FailedJobRunResult
{
    public final String group;
    public final String database;
    public final String submitter;
    public final String description;
    public final String type;
    public final String submitted;
    public final String executionStart;
    public final String failureTime;
    public final String failureDescription;

    FailedJobRunResult( FailedJobRun failedJobRun, ZoneId zoneId )
    {
        group = failedJobRun.getGroup().groupName();
        submitter = SubjectFormatHelper.formatSubject( failedJobRun.getSubmitter() );
        database = defaultString( failedJobRun.getTargetDatabaseName() );
        description = failedJobRun.getDescription();
        type = failedJobRun.getJobType().name();
        submitted = ProceduresTimeFormatHelper.formatTime( failedJobRun.getSubmitted(), zoneId );
        executionStart = ProceduresTimeFormatHelper.formatTime( failedJobRun.getExecutionStart(), zoneId );
        failureTime = ProceduresTimeFormatHelper.formatTime( failedJobRun.getFailureTime(), zoneId );
        failureDescription = failedJobRun.getFailureDescription();
    }
}
