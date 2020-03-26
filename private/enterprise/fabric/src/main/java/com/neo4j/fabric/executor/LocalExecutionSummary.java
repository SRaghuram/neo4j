/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.fabric.executor;

import com.neo4j.fabric.stream.summary.Summary;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.neo4j.graphdb.ExecutionPlanDescription;
import org.neo4j.graphdb.Notification;
import org.neo4j.graphdb.QueryStatistics;
import org.neo4j.kernel.impl.query.QueryExecution;

public class LocalExecutionSummary implements Summary
{
    private final QueryExecution queryExecution;
    private final QueryStatistics queryStatistics;

    public LocalExecutionSummary( QueryExecution queryExecution, QueryStatistics queryStatistics )
    {
        this.queryExecution = queryExecution;
        this.queryStatistics = queryStatistics;
    }

    @Override
    public ExecutionPlanDescription executionPlanDescription()
    {
        return queryExecution.executionPlanDescription();
    }

    @Override
    public Collection<Notification> getNotifications()
    {
        List<Notification> notifications = new ArrayList<>();
        queryExecution.getNotifications().forEach( notifications::add );
        return notifications;
    }

    @Override
    public QueryStatistics getQueryStatistics()
    {
        return queryStatistics;
    }
}
