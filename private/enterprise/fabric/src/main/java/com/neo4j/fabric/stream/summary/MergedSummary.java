/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.fabric.stream.summary;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.neo4j.graphdb.ExecutionPlanDescription;
import org.neo4j.graphdb.Notification;
import org.neo4j.graphdb.QueryExecutionType;
import org.neo4j.graphdb.QueryStatistics;

public class MergedSummary implements Summary
{
    private final MergedQueryStatistics statistics;
    private final List<Notification> notifications;
    private final QueryExecutionType executionType;

    public MergedSummary(QueryExecutionType executionType)
    {
        this.executionType = executionType;
        this.statistics = new MergedQueryStatistics();
        this.notifications = new ArrayList<>();
    }

    public void add( QueryStatistics delta )
    {
        statistics.add( delta );
    }

    public void add( Collection<Notification> delta )
    {
        notifications.addAll( delta );
    }

    @Override
    public QueryExecutionType executionType()
    {
        return executionType;
    }

    @Override
    public ExecutionPlanDescription executionPlanDescription()
    {
        return null;
    }

    @Override
    public Collection<Notification> getNotifications()
    {
        return notifications;
    }

    @Override
    public QueryStatistics getQueryStatistics()
    {
        return statistics;
    }
}
