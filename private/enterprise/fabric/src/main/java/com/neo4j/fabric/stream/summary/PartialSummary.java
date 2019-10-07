/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.fabric.stream.summary;

import java.util.Collection;
import java.util.List;

import org.neo4j.graphdb.Notification;
import org.neo4j.graphdb.QueryExecutionType;
import org.neo4j.graphdb.QueryStatistics;

public class PartialSummary extends EmptySummary
{
    private final QueryStatistics queryStatistics;
    private final QueryExecutionType executionType;
    private final List<Notification> notifications;

    public PartialSummary( QueryStatistics queryStatistics, QueryExecutionType executionType, List<Notification> notifications )
    {
        this.queryStatistics = queryStatistics;
        this.executionType = executionType;
        this.notifications = notifications;
    }

    @Override
    public QueryStatistics getQueryStatistics()
    {
        return queryStatistics;
    }

    @Override
    public QueryExecutionType executionType()
    {
        return executionType;
    }

    @Override
    public Collection<Notification> getNotifications()
    {
        return notifications;
    }
}
