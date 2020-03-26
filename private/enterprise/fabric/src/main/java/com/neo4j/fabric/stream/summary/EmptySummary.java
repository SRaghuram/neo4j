/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.fabric.stream.summary;

import java.util.Collection;
import java.util.Collections;

import org.neo4j.graphdb.ExecutionPlanDescription;
import org.neo4j.graphdb.Notification;
import org.neo4j.graphdb.QueryStatistics;

import static org.neo4j.graphdb.QueryStatistics.EMPTY;

public class EmptySummary implements Summary
{
    @Override
    public ExecutionPlanDescription executionPlanDescription()
    {
        return new EmptyExecutionPlanDescription();
    }

    @Override
    public Collection<Notification> getNotifications()
    {
        return Collections.emptyList();
    }

    @Override
    public QueryStatistics getQueryStatistics()
    {
        return EMPTY;
    }
}
