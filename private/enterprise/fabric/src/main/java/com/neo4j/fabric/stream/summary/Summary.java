/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.fabric.stream.summary;

import java.util.Collection;

import org.neo4j.graphdb.ExecutionPlanDescription;
import org.neo4j.graphdb.Notification;
import org.neo4j.graphdb.QueryStatistics;

public interface Summary
{
    /**
     * @return The plan description of the query.
     */
    ExecutionPlanDescription executionPlanDescription();

    /**
     * @return all notifications and warnings of the query.
     */
    Collection<Notification> getNotifications();

    QueryStatistics getQueryStatistics();
}
