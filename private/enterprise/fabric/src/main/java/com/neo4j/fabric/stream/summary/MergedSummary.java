/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.fabric.stream.summary;

import reactor.core.publisher.Mono;

import java.util.Collection;
import java.util.Set;

import org.neo4j.graphdb.ExecutionPlanDescription;
import org.neo4j.graphdb.Notification;
import org.neo4j.graphdb.QueryStatistics;

public class MergedSummary implements Summary
{
    private final MergedQueryStatistics statistics;
    private final Set<Notification> notifications;
    private Mono<ExecutionPlanDescription> executionPlanDescription;

    public MergedSummary( Mono<ExecutionPlanDescription> executionPlanDescription, MergedQueryStatistics statistics, Set<Notification> notifications )
    {
        this.executionPlanDescription = executionPlanDescription;
        this.statistics = statistics;
        this.notifications = notifications;
    }

    @Override
    public ExecutionPlanDescription executionPlanDescription()
    {
        return executionPlanDescription.cache().block();
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
