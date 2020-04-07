/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.fabric.driver;

import com.neo4j.fabric.stream.summary.EmptySummary;

import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

import org.neo4j.driver.summary.ResultSummary;
import org.neo4j.driver.summary.SummaryCounters;
import org.neo4j.graphdb.InputPosition;
import org.neo4j.graphdb.Notification;
import org.neo4j.graphdb.QueryStatistics;
import org.neo4j.graphdb.SeverityLevel;

public class ResultSummaryWrapper extends EmptySummary
{
    private final QueryStatistics statistics;
    private final List<Notification> notifications;

    public ResultSummaryWrapper( ResultSummary summary )
    {
        this.statistics = new SummaryCountersWrapper( summary.counters() );
        this.notifications = summary.notifications().stream().map( NotificationWrapper::new ).collect( Collectors.toList() );
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

    static class NotificationWrapper implements Notification
    {
        private final org.neo4j.driver.summary.Notification notification;

        NotificationWrapper( org.neo4j.driver.summary.Notification notification )
        {
            this.notification = notification;
        }

        @Override
        public String getCode()
        {
            return notification.code();
        }

        @Override
        public String getTitle()
        {
            return notification.title();
        }

        @Override
        public String getDescription()
        {
            return notification.description();
        }

        @Override
        public SeverityLevel getSeverity()
        {
            return SeverityLevel.valueOf( notification.severity() );
        }

        @Override
        public InputPosition getPosition()
        {
            var pos = notification.position();
            return new InputPosition( pos.offset(), pos.line(), pos.column() );
        }
    }

    public static class SummaryCountersWrapper implements QueryStatistics
    {
        private final SummaryCounters counters;

        SummaryCountersWrapper( SummaryCounters counters )
        {
            this.counters = counters;
        }

        @Override
        public int getNodesCreated()
        {
            return counters.nodesCreated();
        }

        @Override
        public int getNodesDeleted()
        {
            return counters.nodesDeleted();
        }

        @Override
        public int getRelationshipsCreated()
        {
            return counters.relationshipsCreated();
        }

        @Override
        public int getRelationshipsDeleted()
        {
            return counters.relationshipsDeleted();
        }

        @Override
        public int getPropertiesSet()
        {
            return counters.propertiesSet();
        }

        @Override
        public int getLabelsAdded()
        {
            return counters.labelsAdded();
        }

        @Override
        public int getLabelsRemoved()
        {
            return counters.labelsRemoved();
        }

        @Override
        public int getIndexesAdded()
        {
            return counters.indexesAdded();
        }

        @Override
        public int getIndexesRemoved()
        {
            return counters.indexesRemoved();
        }

        @Override
        public int getConstraintsAdded()
        {
            return counters.constraintsAdded();
        }

        @Override
        public int getConstraintsRemoved()
        {
            return counters.constraintsRemoved();
        }

        @Override
        public int getSystemUpdates()
        {
            return counters.systemUpdates();
        }

        @Override
        public boolean containsUpdates()
        {
            return counters.containsUpdates();
        }

        @Override
        public boolean containsSystemUpdates()
        {
            return counters.containsSystemUpdates();
        }
    }
}
