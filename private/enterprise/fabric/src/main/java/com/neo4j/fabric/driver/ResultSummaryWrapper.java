/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.fabric.driver;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.neo4j.driver.summary.Plan;
import org.neo4j.driver.summary.ProfiledPlan;
import org.neo4j.driver.summary.ResultSummary;
import org.neo4j.driver.summary.SummaryCounters;
import org.neo4j.fabric.stream.summary.EmptyExecutionPlanDescription;
import org.neo4j.fabric.stream.summary.Summary;
import org.neo4j.graphdb.ExecutionPlanDescription;
import org.neo4j.graphdb.InputPosition;
import org.neo4j.graphdb.Notification;
import org.neo4j.graphdb.QueryStatistics;
import org.neo4j.graphdb.SeverityLevel;

public class ResultSummaryWrapper implements Summary
{
    private final QueryStatistics statistics;
    private final List<Notification> notifications;
    private final ResultSummary summary;

    public ResultSummaryWrapper( ResultSummary summary )
    {
        this.statistics = new SummaryCountersWrapper( summary.counters() );
        this.notifications = summary.notifications().stream()
                .map( NotificationWrapper::new )
                .collect( Collectors.toList() );
        this.summary = summary;
    }

    @Override
    public ExecutionPlanDescription executionPlanDescription()
    {
        if ( summary.hasProfile() )
        {
            return new ProfiledPlanDescriptionWrapper( summary.profile() );
        }

        if ( summary.hasPlan() )
        {
            return new PlanDescriptionWrapper( summary.plan() );
        }

        return new EmptyExecutionPlanDescription();
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

    private static class PlanDescriptionWrapper implements ExecutionPlanDescription
    {

        private final Plan driverPlan;

        PlanDescriptionWrapper( Plan driverPlan )
        {
            this.driverPlan = driverPlan;
        }

        @Override
        public String getName()
        {
            return driverPlan.operatorType();
        }

        @Override
        public List<ExecutionPlanDescription> getChildren()
        {
            return driverPlan.children().stream()
                    .map( PlanDescriptionWrapper::new )
                    .collect( Collectors.toList());
        }

        @Override
        public Map<String,Object> getArguments()
        {
            var recordConverter = new RecordConverter();

            Map<String,Object> convertedArguments = new HashMap<>();
            driverPlan.arguments().forEach( ( key, value ) -> convertedArguments.put( key, recordConverter.convertValue( value ) ) );

            return convertedArguments;
        }

        @Override
        public Set<String> getIdentifiers()
        {
            return new HashSet<>( driverPlan.identifiers() );
        }

        @Override
        public boolean hasProfilerStatistics()
        {
            return false;
        }

        @Override
        public ProfilerStatistics getProfilerStatistics()
        {
            return null;
        }
    }

    private static class ProfiledPlanDescriptionWrapper extends PlanDescriptionWrapper
    {

        private final ProfiledPlan profiledDriverPlan;

        ProfiledPlanDescriptionWrapper( ProfiledPlan profiledDriverPlan )
        {
            super( profiledDriverPlan );
            this.profiledDriverPlan = profiledDriverPlan;
        }

        @Override
        public boolean hasProfilerStatistics()
        {
            return true;
        }

        @Override
        public ProfilerStatistics getProfilerStatistics()
        {
            return new ProfilerStatisticsImpl( profiledDriverPlan );
        }

        @Override
        public List<ExecutionPlanDescription> getChildren()
        {
            return profiledDriverPlan.children().stream()
                    .map( ProfiledPlanDescriptionWrapper::new )
                    .collect( Collectors.toList());
        }
    }

    private static class ProfilerStatisticsImpl implements ExecutionPlanDescription.ProfilerStatistics
    {
        private final ProfiledPlan profiledDriverPlan;

        ProfilerStatisticsImpl( ProfiledPlan profiledDriverPlan )
        {
            this.profiledDriverPlan = profiledDriverPlan;
        }

        @Override
        public boolean hasRows()
        {
            return profiledDriverPlan.records() > 0;
        }

        @Override
        public long getRows()
        {
            return profiledDriverPlan.records();
        }

        @Override
        public boolean hasDbHits()
        {
            return profiledDriverPlan.dbHits() > 0;
        }

        @Override
        public long getDbHits()
        {
            return profiledDriverPlan.dbHits();
        }

        @Override
        public boolean hasPageCacheStats()
        {
            return profiledDriverPlan.hasPageCacheStats();
        }

        @Override
        public long getPageCacheHits()
        {
            return profiledDriverPlan.pageCacheHits();
        }

        @Override
        public long getPageCacheMisses()
        {
            return profiledDriverPlan.pageCacheMisses();
        }

        @Override
        public boolean hasTime()
        {
            return profiledDriverPlan.time() > 0;
        }

        @Override
        public long getTime()
        {
            return profiledDriverPlan.time();
        }
    }

    private static class NotificationWrapper implements Notification
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

    private static class SummaryCountersWrapper implements QueryStatistics
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
