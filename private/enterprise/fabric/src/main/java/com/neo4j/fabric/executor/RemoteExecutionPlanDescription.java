/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.fabric.executor;

import java.util.List;
import java.util.Map;
import java.util.Set;

import org.neo4j.graphdb.ExecutionPlanDescription;

public class RemoteExecutionPlanDescription implements ExecutionPlanDescription
{
    private final ExecutionPlanDescription remotePlanDescription;
    private final Location.Remote location;
    private final ProfilerStatistics profilerStatistics;

    RemoteExecutionPlanDescription( ExecutionPlanDescription remotePlanDescription, Location.Remote location )
    {
        this.remotePlanDescription = remotePlanDescription;
        this.location = location;

        if ( remotePlanDescription.hasProfilerStatistics() )
        {
            profilerStatistics = new ProfilerStatisticsWrapper( remotePlanDescription.getProfilerStatistics() );
        }
        else
        {
            profilerStatistics = null;
        }
    }

    @Override
    public String getName()
    {
        return "RemoteExecution@graph(" + location.getGraphId() + ")";
    }

    @Override
    public List<ExecutionPlanDescription> getChildren()
    {
        return List.of(remotePlanDescription);
    }

    @Override
    public Map<String,Object> getArguments()
    {
        return remotePlanDescription.getArguments();
    }

    @Override
    public Set<String> getIdentifiers()
    {
        return remotePlanDescription.getIdentifiers();
    }

    @Override
    public boolean hasProfilerStatistics()
    {
        return remotePlanDescription.hasProfilerStatistics();
    }

    @Override
    public ProfilerStatistics getProfilerStatistics()
    {
        return profilerStatistics;
    }

    private static class ProfilerStatisticsWrapper implements ProfilerStatistics
    {

        private final ProfilerStatistics childStatistics;

        ProfilerStatisticsWrapper( ProfilerStatistics childStatistics )
        {
            this.childStatistics = childStatistics;
        }

        @Override
        public boolean hasRows()
        {
            return childStatistics.hasRows();
        }

        @Override
        public long getRows()
        {
            return childStatistics.getRows();
        }

        @Override
        public boolean hasDbHits()
        {
            return childStatistics.hasDbHits();
        }

        @Override
        public long getDbHits()
        {
            return 0;
        }

        @Override
        public boolean hasPageCacheStats()
        {
            return childStatistics.hasPageCacheStats();
        }

        @Override
        public long getPageCacheHits()
        {
            return 0;
        }

        @Override
        public long getPageCacheMisses()
        {
            return 0;
        }

        @Override
        public boolean hasTime()
        {
            // TODO: we need to investigate, how time works
            return false;
        }

        @Override
        public long getTime()
        {
            // TODO: we need to investigate, how time works
            return 0;
        }
    }
}
