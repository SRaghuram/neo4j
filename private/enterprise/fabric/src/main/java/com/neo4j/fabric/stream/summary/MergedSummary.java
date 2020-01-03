/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.fabric.stream.summary;

import com.neo4j.fabric.planning.FabricPlan;
import com.neo4j.fabric.executor.EffectiveQueryType;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.neo4j.bolt.runtime.AccessMode;
import org.neo4j.graphdb.ExecutionPlanDescription;
import org.neo4j.graphdb.Notification;
import org.neo4j.graphdb.QueryExecutionType;
import org.neo4j.graphdb.QueryStatistics;

public class MergedSummary implements Summary
{
    private final MergedQueryStatistics statistics;
    private final List<Notification> notifications;
    private final QueryExecutionType executionType;
    private final FabricExecutionPlanDescription executionPlanDescription;

    public MergedSummary( FabricPlan plan, AccessMode accessMode )
    {
        this.executionType = queryExecutionType( plan, accessMode );
        this.statistics = new MergedQueryStatistics();
        this.notifications = new ArrayList<>();
        if ( plan.executionType() == FabricPlan.EXPLAIN() )
        {
            this.executionPlanDescription = new FabricExecutionPlanDescription( plan.query() );
        }
        else
        {
            this.executionPlanDescription = null;
        }
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
        return executionPlanDescription;
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

    private QueryExecutionType queryExecutionType( FabricPlan plan, AccessMode accessMode )
    {
        if ( plan.executionType() == FabricPlan.EXECUTE() )
        {
            return QueryExecutionType.query( queryType( plan, accessMode ) );
        }
        else if ( plan.executionType() == FabricPlan.EXPLAIN() )
        {
            return QueryExecutionType.explained( queryType( plan, accessMode ) );
        }
        else if ( plan.executionType() == FabricPlan.PROFILE() )
        {
            return QueryExecutionType.profiled( queryType( plan, accessMode ) );
        }
        else
        {
            throw unexpected( "execution type", plan.executionType().toString() );
        }
    }

    private QueryExecutionType.QueryType queryType( FabricPlan plan, AccessMode accessMode )
    {
       return EffectiveQueryType.effectiveQueryType( accessMode, plan.queryType());
    }

    private IllegalArgumentException unexpected( String type, String got )
    {
        return new IllegalArgumentException( "Unexpected " + type + ": " + got );
    }
}
