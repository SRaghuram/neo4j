/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.fabric.stream.summary;

import com.neo4j.fabric.planning.FabricQuery;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.neo4j.graphdb.ExecutionPlanDescription;

import static scala.collection.JavaConverters.seqAsJavaList;
import static scala.collection.JavaConverters.setAsJavaSet;

public class FabricExecutionPlanDescription implements ExecutionPlanDescription
{

    private final FabricQuery query;

    public FabricExecutionPlanDescription( FabricQuery query )
    {
        this.query = query;
    }

    @Override
    public String getName()
    {
        return query.getClass().getSimpleName();
    }

    @Override
    public List<ExecutionPlanDescription> getChildren()
    {
        return seqAsJavaList( query.children() ).stream().map( FabricExecutionPlanDescription::new ).collect( Collectors.toList() );
    }

    @Override
    public Map<String,Object> getArguments()
    {
        var arguments = new HashMap<String,Object>();
        if ( query instanceof FabricQuery.RemoteQuery )
        {
            FabricQuery.RemoteQuery rq = (FabricQuery.RemoteQuery) query;
            arguments.put( "query", rq.queryString() );
        }

        return arguments;
    }

    @Override
    public Set<String> getIdentifiers()
    {
        return setAsJavaSet( query.columns().output().toSet() );
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
