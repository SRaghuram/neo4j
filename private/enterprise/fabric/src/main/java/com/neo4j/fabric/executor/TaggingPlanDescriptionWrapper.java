/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.fabric.executor;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.neo4j.graphdb.ExecutionPlanDescription;

public class TaggingPlanDescriptionWrapper implements ExecutionPlanDescription
{
    private final ExecutionPlanDescription innerPlanDescription;
    private final String graphName;

    public TaggingPlanDescriptionWrapper( ExecutionPlanDescription innerPlanDescription, String graphName )
    {
        this.innerPlanDescription = innerPlanDescription;
        this.graphName = graphName;
    }

    @Override
    public String getName()
    {
        return innerPlanDescription.getName() + "@" + graphName;
    }

    @Override
    public List<ExecutionPlanDescription> getChildren()
    {
        return innerPlanDescription.getChildren().stream()
                .map( child -> new TaggingPlanDescriptionWrapper( child, graphName ) )
                .collect( Collectors.toList());
    }

    @Override
    public Map<String,Object> getArguments()
    {
        return innerPlanDescription.getArguments();
    }

    @Override
    public Set<String> getIdentifiers()
    {
        return innerPlanDescription.getIdentifiers();
    }

    @Override
    public boolean hasProfilerStatistics()
    {
        return innerPlanDescription.hasProfilerStatistics();
    }

    @Override
    public ProfilerStatistics getProfilerStatistics()
    {
        return innerPlanDescription.getProfilerStatistics();
    }
}
