/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.fabric.stream.summary;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.neo4j.graphdb.ExecutionPlanDescription;

/**
 * An empty plan, just to make Bolt server not throwing NPE.
 */
public class EmptyExecutionPlanDescription implements ExecutionPlanDescription
{
    @Override
    public String getName()
    {
        return "";
    }

    @Override
    public List<ExecutionPlanDescription> getChildren()
    {
        return Collections.emptyList();
    }

    @Override
    public Map<String,Object> getArguments()
    {
        return Collections.emptyMap();
    }

    @Override
    public Set<String> getIdentifiers()
    {
        return Collections.emptySet();
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
