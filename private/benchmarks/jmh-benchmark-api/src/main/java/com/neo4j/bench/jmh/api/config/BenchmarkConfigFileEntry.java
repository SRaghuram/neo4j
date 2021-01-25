/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.jmh.api.config;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * Configures a single benchmark
 */
public class BenchmarkConfigFileEntry
{
    private final String name;
    private final boolean isEnabled;
    private final Map<String,Set<String>> values;

    BenchmarkConfigFileEntry( String name, boolean isEnabled )
    {
        this( name, isEnabled, new HashMap<>() );
    }

    BenchmarkConfigFileEntry( String name, boolean isEnabled, Map<String,Set<String>> values )
    {
        this.name = name;
        this.isEnabled = isEnabled;
        this.values = values;
    }

    public String name()
    {
        return name;
    }

    public boolean isEnabled()
    {
        return isEnabled;
    }

    public Map<String,Set<String>> values()
    {
        return values;
    }

    @Override
    public String toString()
    {
        return name + " , " + "isEnabled=" + isEnabled + ", values=" + values;
    }
}
