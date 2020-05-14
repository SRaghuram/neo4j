/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.model.model;

import com.google.common.collect.Maps;

import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;

import static java.util.Collections.emptyMap;
import static java.util.Collections.unmodifiableMap;
import static java.util.Objects.requireNonNull;

public class BenchmarkConfig
{
    private final Map<String,String> config;

    /**
     * WARNING: Never call this explicitly.
     * No-params constructor is only used for JSON (de)serialization.
     */
    public BenchmarkConfig()
    {
        this( emptyMap() );
    }

    public BenchmarkConfig( Map<String,String> config )
    {
        this.config = requireNonNull( config );
    }

    public static BenchmarkConfig from( Path benchmarkConfigurationPath ) throws IOException
    {
        Properties neo4jConfigProperties = new Properties();
        try ( FileInputStream is = new FileInputStream( benchmarkConfigurationPath.toFile() ) )
        {
            neo4jConfigProperties.load( is );
        }
        return new BenchmarkConfig( new HashMap<>( Maps.fromProperties( neo4jConfigProperties ) ) );
    }

    public Map<String,String> toMap()
    {
        return unmodifiableMap( config );
    }

    @Override
    public boolean equals( Object o )
    {
        if ( this == o )
        {
            return true;
        }
        if ( o == null || getClass() != o.getClass() )
        {
            return false;
        }
        BenchmarkConfig that = (BenchmarkConfig) o;
        return Objects.equals( config, that.config );
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( config );
    }

    @Override
    public String toString()
    {
        return getClass().getSimpleName() + "{config=" + config + "}";
    }
}
