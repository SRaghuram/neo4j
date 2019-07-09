/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.client.model;

import com.neo4j.bench.client.util.BenchmarkUtil;
import com.neo4j.bench.client.util.JsonUtil;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static java.util.Collections.unmodifiableMap;
import static java.util.Objects.requireNonNull;

public class Neo4jConfig
{

    public static Neo4jConfig empty()
    {
        return new Neo4jConfig();
    }

    public static Neo4jConfig from( Map<String,String> config, List<String> jvmArgs )
    {
        return new Neo4jConfig( config, jvmArgs );
    }

    public static Neo4jConfig fromJson( String json )
    {
        return JsonUtil.deserializeJson( json, Neo4jConfig.class );
    }

    private final Map<String,String> config;
    private final List<String> jvmArgs;

    /**
     * WARNING: Never call this explicitly.
     * No-params constructor is only used for JSON (de)serialization.
     */
    private Neo4jConfig()
    {
        this( new HashMap<>(), new ArrayList<>() );
    }

    public Neo4jConfig( Map<String,String> config )
    {
        this( requireNonNull( config ), new ArrayList<>() );
    }

    private Neo4jConfig( Map<String,String> config, List<String> jvmArgs )
    {
        this.config = requireNonNull( config );
        this.jvmArgs = requireNonNull( jvmArgs );
    }

    public Map<String,String> toMap()
    {
        return unmodifiableMap( config );
    }

    public List<String> getJvmArgs()
    {
        return Collections.unmodifiableList( jvmArgs );
    }

    public Neo4jConfig addJvmArgs( List<String> additionalJvmArgs )
    {
        Neo4jConfig newNeo4jConig = new Neo4jConfig( new HashMap<>( config ), new ArrayList<>( jvmArgs ) );
        for ( String jvmArg : additionalJvmArgs )
        {
            newNeo4jConig = newNeo4jConig.addJvmArg( jvmArg );
        }
        return newNeo4jConig;
    }

    public Neo4jConfig addJvmArg( String additionalJvmArg )
    {
        List<String> newJvmArgs = new ArrayList<>( jvmArgs );
        if ( !newJvmArgs.contains( additionalJvmArg ) )
        {
            newJvmArgs.add( additionalJvmArg );
        }
        return new Neo4jConfig( new HashMap<>( config ), newJvmArgs );
    }

    public Neo4jConfig setJvmArgs( List<String> newJvmArgs )
    {
        return new Neo4jConfig( new HashMap<>( config ), newJvmArgs );
    }

    public Neo4jConfig withSetting( String setting, String value )
    {
        HashMap<String,String> newConfig = new HashMap<>( config );
        newConfig.put( setting, value );
        return new Neo4jConfig( newConfig, new ArrayList<>( jvmArgs ) );
    }

    public Neo4jConfig mergeWith( Neo4jConfig otherNeo4jConfig )
    {
        Neo4jConfig newNeo4jConfig = new Neo4jConfig( new HashMap<>( config ), new ArrayList<>( jvmArgs ) );
        newNeo4jConfig.config.putAll( otherNeo4jConfig.config );
        return newNeo4jConfig.addJvmArgs( otherNeo4jConfig.jvmArgs );
    }

    public String toJson()
    {
        return JsonUtil.serializeJson( this );
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
        Neo4jConfig that = (Neo4jConfig) o;
        return Objects.equals( config, that.config ) &&
               Objects.equals( jvmArgs, that.jvmArgs );
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( config, jvmArgs );
    }

    @Override
    public String toString()
    {
        return getClass().getSimpleName() + ":\n" +
               "JVM Args: " + jvmArgs + "\n" +
               BenchmarkUtil.prettyPrint( config );
    }
}
