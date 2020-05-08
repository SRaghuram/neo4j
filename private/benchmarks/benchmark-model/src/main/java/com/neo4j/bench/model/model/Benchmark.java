/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.model.model;

import com.neo4j.bench.model.util.JsonUtil;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import static com.neo4j.bench.model.model.Benchmark.Mode.LATENCY;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class Benchmark
{
    public enum Mode
    {
        THROUGHPUT,
        LATENCY,
        SINGLE_SHOT
    }

    public static final String NAME = "name";
    public static final String SIMPLE_NAME = "simple_name";
    public static final String ACTIVE = "active";
    public static final String DESCRIPTION = "description";
    public static final String QUERY = "cypher_query";
    public static final String MODE = "mode";

    public static Benchmark benchmarkFor( String description, String simpleName, Mode mode, Map<String,String> parametersMap )
    {
        Parameters parameters = Parameters.fromMap( parametersMap );
        String name = constructName( simpleName, parameters, mode );
        return new Benchmark( name, simpleName, description, mode, parameters );
    }

    public static Benchmark benchmarkFor( String description, String simpleName, Mode mode, Map<String,String> parametersMap, String queryString )
    {
        Parameters parameters = Parameters.fromMap( parametersMap );
        String name = constructName( simpleName, parameters, mode );
        return new Benchmark( name, simpleName, description, mode, parameters, queryString );
    }

    // TODO rather than saving the 'name' this method could split the name into params, and not take params as input at all
    public static Benchmark benchmarkFor( String description, String simpleName, String name, Mode mode, Map<String,String> parametersMap )
    {
        return new Benchmark( name, simpleName, description, mode, Parameters.fromMap( parametersMap ) );
    }

    private static String constructName( String simpleName, Parameters parameters, Mode mode )
    {
        return simpleName + nameSuffixFor( parameters, mode );
    }

    // orders parameters to achieve determinism, names are used as keys in results store
    private static String nameSuffixFor( Parameters parameters, Mode mode )
    {
        String parametersString = parameters.toString();
        parametersString = parametersString.isEmpty() ? parametersString : "_" + parametersString;
        return parametersString + format( "_(%s,%s)", MODE, mode.name() );
    }

    // TODO is it even necessary to store the 'name', given that it can be recomputed from the other fields?
    private final String name;
    private final String queryString;
    private final String simpleName;
    private final String description;
    private final Mode mode;
    private final Parameters parameters;

    /**
     * WARNING: Never call this explicitly.
     * No-params constructor is only used for JSON (de)serialization.
     */
    Benchmark()
    {
        this( "11", "1", "1", LATENCY, Parameters.NONE, "" );
    }

    private Benchmark( String name, String simpleName, String description, Mode mode, Parameters parameters )
    {
        this( name, simpleName, description, mode, parameters, null );
    }

    private Benchmark( String name, String simpleName, String description, Mode mode, Parameters parameters, String queryString )
    {
        this.name = requireNonNull( name );
        this.simpleName = requireNonNull( simpleName );
        this.description = requireNonNull( description );
        this.mode = requireNonNull( mode );
        this.parameters = requireNonNull( parameters );
        this.queryString = queryString;
        assertSimpleNameIsPrefixOfName( name, simpleName );
        assertNotEmptyString( name, simpleName );
    }

    private static void assertSimpleNameIsPrefixOfName( String name, String simpleName )
    {
        if ( !name.startsWith( simpleName ) )
        {
            throw new RuntimeException( format( "'%s' (%s) should be prefixed by '%s' (%s), but is not",
                                                NAME, name, SIMPLE_NAME, simpleName ) );
        }
    }

    private static void assertNotEmptyString( String name, String simpleName )
    {
        if ( name.isEmpty() )
        {
            throw new RuntimeException( format( "'%s' must not be empty string", NAME ) );
        }
        if ( simpleName.isEmpty() )
        {
            throw new RuntimeException( format( "'%s' must not be empty string", SIMPLE_NAME ) );
        }
    }

    public String name()
    {
        return name;
    }

    public String simpleName()
    {
        return simpleName;
    }

    public Mode mode()
    {
        return mode;
    }

    public Map<String,String> parameters()
    {
        return parameters.asMap();
    }

    public Map<String,Object> toMap()
    {
        Map<String,Object> benchmarkNodeMap = new HashMap<>();
        benchmarkNodeMap.put( NAME, name() );
        benchmarkNodeMap.put( SIMPLE_NAME, simpleName() );
        benchmarkNodeMap.put( MODE, mode().name() );
        benchmarkNodeMap.put( QUERY, queryString );
        benchmarkNodeMap.put( DESCRIPTION, description );
        benchmarkNodeMap.put( ACTIVE, true );
        return benchmarkNodeMap;
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
        Benchmark benchmark = (Benchmark) o;
        return Objects.equals( name, benchmark.name ) && Objects.equals( simpleName, benchmark.simpleName ) &&
               Objects.equals( description, benchmark.description ) && mode == benchmark.mode &&
               Objects.equals( parameters, benchmark.parameters ) && Objects.equals( queryString, benchmark.queryString );
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( name, simpleName, description, mode, parameters, queryString );
    }

    @Override
    public String toString()
    {
        //        return format( "%s(%s=%s, %s=%s, %s=%s, %s=%s, parameters=%s)",
        //                getClass().getSimpleName(),
        //                NAME, name,
        //                SIMPLE_NAME, simpleName,
        //                DESCRIPTION, description,
        //                MODE, mode,
        //                parameters() );
        // TODO JSON map key deserialization depends on this. Do not change until that dependency is removed/fixed.
        return JsonUtil.serializeJson( this );
    }
}