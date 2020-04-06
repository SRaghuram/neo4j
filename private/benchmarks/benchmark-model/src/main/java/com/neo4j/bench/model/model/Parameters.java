/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.model.model;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonMap;
import static java.util.stream.Collectors.joining;

public class Parameters
{
    public static final Parameters NONE = Parameters.fromMap( emptyMap() );
    public static final Parameters CLIENT = Parameters.fromMap( singletonMap( "process", "client" ) );
    public static final Parameters SERVER = Parameters.fromMap( singletonMap( "process", "server" ) );

    private static final String PREFIX = "(";
    private static final String SUFFIX = ")";
    private static final String SEPARATOR = ",";
    private static final String PARAMETER_SEPARATOR = "_";

    // String must be a sequence of key-value pairs, of the form [_(k,v)]*
    public static Parameters parse( String parametersString )
    {
        if ( Objects.requireNonNull( parametersString ).isEmpty() )
        {
            return new Parameters( new HashMap<>() );
        }
        if ( !parametersString.startsWith( PREFIX ) || !parametersString.endsWith( SUFFIX ) )
        {
            throw new RuntimeException( "Invalid parameters string: " + parametersString + "\n" +
                                        "Expected prefix: " + PREFIX + "\n" +
                                        "Expected suffix: " + SUFFIX );
        }
        int keyStart = parametersString.indexOf( PREFIX ) + PREFIX.length();
        int keyEnd = parametersString.indexOf( SEPARATOR );
        String key = parametersString.substring( keyStart, keyEnd );
        int valueStart = keyEnd + SEPARATOR.length();
        int valueEnd = parametersString.indexOf( SUFFIX );
        String value = parametersString.substring( valueStart, valueEnd );
        Map<String,String> remaining = parse( parametersString.substring( valueEnd + SUFFIX.length() ) ).asMap();
        remaining.put( key, value );
        return new Parameters( remaining );
    }

    public static Parameters fromMap( Map<String,String> parametersMap )
    {
        return new Parameters( parametersMap );
    }

    private final Map<String,String> inner;

    /**
     * WARNING: Never call this explicitly.
     * No-params constructor is only used for JSON (de)serialization.
     */
    public Parameters()
    {
        this( new HashMap<>() );
    }

    public Parameters( Map<String,String> inner )
    {
        this.inner = inner;
    }

    public Map<String,String> asMap()
    {
        return inner;
    }

    public boolean isEmpty()
    {
        return inner.isEmpty();
    }

    @Override
    public String toString()
    {
        return inner.keySet().stream()
                    .sorted()
                    .map( key -> PREFIX + key + SEPARATOR + inner.get( key ) + SUFFIX )
                    .collect( joining( PARAMETER_SEPARATOR ) );
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
        Parameters that = (Parameters) o;
        return Objects.equals( inner, that.inner );
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( inner );
    }
}
