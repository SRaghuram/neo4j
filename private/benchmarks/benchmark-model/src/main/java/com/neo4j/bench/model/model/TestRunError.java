/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.model.model;

import java.util.Objects;

import static java.lang.String.format;

public class TestRunError
{
    private final String groupName;
    private final String benchmarkName;
    private final String message;

    /**
     * WARNING: Never call this explicitly.
     * No-params constructor is only used for JSON (de)serialization.
     */
    public TestRunError()
    {
        this( "", "", "" );
    }

    public TestRunError( String groupName, String benchmarkName, String message )
    {
        this.groupName = assertValidName( groupName );
        this.benchmarkName = assertValidName( benchmarkName );
        this.message = message;
    }

    private static String assertValidName( String name )
    {
        boolean isNotOneLine = name.contains( "\n" ) || name.contains( "\r" );
        if ( isNotOneLine )
        {
            throw new RuntimeException( "Name must be one line\n" + name );
        }
        return name;
    }

    public String groupName()
    {
        return groupName;
    }

    public String benchmarkName()
    {
        return benchmarkName;
    }

    public String message()
    {
        return message;
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
        TestRunError that = (TestRunError) o;
        return Objects.equals( groupName, that.groupName ) &&
               Objects.equals( benchmarkName, that.benchmarkName ) &&
               Objects.equals( message, that.message );
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( groupName, benchmarkName, message );
    }

    @Override
    public String toString()
    {
        return format( "%s.%s\n" +
                       "%s", groupName, benchmarkName, message );
    }
}
