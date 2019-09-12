/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.fabric.executor;

import java.io.PrintStream;
import java.io.PrintWriter;
import java.util.List;
import java.util.stream.Collectors;

public class FabricMultiException extends RuntimeException
{
    private final List<RuntimeException> errors;

    public FabricMultiException( List<RuntimeException> errors )
    {
        super( getFirst( errors ).getMessage(), getFirst( errors ) );
        this.errors = errors;
    }

    private static RuntimeException getFirst( List<RuntimeException> errors )
    {
        if ( errors.isEmpty() )
        {
            throw new IllegalArgumentException( "The submitted error list cannot be empty" );
        }

        return errors.get( 0 );
    }

    public List<RuntimeException> getErros()
    {
        return errors;
    }

    @Override
    public String getMessage()
    {
        return errors.stream()
                .map( e -> e.getClass().getName() + ((e.getMessage() != null) ? ": " + e.getMessage() : "") )
                .collect( Collectors.joining( "\n", "A MultiException has the following " + errors.size() + " exceptions:\n", "" ) );
    }

    @Override
    public void printStackTrace( PrintStream s )
    {
        for ( Throwable error : errors )
        {
            error.printStackTrace( s );
        }
    }

    @Override
    public void printStackTrace( PrintWriter s )
    {
        for ( Throwable error : errors )
        {
            error.printStackTrace( s );
        }
    }
}
