/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.state;

public class StateMachineResult
{
    private final Exception exception;
    private final Object result;

    private StateMachineResult( Exception exception )
    {
        this.exception = exception;
        this.result = null;
    }

    private StateMachineResult( Object result )
    {
        this.result = result;
        this.exception = null;
    }

    public static StateMachineResult of( Object result )
    {
        return new StateMachineResult( result );
    }

    public static StateMachineResult of( Exception exception )
    {
        return new StateMachineResult( exception );
    }

    public <T> T consume() throws Exception
    {
        if ( exception != null )
        {
            throw exception;
        }
        else
        {
            //noinspection unchecked
            return (T) result;
        }
    }

    @Override
    public String toString()
    {
        return "Result{" +
               "exception=" + exception +
               ", result=" + result +
               '}';
    }
}
