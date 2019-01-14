/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.causalclustering.core.state;

public class Result
{
    private final Exception exception;
    private final Object result;

    private Result( Exception exception )
    {
        this.exception = exception;
        this.result = null;
    }

    private Result( Object result )
    {
        this.result = result;
        this.exception = null;
    }

    public static Result of( Object result )
    {
        return new Result( result );
    }

    public static Result of( Exception exception )
    {
        return new Result( exception );
    }

    public Object consume() throws Exception
    {
        if ( exception != null )
        {
            throw exception;
        }
        else
        {
            return result;
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
