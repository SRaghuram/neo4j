/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.procedure.enterprise.builtin;

import java.util.Objects;

import org.neo4j.kernel.api.exceptions.InvalidArgumentsException;

public class QueryId
{
    public static final String PREFIX = "query-";
    private static final String EXPECTED_FORMAT_MSG = "(expected format: query-<id>)";
    private final long internalId;

    QueryId( long internalId ) throws InvalidArgumentsException
    {
        if ( internalId <= 0 )
        {
            throw new InvalidArgumentsException( "Negative ids are not supported " + EXPECTED_FORMAT_MSG );
        }
        this.internalId = internalId;
    }

    static QueryId parse( String queryIdText ) throws InvalidArgumentsException
    {
        try
        {
            if ( !queryIdText.startsWith( PREFIX ) )
            {
                throw new InvalidArgumentsException( "Expected prefix " + PREFIX );
            }
            String qid = queryIdText.substring( PREFIX.length() );
            var internalId = Long.parseLong( qid );
            return new QueryId( internalId );
        }
        catch ( Exception e )
        {
            throw new InvalidArgumentsException( "Could not parse id " + queryIdText + " " + EXPECTED_FORMAT_MSG, e );
        }
    }

    long internalId()
    {
        return internalId;
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
        QueryId other = (QueryId) o;
        return internalId == other.internalId;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( internalId );
    }

    @Override
    public String toString()
    {
        return PREFIX + internalId;
    }
}
