/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.procedure.enterprise.builtin;

import java.util.Objects;

import org.neo4j.configuration.helpers.DatabaseNameValidator;
import org.neo4j.configuration.helpers.NormalizedDatabaseName;
import org.neo4j.kernel.api.exceptions.InvalidArgumentsException;

import static java.util.Objects.requireNonNull;

class TransactionId
{
    private static final String SEPARATOR = "-transaction-";
    private static final String EXPECTED_FORMAT_MSG = "(expected format: <databasename>" + SEPARATOR + "<id>)";
    private final NormalizedDatabaseName database;
    private final long internalId;

    TransactionId( String database, long internalId ) throws InvalidArgumentsException
    {
        this.database = new NormalizedDatabaseName( requireNonNull( database ) );
        if ( internalId <= 0 )
        {
            throw new InvalidArgumentsException( "Negative ids are not supported " + EXPECTED_FORMAT_MSG );
        }
        this.internalId = internalId;
    }

    static TransactionId parse( String queryIdText ) throws InvalidArgumentsException
    {
        try
        {
            int i = queryIdText.lastIndexOf( SEPARATOR );
            if ( i != -1 )
            {
                var database = normalizeAndValidateDatabaseName( queryIdText.substring( 0, i ) );
                if ( database.name().length() > 0 )
                {
                    String qid = queryIdText.substring( i + SEPARATOR.length() );
                    var internalId = Long.parseLong( qid );
                    return new TransactionId( database.name(), internalId );
                }
            }
        }
        catch ( NumberFormatException e )
        {
            throw new InvalidArgumentsException( "Could not parse id " + EXPECTED_FORMAT_MSG, e );
        }
        throw new InvalidArgumentsException( "Could not parse id " + EXPECTED_FORMAT_MSG );
    }

    NormalizedDatabaseName database()
    {
        return database;
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
        TransactionId other = (TransactionId) o;
        return internalId == other.internalId && database.equals( other.database );
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( database, internalId );
    }

    @Override
    public String toString()
    {
        return database.name() + SEPARATOR + internalId;
    }

    private static NormalizedDatabaseName normalizeAndValidateDatabaseName( String databaseName )
    {
        NormalizedDatabaseName normalizedDatabaseName = new NormalizedDatabaseName( databaseName );
        DatabaseNameValidator.validateInternalDatabaseName( normalizedDatabaseName );
        return normalizedDatabaseName;
    }
}
