/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.procedure.enterprise.builtin;

import java.util.Objects;

import org.neo4j.configuration.helpers.DatabaseNameValidator;
import org.neo4j.configuration.helpers.NormalizedDatabaseName;
import org.neo4j.kernel.api.exceptions.InvalidArgumentsException;

import static java.util.Objects.requireNonNull;

class DbmsId
{
    private final String separator;
    private final NormalizedDatabaseName database;
    private final long internalId;

    DbmsId( String queryIdText, String separator ) throws InvalidArgumentsException
    {
        this.separator = separator;
        try
        {
            int i = queryIdText.lastIndexOf( separator );
            if ( i != -1 )
            {
                database = normalizeAndValidateDatabaseName( queryIdText.substring( 0, i ) );
                if ( database.name().length() > 0 )
                {
                    String qid = queryIdText.substring( i + separator.length() );
                    internalId = Long.parseLong( qid );
                    if ( internalId <= 0 )
                    {
                        throw new InvalidArgumentsException( "Negative ids are not supported (expected format: <databasename>" + separator + "<id>)" );
                    }
                    return;
                }
            }
        }
        catch ( NumberFormatException e )
        {
            throw new InvalidArgumentsException( "Could not parse id (expected format: <databasename>" + separator + "<id>)", e );
        }
        throw new InvalidArgumentsException( "Could not parse id (expected format: <databasename>" + separator + "<id>)" );
    }

    DbmsId( String database, long internalId, String separator ) throws InvalidArgumentsException
    {
        this.separator = separator;
        this.database = new NormalizedDatabaseName( requireNonNull( database ) );
        if ( internalId <= 0 )
        {
            throw new InvalidArgumentsException( "Negative ids are not supported (expected format: <databasename>" + separator + "<id>)" );
        }
        this.internalId = internalId;
    }

    long internalId()
    {
        return internalId;
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
        DbmsId other = (DbmsId) o;
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
        return database.name() + separator + internalId;
    }

    private static NormalizedDatabaseName normalizeAndValidateDatabaseName( String databaseName )
    {
        NormalizedDatabaseName normalizedDatabaseName = new NormalizedDatabaseName( databaseName );
        DatabaseNameValidator.assertValidDatabaseName( normalizedDatabaseName );
        return normalizedDatabaseName;
    }
}
