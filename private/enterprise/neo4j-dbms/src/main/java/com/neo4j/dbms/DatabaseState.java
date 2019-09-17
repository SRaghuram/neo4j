/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.dbms;

import java.util.Objects;
import java.util.Optional;

import org.neo4j.kernel.database.DatabaseId;

import static com.neo4j.dbms.OperatorState.INITIAL;
import static com.neo4j.dbms.OperatorState.UNKNOWN;

class DatabaseState
{
    private final DatabaseId databaseId;
    private final OperatorState operationalState;
    private final Throwable failure;

    public static DatabaseState initial( DatabaseId id )
    {
        return new DatabaseState( id, INITIAL, null );
    }

    public static DatabaseState unknown( DatabaseId id )
    {
        return new DatabaseState( id, UNKNOWN, null );
    }

    public static DatabaseState failedUnknownId( Throwable failure )
    {
        return new DatabaseState( null, UNKNOWN, failure );
    }

    DatabaseState( DatabaseId databaseId, OperatorState operationalState )
    {
        this( databaseId, operationalState, null );
    }

    private DatabaseState( DatabaseId databaseId, OperatorState operationalState, Throwable failure )
    {
        this.databaseId = databaseId;
        this.operationalState = operationalState;
        this.failure = failure;
    }

    public DatabaseId databaseId()
    {
        return databaseId;
    }

    OperatorState operationalState()
    {
        return operationalState;
    }

    public DatabaseState healthy()
    {
        return new DatabaseState( databaseId, operationalState, null );
    }

    public DatabaseState failed( Throwable failure )
    {
        return new DatabaseState( databaseId, operationalState, failure );
    }

    public boolean hasFailed()
    {
        return failure != null;
    }

    public Optional<Throwable> failure()
    {
        return Optional.ofNullable( failure );
    }

    @Override
    public String toString()
    {
        return "DatabaseState{" + "databaseId=" + databaseId + ", operationalState=" + operationalState + ", failed=" + hasFailed() + '}';
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
        DatabaseState that = (DatabaseState) o;
        return hasFailed() == that.hasFailed() && Objects.equals( databaseId, that.databaseId ) && operationalState == that.operationalState;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( databaseId, operationalState, hasFailed() );
    }
}
