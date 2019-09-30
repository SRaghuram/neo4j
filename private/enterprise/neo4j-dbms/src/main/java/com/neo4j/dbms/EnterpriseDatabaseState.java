/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.dbms;

import java.util.Objects;
import java.util.Optional;

import org.neo4j.dbms.DatabaseState;
import org.neo4j.kernel.database.DatabaseId;

import static com.neo4j.dbms.EnterpriseOperatorState.INITIAL;
import static com.neo4j.dbms.EnterpriseOperatorState.UNKNOWN;

/**
 * Instances of this class record the current {@link EnterpriseOperatorState} of a database with the given {@link DatabaseId}
 * on this Neo4j instance. If the database in question has previously failed in transitioning from one state to
 * another then this object also records that failure.
 */
public class EnterpriseDatabaseState implements DatabaseState
{
    private final DatabaseId databaseId;
    private final EnterpriseOperatorState operationalState;
    private final Throwable failure;

    public static EnterpriseDatabaseState initial( DatabaseId id )
    {
        return new EnterpriseDatabaseState( id, INITIAL, null );
    }

    public static EnterpriseDatabaseState unknown( DatabaseId id )
    {
        return new EnterpriseDatabaseState( id, UNKNOWN, null );
    }

    static EnterpriseDatabaseState failedUnknownId( Throwable failure )
    {
        return new EnterpriseDatabaseState( null, UNKNOWN, failure );
    }

    public EnterpriseDatabaseState( DatabaseId databaseId, EnterpriseOperatorState operationalState )
    {
        this( databaseId, operationalState, null );
    }

    private EnterpriseDatabaseState( DatabaseId databaseId, EnterpriseOperatorState operationalState, Throwable failure )
    {
        this.databaseId = databaseId;
        this.operationalState = operationalState;
        this.failure = failure;
    }

    public DatabaseId databaseId()
    {
        return databaseId;
    }

    public EnterpriseOperatorState operatorState()
    {
        return operationalState;
    }

    public EnterpriseDatabaseState healthy()
    {
        return new EnterpriseDatabaseState( databaseId, operationalState, null );
    }

    public EnterpriseDatabaseState failed( Throwable failure )
    {
        return new EnterpriseDatabaseState( databaseId, operationalState, failure );
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
        return "EnterpriseDatabaseState{" + "databaseId=" + databaseId + ", operatorState=" + operationalState + ", failed=" + hasFailed() + '}';
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
        EnterpriseDatabaseState that = (EnterpriseDatabaseState) o;
        return hasFailed() == that.hasFailed() && Objects.equals( databaseId, that.databaseId ) && operationalState == that.operationalState;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( databaseId, operationalState, hasFailed() );
    }
}
