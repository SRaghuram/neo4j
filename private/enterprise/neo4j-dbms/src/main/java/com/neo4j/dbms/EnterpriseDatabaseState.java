/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.dbms;

import java.util.Objects;
import java.util.Optional;

import org.neo4j.dbms.DatabaseState;
import org.neo4j.kernel.database.NamedDatabaseId;

import static com.neo4j.dbms.EnterpriseOperatorState.INITIAL;
import static com.neo4j.dbms.EnterpriseOperatorState.UNKNOWN;

/**
 * Instances of this class record the current {@link EnterpriseOperatorState} of a database with the given {@link NamedDatabaseId}
 * on this Neo4j instance. If the database in question has previously failed in transitioning from one state to
 * another then this object also records that failure.
 */
public class EnterpriseDatabaseState implements DatabaseState
{
    private final NamedDatabaseId namedDatabaseId;
    private final EnterpriseOperatorState operationalState;
    private final Throwable failure;

    public static EnterpriseDatabaseState initial( NamedDatabaseId id )
    {
        return new EnterpriseDatabaseState( id, INITIAL, null );
    }

    public static EnterpriseDatabaseState unknown( NamedDatabaseId id )
    {
        return new EnterpriseDatabaseState( id, UNKNOWN, null );
    }

    public static EnterpriseDatabaseState initialUnknownId()
    {
        return new EnterpriseDatabaseState( null, INITIAL, null );
    }

    public EnterpriseDatabaseState( NamedDatabaseId namedDatabaseId, EnterpriseOperatorState operationalState )
    {
        this( namedDatabaseId, operationalState, null );
    }

    private EnterpriseDatabaseState( NamedDatabaseId namedDatabaseId, EnterpriseOperatorState operationalState, Throwable failure )
    {
        this.namedDatabaseId = namedDatabaseId;
        this.operationalState = operationalState;
        this.failure = failure;
    }

    @Override
    public NamedDatabaseId databaseId()
    {
        return namedDatabaseId;
    }

    @Override
    public EnterpriseOperatorState operatorState()
    {
        return operationalState;
    }

    public EnterpriseDatabaseState healthy()
    {
        return new EnterpriseDatabaseState( namedDatabaseId, operationalState, null );
    }

    public EnterpriseDatabaseState failed( Throwable failure )
    {
        return new EnterpriseDatabaseState( namedDatabaseId, operationalState, failure );
    }

    @Override
    public boolean hasFailed()
    {
        return failure != null;
    }

    @Override
    public Optional<Throwable> failure()
    {
        return Optional.ofNullable( failure );
    }

    @Override
    public String toString()
    {
        return "EnterpriseDatabaseState{" + "databaseId=" + namedDatabaseId + ", operatorState=" + operationalState + ", failed=" + hasFailed() + '}';
    }

    public static String logFromTo( EnterpriseDatabaseState currentState, EnterpriseDatabaseState desiredState )
    {
        return String.format( "from %s to %s", toShortString( currentState ), toShortString( desiredState ) );
    }

    private static String toShortString( EnterpriseDatabaseState state )
    {
        return state == null ? "unknown" :
                String.format( "%s%s{db=%s}", state.hasFailed() ? "FAILED/" : "", state.operatorState(), state.namedDatabaseId.logPrefix() );
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
        return hasFailed() == that.hasFailed() && Objects.equals( namedDatabaseId, that.namedDatabaseId ) && operationalState == that.operationalState;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( namedDatabaseId, operationalState, hasFailed() );
    }
}
