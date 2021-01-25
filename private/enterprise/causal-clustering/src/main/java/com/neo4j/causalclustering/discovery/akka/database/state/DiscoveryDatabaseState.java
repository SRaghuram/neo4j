/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery.akka.database.state;

import java.util.Objects;
import java.util.Optional;

import org.neo4j.dbms.DatabaseState;
import org.neo4j.dbms.OperatorState;
import org.neo4j.kernel.database.DatabaseId;

import static com.neo4j.dbms.EnterpriseOperatorState.UNKNOWN;

public class DiscoveryDatabaseState
{
    private final DatabaseId databaseId;
    private final OperatorState operatorState;
    private final Throwable failure;

    public DiscoveryDatabaseState( DatabaseId databaseId, OperatorState operatorState )
    {
        this( databaseId, operatorState, null );
    }

    public DiscoveryDatabaseState( DatabaseId databaseId, OperatorState operatorState, Throwable failure )
    {
        this.databaseId = databaseId;
        this.operatorState = operatorState;
        this.failure = failure;
    }

    public static DiscoveryDatabaseState from( DatabaseState databaseState )
    {
        return new DiscoveryDatabaseState(
                databaseState.databaseId().databaseId(),
                databaseState.operatorState(),
                databaseState.failure().orElse( null ) );
    }

    public static DiscoveryDatabaseState unknown( DatabaseId id )
    {
        return new DiscoveryDatabaseState( id, UNKNOWN, null );
    }

    public DatabaseId databaseId()
    {
        return databaseId;
    }

    public OperatorState operatorState()
    {
        return operatorState;
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
        DiscoveryDatabaseState that = (DiscoveryDatabaseState) o;
        return Objects.equals( databaseId, that.databaseId ) && Objects.equals( operatorState, that.operatorState ) && Objects.equals( failure, that.failure );
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( databaseId, operatorState, failure );
    }

    @Override
    public String toString()
    {
        return "DiscoveryDatabaseState{" + databaseId + ", operatorState=" + operatorState + ", result=" + printResult( failure ) + '}';
    }

    private static String printResult( Throwable failure )
    {
        return failure == null ? "SUCCESS" : "FAIL [" + failure + ']';
    }
}
