/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.dbms;

import com.neo4j.dbms.database.ClusteredMultiDatabaseManager;

import java.util.function.Consumer;

import org.neo4j.kernel.database.NamedDatabaseId;

import static com.neo4j.dbms.EnterpriseOperatorState.DIRTY;
import static com.neo4j.dbms.EnterpriseOperatorState.DROPPED;
import static com.neo4j.dbms.EnterpriseOperatorState.QUARANTINED;
import static com.neo4j.dbms.EnterpriseOperatorState.STARTED;
import static com.neo4j.dbms.EnterpriseOperatorState.STOPPED;
import static com.neo4j.dbms.EnterpriseOperatorState.STORE_COPYING;

/**
 * {@inheritDoc}
 *
 * This subclass contains those additional transition functions which are unique to clustered databases.
 */
final class ClusterReconcilerTransitions extends ReconcilerTransitions
{
    private final ClusteredMultiDatabaseManager databaseManager;
    private final QuarantineOperator quarantineOperator;

    ClusterReconcilerTransitions( ClusteredMultiDatabaseManager databaseManager, QuarantineOperator quarantineOperator )
    {
        super( databaseManager );
        this.databaseManager = databaseManager;
        this.quarantineOperator = quarantineOperator;
    }

    private static Transition startAfterStoreCopyFactory( ClusteredMultiDatabaseManager databaseManager )
    {
        return Transition.from( STORE_COPYING )
                         .doTransition( databaseManager::startDatabaseAfterStoreCopy )
                         .ifSucceeded( STARTED )
                         .ifFailedThenDo( databaseManager::stopDatabase, STOPPED );
    }

    private static Transition stopBeforeStoreCopyFactory( ClusteredMultiDatabaseManager databaseManager )
    {
        return Transition.from( STARTED )
                         .doTransition( databaseManager::stopDatabaseBeforeStoreCopy )
                         .ifSucceeded( STORE_COPYING )
                         .ifFailedThenDo( databaseManager::stopDatabase, STOPPED );
    }

    private static Transition ensureDirtyDatabaseExists( ClusteredMultiDatabaseManager databaseManager )
    {
        Consumer<NamedDatabaseId> transitionFunction = databaseId ->
        {
            var databaseExists = databaseManager.getDatabaseContext( databaseId ).isPresent();
            if ( !databaseExists )
            {
                databaseManager.createDatabase( databaseId );
            }
        };
        return Transition.from( DIRTY )
                         .doTransition( transitionFunction )
                         .ifSucceeded( DIRTY )
                         .ifFailedThenDo( ReconcilerTransitions.nothing, DIRTY );
    }

    private static Transition setQuarantineMarkerFactory( ClusteredMultiDatabaseManager databaseManager, QuarantineOperator quarantineOperator )
    {
        return Transition.from( STOPPED, DIRTY )
                .doTransition( namedDatabaseId -> {
                    quarantineOperator.setQuarantineMarker( namedDatabaseId );
                    databaseManager.removeDatabaseContext( namedDatabaseId );
                } )
                .ifSucceeded( QUARANTINED )
                .ifFailedThenDo( nothing, QUARANTINED );
    }

    private static Transition removeQuarantineMarkerFactory( QuarantineOperator quarantineOperator )
    {
        return Transition.from( QUARANTINED )
                .doTransition( quarantineOperator::removeQuarantineMarker )
                .ifSucceeded( DROPPED )
                .ifFailedThenDo( nothing, DIRTY );
    }

    Transition ensureDirtyDatabaseExists()
    {
        return ensureDirtyDatabaseExists( databaseManager );
    }

    Transition startAfterStoreCopy()
    {
        return startAfterStoreCopyFactory( databaseManager );
    }

    Transition stopBeforeStoreCopy()
    {
        return stopBeforeStoreCopyFactory( databaseManager );
    }

    Transition setQuarantine()
    {
        return setQuarantineMarkerFactory( databaseManager, quarantineOperator );
    }

    Transition removeQuarantine()
    {
        return removeQuarantineMarkerFactory( quarantineOperator );
    }
}
