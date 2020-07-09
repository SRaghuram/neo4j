/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.dbms;

import com.neo4j.dbms.database.ClusteredMultiDatabaseManager;

import java.util.function.Consumer;

import org.neo4j.kernel.database.NamedDatabaseId;

import static com.neo4j.dbms.EnterpriseOperatorState.DIRTY;
import static com.neo4j.dbms.EnterpriseOperatorState.INITIAL;
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

    ClusterReconcilerTransitions( ClusteredMultiDatabaseManager databaseManager )
    {
        super( databaseManager );
        this.databaseManager = databaseManager;
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
}
