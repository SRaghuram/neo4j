/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.dbms;

import com.neo4j.dbms.database.ClusteredMultiDatabaseManager;

import org.neo4j.logging.LogProvider;

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
    private final Transition startAfterStoreCopy;
    private final Transition stopBeforeStoreCopy;

    ClusterReconcilerTransitions( ClusteredMultiDatabaseManager databaseManager, LogProvider logProvider )
    {
        super( databaseManager );
        this.startAfterStoreCopy = startAfterStoreCopyFactory( databaseManager );
        this.stopBeforeStoreCopy = stopBeforeStoreCopyFactory( databaseManager );
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

    Transition startAfterStoreCopy()
    {
        return startAfterStoreCopy;
    }

    Transition stopBeforeStoreCopy()
    {
        return stopBeforeStoreCopy;
    }
}
