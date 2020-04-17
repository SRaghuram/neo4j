/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.dbms;

import com.neo4j.dbms.database.ClusteredMultiDatabaseManager;
import com.neo4j.dbms.database.DatabaseOperationCountMonitor;

import java.util.function.Consumer;

import org.neo4j.kernel.database.NamedDatabaseId;
import org.neo4j.logging.LogProvider;

import static com.neo4j.dbms.EnterpriseOperatorState.DIRTY;
import static com.neo4j.dbms.EnterpriseOperatorState.DROPPED;
import static com.neo4j.dbms.EnterpriseOperatorState.STARTED;
import static com.neo4j.dbms.EnterpriseOperatorState.STORE_COPYING;
import static com.neo4j.dbms.EnterpriseOperatorState.UNKNOWN;
import static java.lang.String.format;

/**
 * {@inheritDoc}
 *
 * This subclass contains those additional transition functions which are unique to clustered databases.
 */
final class ClusterReconcilerTransitions extends ReconcilerTransitions
{
    private final Transition startAfterStoreCopy;
    private final Transition stopBeforeStoreCopy;
    private final Transition logCleanupAndDrop;

    ClusterReconcilerTransitions( ClusteredMultiDatabaseManager databaseManager, LogProvider logProvider, DatabaseOperationCountMonitor monitor )
    {
        super( databaseManager, monitor );
        this.startAfterStoreCopy = startAfterStoreCopyFactory( databaseManager );
        this.stopBeforeStoreCopy = stopBeforeStoreCopyFactory( databaseManager );
        this.logCleanupAndDrop = logCleanupAndDropFactory( databaseManager, logProvider );
    }

    private static Transition startAfterStoreCopyFactory( ClusteredMultiDatabaseManager databaseManager )
    {
        return Transition.from( STORE_COPYING )
                         .doTransition( databaseManager::startDatabaseAfterStoreCopy )
                         .ifSucceeded( STARTED )
                         .ifFailed( STORE_COPYING );
    }

    private static Transition stopBeforeStoreCopyFactory( ClusteredMultiDatabaseManager databaseManager )
    {
        return Transition.from( STARTED )
                         .doTransition( databaseManager::stopDatabaseBeforeStoreCopy )
                         .ifSucceeded( STORE_COPYING )
                         .ifFailed( STORE_COPYING );
    }

    private static Transition logCleanupAndDropFactory( ClusteredMultiDatabaseManager databaseManager, LogProvider logProvider )
    {
        Consumer<NamedDatabaseId> transition = id ->
        {
            var log = logProvider.getLog( ClusterReconcilerTransitions.class );
            log.warn( format( "Pre-existing cluster state found with an unexpected id %s. This may indicate a previous " +
                              "DROP operation for %s did not complete. Cleanup of both the database and cluster-sate has been attempted. " +
                              "You may need to re-seed", id.databaseId().uuid(), id.name() ) );
            databaseManager.dropDatabase( id );
        };

        return Transition.from( UNKNOWN, DIRTY )
                         .doTransition( transition )
                         .ifSucceeded( DROPPED )
                         .ifFailed( DIRTY );
    }

    Transition startAfterStoreCopy()
    {
        return startAfterStoreCopy;
    }

    Transition stopBeforeStoreCopy()
    {
        return stopBeforeStoreCopy;
    }

    Transition logCleanupAndDrop()
    {
        return logCleanupAndDrop;
    }
}
