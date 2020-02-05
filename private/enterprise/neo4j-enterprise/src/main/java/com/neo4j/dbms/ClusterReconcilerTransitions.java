/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.dbms;

import com.neo4j.dbms.database.ClusteredMultiDatabaseManager;

import org.neo4j.kernel.database.NamedDatabaseId;
import org.neo4j.logging.LogProvider;

import static com.neo4j.dbms.EnterpriseOperatorState.DROPPED;
import static com.neo4j.dbms.EnterpriseOperatorState.STARTED;
import static com.neo4j.dbms.EnterpriseOperatorState.STORE_COPYING;
import static java.lang.String.format;

/**
 * {@inheritDoc}
 *
 * This subclass contains those additional transition functions which are unique to clustered databases.
 */
final class ClusterReconcilerTransitions extends ReconcilerTransitions
{
    private final ClusteredMultiDatabaseManager databaseManager;
    private final LogProvider logProvider;

    ClusterReconcilerTransitions( ClusteredMultiDatabaseManager databaseManager, LogProvider logProvider )
    {
        super( databaseManager );
        this.databaseManager = databaseManager;
        this.logProvider = logProvider;
    }

    EnterpriseDatabaseState startAfterStoreCopy( NamedDatabaseId namedDatabaseId )
    {
        databaseManager.startDatabaseAfterStoreCopy( namedDatabaseId );
        return new EnterpriseDatabaseState( namedDatabaseId, STARTED );
    }

    EnterpriseDatabaseState stopBeforeStoreCopy( NamedDatabaseId namedDatabaseId )
    {
        databaseManager.stopDatabaseBeforeStoreCopy( namedDatabaseId );
        return new EnterpriseDatabaseState( namedDatabaseId, STORE_COPYING );
    }

    EnterpriseDatabaseState logCleanupAndDrop( NamedDatabaseId namedDatabaseId )
    {
        var log = logProvider.getLog( getClass() );
        log.warn( format( "Pre-existing cluster state found with an unexpected id %s. This may indicate a previous " +
                        "DROP operation for %s did not complete. Cleanup of both the database and cluster-sate has been attempted. You may need to re-seed",
                namedDatabaseId.databaseId().uuid(), namedDatabaseId.name() ) );
        databaseManager.dropDatabase( namedDatabaseId );
        return new EnterpriseDatabaseState( namedDatabaseId, DROPPED );
    }
}
