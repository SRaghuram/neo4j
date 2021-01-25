/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.catchup.v4.info;

import org.neo4j.bolt.txtracking.ReconciledTransactionTracker;
import org.neo4j.dbms.DatabaseStateService;
import org.neo4j.dbms.database.DatabaseManager;
import org.neo4j.kernel.availability.UnavailableException;
import org.neo4j.kernel.database.DatabaseIdRepository;
import org.neo4j.kernel.database.NamedDatabaseId;

public class InfoProvider
{

    private static UnavailableException unavailableDbException( String databaseDescription )
    {
        return new UnavailableException( "Unable to resolve local database for id " + databaseDescription );
    }

    private ReconciledTransactionTracker reconciledTransactionTracker;
    private final DatabaseManager<?> databaseManager;
    private final DatabaseStateService databaseStateService;

    public InfoProvider( DatabaseManager<?> databaseManager, DatabaseStateService databaseStateService )
    {
        this.databaseManager = databaseManager;
        this.databaseStateService = databaseStateService;
    }

    public InfoResponse getInfo( NamedDatabaseId namedDatabaseId ) throws UnavailableException
    {
        var reconciledId = getReconciliationTxIdTracker().getLastReconciledTransactionId();

        var reconcilerFailureForDb = databaseStateService.causeOfFailure( namedDatabaseId );

        return InfoResponse.create( reconciledId, reconcilerFailureForDb.map( Throwable::toString ).orElse( null ) );
    }

    private ReconciledTransactionTracker getReconciliationTxIdTracker() throws UnavailableException
    {
        if ( reconciledTransactionTracker == null )
        {
            reconciledTransactionTracker = databaseManager.getDatabaseContext( DatabaseIdRepository.NAMED_SYSTEM_DATABASE_ID )
                    .map( databaseContext -> databaseContext.dependencies()
                            .resolveDependency( ReconciledTransactionTracker.class ) )
                    .orElseThrow( () -> unavailableDbException( DatabaseIdRepository.NAMED_SYSTEM_DATABASE_ID.toString() ) );
        }
        return reconciledTransactionTracker;
    }
}
