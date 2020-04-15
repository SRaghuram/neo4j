/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.dbms;

import java.util.HashSet;
import java.util.Map;
import java.util.stream.Collectors;

import org.neo4j.bolt.txtracking.ReconciledTransactionTracker;
import org.neo4j.dbms.DatabaseStateService;
import org.neo4j.graphdb.event.TransactionData;
import org.neo4j.kernel.database.NamedDatabaseId;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

import static com.neo4j.dbms.EnterpriseOperatorState.DIRTY;
import static com.neo4j.dbms.EnterpriseOperatorState.STARTED;
import static org.neo4j.kernel.database.DatabaseIdRepository.NAMED_SYSTEM_DATABASE_ID;

/**
 * Operator driving database management operations in response to changes in the system database
 */
class SystemGraphDbmsOperator extends DbmsOperator
{
    private final EnterpriseSystemGraphDbmsModel dbmsModel;
    private final ReconciledTransactionTracker reconciledTxTracker;
    private final DatabaseStateService databaseStateService;
    private final Log log;

    SystemGraphDbmsOperator( EnterpriseSystemGraphDbmsModel dbmsModel, ReconciledTransactionTracker reconciledTxTracker,
            DatabaseStateService databaseStateService, LogProvider logProvider )
    {
        this.dbmsModel = dbmsModel;
        this.reconciledTxTracker = reconciledTxTracker;
        this.desired.put( NAMED_SYSTEM_DATABASE_ID.name(), new EnterpriseDatabaseState( NAMED_SYSTEM_DATABASE_ID, STARTED ) );
        this.databaseStateService = databaseStateService;
        this.log = logProvider.getLog( getClass() );
    }

    void transactionCommitted( long txId, TransactionData transactionData )
    {
        reconcile( txId, transactionData, false );
    }

    void storeReplaced( long txId )
    {
        reconcile( txId, null, true );
    }

    private void reconcile( long txId, TransactionData transactionData, boolean asPartOfStoreCopy )
    {
        var databasesToHandle = extractUpdatedDatabases( transactionData );
        updateDesiredStates(); // TODO: Handle exceptions from this!
        if ( asPartOfStoreCopy )
        {
            reconciledTxTracker.disable();
        }
        var databasesForPriority = databasesToHandle.touched().stream().filter( this::mayDatabaseBeRetriedWithPriority ).collect( Collectors.toSet() );

        var reconcilerResult = trigger( databasesForPriority.isEmpty() ? ReconcilerRequest.simple() : ReconcilerRequest.priority( databasesForPriority ) );
        reconcilerResult.whenComplete( () -> offerReconciledTransactionId( txId, asPartOfStoreCopy ) );

        var databasesToAwait = new HashSet<>( databasesToHandle.changed() );
        databasesToAwait.addAll( databasesForPriority );

        // Note: only blocks for completed reconciliation on this machine. Global reconciliation (e.g. including other cluster members) is still asynchronous
        reconcilerResult.await( databasesToAwait );
    }

    /**
     * Synchronized so that concurrent reads of the states from the database and the updates
     * of the desired states happen in order. Otherwise r(A), r(B) representing reads of database
     * states could be written into the the desired map as w(B), w(A) leaving the older states
     * in place.
     */
    synchronized void updateDesiredStates()
    {
        Map<String,EnterpriseDatabaseState> systemStates = dbmsModel.getDatabaseStates();
        systemStates.forEach( desired::put );
    }

    private DatabaseUpdates extractUpdatedDatabases( TransactionData transactionData )
    {
        if ( transactionData == null )
        {
            return DatabaseUpdates.EMPTY;
        }
        return dbmsModel.updatedDatabases( transactionData );
    }

    private boolean mayDatabaseBeRetriedWithPriority( NamedDatabaseId namedDatabaseId )
    {
        var state = databaseStateService.stateOfDatabase( namedDatabaseId );
        return (state != null) && (state != DIRTY) && databaseStateService.causeOfFailure( namedDatabaseId ).isPresent();
    }

    private void offerReconciledTransactionId( long txId, boolean asPartOfStoreCopy )
    {
        try
        {
            if ( asPartOfStoreCopy )
            {
                reconciledTxTracker.enable( txId );
            }
            else
            {
                reconciledTxTracker.offerReconciledTransactionId( txId );
            }
        }
        catch ( Throwable t )
        {
            log.error( "Failed to update the last reconciled transaction ID", t );
        }
    }
}
