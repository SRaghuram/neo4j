/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.dbms;

import java.util.Collection;
import java.util.Map;

import org.neo4j.bolt.txtracking.ReconciledTransactionTracker;
import org.neo4j.graphdb.event.TransactionData;
import org.neo4j.kernel.database.DatabaseId;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

import static com.neo4j.dbms.EnterpriseOperatorState.STARTED;
import static java.util.Collections.emptySet;
import static org.neo4j.kernel.database.DatabaseIdRepository.SYSTEM_DATABASE_ID;

/**
 * Operator driving database management operations in response to changes in the system database
 */
class SystemGraphDbmsOperator extends DbmsOperator
{
    private final EnterpriseSystemGraphDbmsModel dbmsModel;
    private final ReconciledTransactionTracker reconciledTxTracker;
    private final Log log;

    SystemGraphDbmsOperator( EnterpriseSystemGraphDbmsModel dbmsModel,
                             ReconciledTransactionTracker reconciledTxTracker, LogProvider logProvider )
    {
        this.dbmsModel = dbmsModel;
        this.reconciledTxTracker = reconciledTxTracker;
        this.desired.put( SYSTEM_DATABASE_ID.name(), new EnterpriseDatabaseState( SYSTEM_DATABASE_ID, STARTED ) );
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
        var databasesToAwait = extractUpdatedDatabases( transactionData );

        updateDesiredStates(); // TODO: Handle exceptions from this!
        ReconcilerResult reconcilerResult = trigger( ReconcilerRequest.simple() );
        reconcilerResult.whenComplete( () -> updateLastReconciledTransactionId( txId, asPartOfStoreCopy ) );

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

    private Collection<DatabaseId> extractUpdatedDatabases( TransactionData transactionData )
    {
        if ( transactionData == null )
        {
            return emptySet();
        }
        return dbmsModel.updatedDatabases( transactionData );
    }

    private void updateLastReconciledTransactionId( long txId, boolean asPartOfStoreCopy )
    {
        try
        {
            if ( asPartOfStoreCopy )
            {
                reconciledTxTracker.initialize( txId );
            }
            else
            {
                reconciledTxTracker.setLastReconciledTransactionId( txId );
            }
        }
        catch ( Throwable t )
        {
            log.error( "Failed to update the last reconciled transaction ID", t );
        }
    }
}
