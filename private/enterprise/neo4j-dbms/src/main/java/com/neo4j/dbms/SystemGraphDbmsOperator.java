/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.dbms;

import java.util.Collection;
import java.util.Map;

import org.neo4j.graphdb.event.TransactionData;
import org.neo4j.kernel.database.DatabaseId;
import org.neo4j.kernel.database.DatabaseIdRepository;
import org.neo4j.kernel.impl.core.ThreadToStatementContextBridge;

import static com.neo4j.dbms.OperatorState.DROPPED;
import static com.neo4j.dbms.OperatorState.STARTED;
import static com.neo4j.dbms.OperatorState.STOPPED;
import static java.util.Collections.emptySet;

/**
 * Operator driving database management operations in response to changes in the system database
 */
public final class SystemGraphDbmsOperator extends DbmsOperator
{
    private final SystemGraphDbmsModel dbmsModel;
    private final ThreadToStatementContextBridge txBridge;

    private long mostRecentlySeenTx;

    SystemGraphDbmsOperator( SystemGraphDbmsModel dbmsModel, DatabaseIdRepository databaseIdRepository, ThreadToStatementContextBridge txBridge )
    {
        this.dbmsModel = dbmsModel;
        this.txBridge = txBridge;
        this.desired.put( databaseIdRepository.systemDatabase(), STARTED );
    }

    void transactionCommitted( long txId, TransactionData transactionData )
    {
        if ( txId <= mostRecentlySeenTx )
        {
            throw new IllegalArgumentException( "Transaction ID should be strictly monotonic." );
        }
        mostRecentlySeenTx = txId;

        if ( txBridge.hasTransaction() )
        {
            // Still in a transaction. This method was most likely invoked after a nested transaction was committed.
            // For example, such transaction could be a token-introducing internal transaction. No need to reconcile.
            return;
        }

        var databasesToAwait = extractUpdatedDatabases( transactionData );

        updateDesiredStates(); // TODO: Handle exceptions from this!
        Reconciliation reconciliation = trigger( false );

        // TODO: Remove below when Standalone tests( e.g.SystemDatabaseDatabaseManagementIT ) no longer depend on blocking behaviour of create.
        // Clustered version of this listener does *not * block
        reconciliation.await( databasesToAwait );
    }

    /**
     * Synchronized so that concurrent reads of the states from the database and the updates
     * of the desired states happen in order. Otherwise r(A), r(B) representing reads of database
     * states could be written into the the desired map as w(B), w(A) leaving the older states
     * in place.
     */
    synchronized void updateDesiredStates()
    {
        Map<DatabaseId,SystemGraphDbmsModel.DatabaseState> systemStates = dbmsModel.getDatabaseStates();
        systemStates.forEach( ( key, value ) -> desired.put( key, toOperatorState( value ) ) );
    }

    private OperatorState toOperatorState( SystemGraphDbmsModel.DatabaseState dbmsState )
    {
        switch ( dbmsState )
        {
        case ONLINE:
            return STARTED;
        case OFFLINE:
            return STOPPED;
        case DELETED:
            return DROPPED;
        default:
            throw new IllegalArgumentException( "Unsupported database state: " + dbmsState );
        }
    }

    private Collection<DatabaseId> extractUpdatedDatabases( TransactionData transactionData )
    {
        if ( transactionData == null )
        {
            return emptySet();
        }
        return dbmsModel.updatedDatabases( transactionData );
    }
}
