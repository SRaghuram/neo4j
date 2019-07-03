/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.dbms;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

import org.neo4j.kernel.database.DatabaseId;
import org.neo4j.kernel.database.DatabaseIdRepository;

import static com.neo4j.dbms.OperatorState.DROPPED;
import static com.neo4j.dbms.OperatorState.STARTED;
import static com.neo4j.dbms.OperatorState.STOPPED;

/**
 * Operator driving database management operations in response to changes in the system database
 */
public final class SystemGraphDbmsOperator extends DbmsOperator
{
    private final SystemGraphDbmsModel dbmsModel;

    private long mostRecentlySeenTx;

    SystemGraphDbmsOperator( SystemGraphDbmsModel dbmsModel, DatabaseIdRepository databaseIdRepository )
    {
        this.dbmsModel = dbmsModel;
        this.desired.put( databaseIdRepository.systemDatabase(), STARTED );
    }

    void transactionCommitted( long txId, Collection<DatabaseId> databasesToAwait )
    {
        if ( txId <= mostRecentlySeenTx )
        {
            throw new IllegalArgumentException( "Transaction ID should be strictly monotonic." );
        }
        mostRecentlySeenTx = txId;

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
}
