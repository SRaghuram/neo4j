/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.dbms;

import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;
import org.neo4j.util.concurrent.ArrayQueueOutOfOrderSequence;
import org.neo4j.util.concurrent.OutOfOrderSequence;

import static org.neo4j.util.Preconditions.checkArgument;
import static org.neo4j.util.Preconditions.checkState;
import static org.neo4j.util.Preconditions.requireNonNegative;

public class ReconciledTransactionIdTracker
{
    private static final long NO_RECONCILED_TRANSACTION_ID = -1;
    private static final int INITIAL_ARRAY_SIZE = 200;
    private static final long[] NO_METADATA = new long[0];

    private final ReadWriteLock initializationLock;
    private final Log log;

    private OutOfOrderSequence sequence;

    ReconciledTransactionIdTracker( LogProvider logProvider )
    {
        initializationLock = new ReentrantReadWriteLock();
        log = logProvider.getLog( getClass() );
    }

    void initialize( long reconciledTransactionId )
    {
        requireNonNegative( reconciledTransactionId );

        initializationLock.writeLock().lock();
        try
        {
            if ( sequence == null )
            {
                log.info( "Initializing with transaction ID %s", reconciledTransactionId );
            }
            else
            {
                log.info( "Re-initializing from %s to transaction ID %s", sequence, reconciledTransactionId );
            }
            sequence = new ArrayQueueOutOfOrderSequence( reconciledTransactionId, INITIAL_ARRAY_SIZE, NO_METADATA );
        }
        finally
        {
            initializationLock.writeLock().unlock();
        }
    }

    long getLastReconciledTransactionId()
    {
        initializationLock.readLock().lock();
        try
        {
            return sequence != null ? sequence.getHighestGapFreeNumber() : NO_RECONCILED_TRANSACTION_ID;
        }
        finally
        {
            initializationLock.readLock().unlock();
        }
    }

    void setLastReconciledTransactionId( long reconciledTransactionId )
    {
        requireNonNegative( reconciledTransactionId );

        initializationLock.readLock().lock();
        try
        {
            checkState( sequence != null, "Not initialized" );

            var currentLastReconciledTxId = getLastReconciledTransactionId();
            checkArgument( reconciledTransactionId > currentLastReconciledTxId,
                    "Received illegal transaction ID %s which is lower than the current transaction ID %s. Sequence: %s",
                    reconciledTransactionId, currentLastReconciledTxId, sequence );

            log.debug( "Updating %s with transaction ID %s", sequence, reconciledTransactionId );
            sequence.offer( reconciledTransactionId, NO_METADATA );
        }
        finally
        {
            initializationLock.readLock().unlock();
        }
    }
}
