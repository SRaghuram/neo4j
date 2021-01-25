/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.kernel.impl.transaction.log;

import java.io.IOException;

import org.neo4j.kernel.impl.api.TransactionToApply;
import org.neo4j.kernel.impl.transaction.TransactionRepresentation;
import org.neo4j.kernel.impl.transaction.log.files.LogFile;
import org.neo4j.kernel.impl.transaction.log.files.LogFiles;
import org.neo4j.kernel.impl.transaction.log.rotation.LogRotation;
import org.neo4j.kernel.impl.transaction.tracing.LogAppendEvent;
import org.neo4j.kernel.impl.transaction.tracing.SerializeTransactionEvent;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.monitoring.Health;
import org.neo4j.storageengine.api.TransactionIdStore;
import org.neo4j.util.VisibleForTesting;

import static org.neo4j.kernel.impl.api.TransactionToApply.TRANSACTION_ID_NOT_SPECIFIED;

/**
 * Concurrently appends transactions to the transaction log, while coordinating with the log rotation and forcing the
 * log file in batches for higher throughput in a concurrent scenario.
 */
public class BatchingTransactionAppender extends LifecycleAdapter implements TransactionAppender
{
    private final TransactionMetadataCache transactionMetadataCache;
    private final LogFile logFile;
    private final LogRotation logRotation;
    private final TransactionIdStore transactionIdStore;
    private final Health databaseHealth;

    private TransactionLogWriter transactionLogWriter;
    private int previousChecksum;

    public BatchingTransactionAppender( LogFiles logFiles, LogRotation logRotation, TransactionMetadataCache transactionMetadataCache,
            TransactionIdStore transactionIdStore, Health databaseHealth )
    {
        this.logFile = logFiles.getLogFile();
        this.logRotation = logRotation;
        this.transactionIdStore = transactionIdStore;
        this.databaseHealth = databaseHealth;
        this.transactionMetadataCache = transactionMetadataCache;
        this.previousChecksum = transactionIdStore.getLastCommittedTransaction().checksum();
    }

    @VisibleForTesting
    public BatchingTransactionAppender( LogFiles logFiles, LogRotation logRotation, TransactionMetadataCache transactionMetadataCache,
            TransactionIdStore transactionIdStore, Health databaseHealth, int previousChecksum )
    {
        this.logFile = logFiles.getLogFile();
        this.logRotation = logRotation;
        this.transactionIdStore = transactionIdStore;
        this.databaseHealth = databaseHealth;
        this.transactionMetadataCache = transactionMetadataCache;
        this.previousChecksum = previousChecksum;
    }

    @Override
    public void start()
    {
        this.transactionLogWriter = logFile.getTransactionLogWriter();
    }

    @Override
    public long append( TransactionToApply batch, LogAppendEvent logAppendEvent ) throws IOException
    {
        // Assigned base tx id just to make compiler happy
        long lastTransactionId = TransactionIdStore.BASE_TX_ID;
        // Synchronized with logFile to get absolute control over concurrent rotations happening
        synchronized ( logFile )
        {
            // Assert that kernel is healthy before making any changes
            databaseHealth.assertHealthy( IOException.class );
            try ( SerializeTransactionEvent serialiseEvent = logAppendEvent.beginSerializeTransaction() )
            {
                // Append all transactions in this batch to the log under the same logFile monitor
                TransactionToApply tx = batch;
                while ( tx != null )
                {
                    long transactionId = transactionIdStore.nextCommittingTransactionId();

                    // If we're in a scenario where we're merely replicating transactions, i.e. transaction
                    // id have already been generated by another entity we simply check that our id
                    // that we generated match that id. If it doesn't we've run into a problem we can't ´
                    // really recover from and would point to a bug somewhere.
                    matchAgainstExpectedTransactionIdIfAny( transactionId, tx );

                    TransactionCommitment commitment = appendToLog( tx.transactionRepresentation(), transactionId, logAppendEvent, previousChecksum );
                    previousChecksum = commitment.getTransactionChecksum();
                    tx.commitment( commitment, transactionId );
                    tx.logPosition( commitment.logPosition() );
                    tx = tx.next();
                    lastTransactionId = transactionId;
                }
            }
        }

        // At this point we've appended all transactions in this batch, but we can't mark any of them
        // as committed since they haven't been forced to disk yet. So here we force, or potentially
        // piggy-back on another force, but anyway after this call below we can be sure that all our transactions
        // in this batch exist durably on disk.
        if ( logFile.forceAfterAppend( logAppendEvent ) )
        {
            // We got lucky and were the one forcing the log. It's enough if ones of all doing concurrent committers
            // checks the need for log rotation.
            boolean logRotated = logRotation.rotateLogIfNeeded( logAppendEvent );
            logAppendEvent.setLogRotated( logRotated );
        }

        // Mark all transactions as committed
        publishAsCommitted( batch );

        return lastTransactionId;
    }

    private void matchAgainstExpectedTransactionIdIfAny( long transactionId, TransactionToApply tx )
    {
        long expectedTransactionId = tx.transactionId();
        if ( expectedTransactionId != TRANSACTION_ID_NOT_SPECIFIED )
        {
            if ( transactionId != expectedTransactionId )
            {
                IllegalStateException ex = new IllegalStateException(
                        "Received " + tx.transactionRepresentation() + " with txId:" + expectedTransactionId +
                                " to be applied, but appending it ended up generating an unexpected txId:" +
                                transactionId );
                databaseHealth.panic( ex );
                throw ex;
            }
        }
    }

    private static void publishAsCommitted( TransactionToApply batch )
    {
        while ( batch != null )
        {
            batch.publishAsCommitted();
            batch = batch.next();
        }
    }

    /**
     * @return A TransactionCommitment instance with metadata about the committed transaction, such as whether or not
     * this transaction contains any explicit index changes.
     */
    private TransactionCommitment appendToLog( TransactionRepresentation transaction, long transactionId, LogAppendEvent logAppendEvent, int previousChecksum )
            throws IOException
    {
        // The outcome of this try block is either of:
        // a) transaction successfully appended, at which point we return a Commitment to be used after force
        // b) transaction failed to be appended, at which point a kernel panic is issued
        // The reason that we issue a kernel panic on failure in here is that at this point we're still
        // holding the logFile monitor, and a failure to append needs to be communicated with potential
        // log rotation, which will wait for all transactions closed or fail on kernel panic.
        try
        {
            var logPositionBeforeCommit = transactionLogWriter.getCurrentPosition();
            int checksum = transactionLogWriter.append( transaction, transactionId, previousChecksum );
            var logPositionAfterCommit = transactionLogWriter.getCurrentPosition();
            logAppendEvent.appendToLogFile( logPositionBeforeCommit, logPositionAfterCommit );

            transactionMetadataCache.cacheTransactionMetadata( transactionId, logPositionBeforeCommit, checksum, transaction.getTimeCommitted() );

            return new TransactionCommitment( transactionId, checksum, transaction.getTimeCommitted(), logPositionAfterCommit, transactionIdStore );
        }
        catch ( final Throwable panic )
        {
            databaseHealth.panic( panic );
            throw panic;
        }
    }
}
