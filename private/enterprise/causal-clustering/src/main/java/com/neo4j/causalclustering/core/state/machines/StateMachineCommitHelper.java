/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.state.machines;

import com.neo4j.dbms.ReplicatedDatabaseEventService.ReplicatedDatabaseEventDispatch;

import java.util.function.LongConsumer;

import org.neo4j.internal.kernel.api.exceptions.TransactionFailureException;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.io.pagecache.tracing.cursor.context.VersionContextSupplier;
import org.neo4j.kernel.impl.api.TransactionCommitProcess;
import org.neo4j.kernel.impl.api.TransactionToApply;
import org.neo4j.kernel.impl.transaction.TransactionRepresentation;
import org.neo4j.kernel.impl.transaction.tracing.CommitEvent;
import org.neo4j.storageengine.api.TransactionApplicationMode;

import static java.lang.String.format;

public class StateMachineCommitHelper
{
    private static final String STATE_MACHINE_COMMIT_HELPER_TAG = "stateMachineCommitHelper";
    private static final LongConsumer NO_OP_COMMIT_CALLBACK = ignore ->
    {
    };

    private final CommandIndexTracker commandIndexTracker;
    private final VersionContextSupplier versionContextSupplier;
    private final ReplicatedDatabaseEventDispatch databaseEventDispatch;
    private final PageCacheTracer pageCacheTracer;

    public StateMachineCommitHelper( CommandIndexTracker commandIndexTracker,
            VersionContextSupplier versionContextSupplier, ReplicatedDatabaseEventDispatch databaseEventDispatch,
            PageCacheTracer pageCacheTracer )
    {
        this.commandIndexTracker = commandIndexTracker;
        this.versionContextSupplier = versionContextSupplier;
        this.databaseEventDispatch = databaseEventDispatch;
        this.pageCacheTracer = pageCacheTracer;
    }

    public void updateLastAppliedCommandIndex( long commandIndex )
    {
        commandIndexTracker.setAppliedCommandIndex( commandIndex );
    }

    public void commit( TransactionCommitProcess commitProcess, TransactionRepresentation tx, long commandIndex ) throws TransactionFailureException
    {
        var txToApply = newTransactionToApply( tx, commandIndex, NO_OP_COMMIT_CALLBACK );
        commit( commitProcess, txToApply );
    }

    public void commit( TransactionCommitProcess commitProcess, TransactionToApply txToApply ) throws TransactionFailureException
    {
        commitProcess.commit( txToApply, CommitEvent.NULL, TransactionApplicationMode.EXTERNAL );
    }

    public TransactionToApply newTransactionToApply( TransactionRepresentation txRepresentation, long commandIndex, LongConsumer txCommittedCallback )
    {
        var cursorTracer = pageCacheTracer.createPageCursorTracer( STATE_MACHINE_COMMIT_HELPER_TAG );
        var txToApply = new TransactionToApply( txRepresentation, versionContextSupplier, cursorTracer );
        txToApply.onClose( committedTxId ->
        {
            try
            {
                var latestCommittedTxIdWhenStarted = txRepresentation.getLatestCommittedTxWhenStarted();
                if ( latestCommittedTxIdWhenStarted >= committedTxId )
                {
                    throw new IllegalStateException(
                            format( "Out of order transaction. Expected that %d < %d", latestCommittedTxIdWhenStarted, committedTxId ) );
                }

                txCommittedCallback.accept( committedTxId );
                databaseEventDispatch.fireTransactionCommitted( committedTxId );
                updateLastAppliedCommandIndex( commandIndex );
            }
            finally
            {
                cursorTracer.close();
            }
        } );
        return txToApply;
    }
}
