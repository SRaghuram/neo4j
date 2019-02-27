/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.catchup.storecopy;

import java.io.IOException;
import java.util.Optional;

import org.neo4j.configuration.Config;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.impl.transaction.CommittedTransactionRepresentation;
import org.neo4j.kernel.impl.transaction.log.NoSuchTransactionException;
import org.neo4j.kernel.impl.transaction.log.ReadOnlyTransactionStore;
import org.neo4j.kernel.impl.transaction.log.TransactionCursor;
import org.neo4j.kernel.impl.transaction.log.files.LogFilesBuilder;
import org.neo4j.kernel.impl.util.Dependencies;
import org.neo4j.kernel.lifecycle.Lifespan;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.storageengine.api.StorageEngineFactory;
import org.neo4j.storageengine.api.TransactionIdStore;

import static org.neo4j.storageengine.api.TransactionIdStore.BASE_TX_ID;

public class CommitStateHelper
{
    private final StorageEngineFactory storageEngineFactory;
    private PageCache pageCache;
    private FileSystemAbstraction fs;
    private Config config;

    public CommitStateHelper( PageCache pageCache, FileSystemAbstraction fs, Config config, StorageEngineFactory storageEngineFactory )
    {
        this.pageCache = pageCache;
        this.fs = fs;
        this.config = config;
        this.storageEngineFactory = storageEngineFactory;
    }

    CommitState getStoreState( DatabaseLayout databaseLayout ) throws IOException
    {
        Dependencies dependencies = new Dependencies();
        dependencies.satisfyDependencies( pageCache, databaseLayout );
        TransactionIdStore txIdStore = storageEngineFactory.readOnlyTransactionIdStore( dependencies );
        long lastCommittedTxId = txIdStore.getLastCommittedTransactionId();

        Optional<Long> latestTransactionLogIndex = getLatestTransactionLogIndex( lastCommittedTxId, databaseLayout );

        //noinspection OptionalIsPresent
        if ( latestTransactionLogIndex.isPresent() )
        {
            return new CommitState( lastCommittedTxId, latestTransactionLogIndex.get() );
        }
        else
        {
            return new CommitState( lastCommittedTxId );
        }
    }

    private Optional<Long> getLatestTransactionLogIndex( long startTxId, DatabaseLayout databaseLayout ) throws IOException
    {
        if ( !hasTxLogs( databaseLayout ) )
        {
            return Optional.empty();
        }

        // this is not really a read-only store, because it will create an empty transaction log if there is none
        ReadOnlyTransactionStore txStore = new ReadOnlyTransactionStore( pageCache, fs, databaseLayout, config, new Monitors() );

        long lastTxId = BASE_TX_ID;
        try ( Lifespan ignored = new Lifespan( txStore ); TransactionCursor cursor = txStore.getTransactions( startTxId ) )
        {
            while ( cursor.next() )
            {
                CommittedTransactionRepresentation tx = cursor.get();
                lastTxId = tx.getCommitEntry().getTxId();
            }

            return Optional.of( lastTxId );
        }
        catch ( NoSuchTransactionException e )
        {
            return Optional.empty();
        }
    }

    public boolean hasTxLogs( DatabaseLayout databaseLayout ) throws IOException
    {
        return LogFilesBuilder.activeFilesBuilder( databaseLayout, fs, pageCache ).withConfig( config ).build().logFiles().length > 0;
    }
}
