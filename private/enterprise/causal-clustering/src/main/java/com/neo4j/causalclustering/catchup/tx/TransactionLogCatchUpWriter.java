/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.catchup.tx;

import org.neo4j.helpers.collection.LongRange;

import java.io.IOException;
import java.util.Map;

import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.transaction.CommittedTransactionRepresentation;
import org.neo4j.kernel.impl.transaction.log.FlushablePositionAwareChannel;
import org.neo4j.kernel.impl.transaction.log.LogPosition;
import org.neo4j.kernel.impl.transaction.log.LogPositionMarker;
import org.neo4j.kernel.impl.transaction.log.TransactionLogWriter;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryWriter;
import org.neo4j.kernel.impl.transaction.log.files.LogFiles;
import org.neo4j.kernel.impl.transaction.log.files.LogFilesBuilder;
import org.neo4j.kernel.impl.util.Dependencies;
import org.neo4j.kernel.lifecycle.Lifespan;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;
import org.neo4j.storageengine.api.StorageEngineFactory;
import org.neo4j.storageengine.api.TransactionId;
import org.neo4j.storageengine.api.TransactionMetaDataStore;

import static java.lang.String.format;
import static java.lang.System.currentTimeMillis;
import static org.neo4j.graphdb.factory.GraphDatabaseSettings.logical_log_rotation_threshold;
import static org.neo4j.graphdb.factory.GraphDatabaseSettings.transaction_logs_root_path;
import static org.neo4j.kernel.impl.transaction.log.entry.LogHeader.LOG_HEADER_SIZE;

public class TransactionLogCatchUpWriter implements TxPullResponseListener, AutoCloseable
{
    private final Lifespan lifespan = new Lifespan();
    private final Log log;
    private final boolean asPartOfStoreCopy;
    private final TransactionLogWriter writer;
    private final LogFiles logFiles;
    private final TransactionMetaDataStore metaDataStore;
    private final boolean rotateTransactionsManually;
    private final LongRange validInitialTxId;
    private final FlushablePositionAwareChannel logChannel;
    private final LogPositionMarker logPositionMarker = new LogPositionMarker();

    private long lastTxId = -1;
    private long expectedTxId = -1;

    TransactionLogCatchUpWriter( DatabaseLayout databaseLayout, FileSystemAbstraction fs, PageCache pageCache, Config config, LogProvider logProvider,
            StorageEngineFactory storageEngineFactory, LongRange validInitialTxId, boolean asPartOfStoreCopy, boolean keepTxLogsInStoreDir,
            boolean forceTransactionRotations ) throws IOException
    {
        this.log = logProvider.getLog( getClass() );
        this.asPartOfStoreCopy = asPartOfStoreCopy;
        this.rotateTransactionsManually = forceTransactionRotations;
        Dependencies dependencies = new Dependencies();
        dependencies.satisfyDependencies( databaseLayout, fs, pageCache, configWithoutSpecificStoreFormat( config ) );
        metaDataStore = storageEngineFactory.transactionMetaDataStore( dependencies );
        LogFilesBuilder logFilesBuilder = LogFilesBuilder
                .builder( databaseLayout, fs ).withDependencies( dependencies ).withLastCommittedTransactionIdSupplier( () -> validInitialTxId.from() - 1 )
                .withConfig( customisedConfig( config, keepTxLogsInStoreDir, forceTransactionRotations ) )
                .withLogVersionRepository( metaDataStore )
                .withTransactionIdStore( metaDataStore );
        this.logFiles = logFilesBuilder.build();
        this.lifespan.add( logFiles );
        this.logChannel = logFiles.getLogFile().getWriter();
        this.writer = new TransactionLogWriter( new LogEntryWriter( logChannel ) );
        this.validInitialTxId = validInitialTxId;
    }

    private Config configWithoutSpecificStoreFormat( Config config )
    {
        Map<String,String> raw = config.getRaw();
        raw.remove( GraphDatabaseSettings.record_format.name() );
        return Config.defaults( raw );
    }

    private Config customisedConfig( Config original, boolean keepTxLogsInStoreDir, boolean forceTransactionRotations )
    {
        Config.Builder builder = Config.builder();
        if ( !keepTxLogsInStoreDir && original.isConfigured( transaction_logs_root_path ) )
        {
            builder.withSetting( transaction_logs_root_path, original.get( transaction_logs_root_path ).toString() );
        }
        if ( forceTransactionRotations && original.isConfigured( logical_log_rotation_threshold ) )
        {
            builder.withSetting( logical_log_rotation_threshold, original.get( logical_log_rotation_threshold ).toString() );
        }
        return builder.build();
    }

    @Override
    public synchronized void onTxReceived( TxPullResponse txPullResponse )
    {
        CommittedTransactionRepresentation tx = txPullResponse.tx();
        long receivedTxId = tx.getCommitEntry().getTxId();

        // neo4j admin backup clients pull transactions indefinitely and have no monitoring mechanism for tx log rotation
        // Other cases, ex. Read Replicas have an external mechanism that rotates independently of this process and don't need to
        // manually rotate while pulling
        if ( rotateTransactionsManually && logFiles.getLogFile().rotationNeeded() )
        {
            rotateTransactionLogs( logFiles );
        }

        validateReceivedTxId( receivedTxId );

        lastTxId = receivedTxId;
        expectedTxId++;

        try
        {
            logChannel.getCurrentPosition( logPositionMarker );
            writer.append( tx.getTransactionRepresentation(), lastTxId );
        }
        catch ( IOException e )
        {
            log.error( "Failed when appending to transaction log", e );
        }
    }

    private void validateReceivedTxId( long receivedTxId )
    {
        if ( isFirstTx() )
        {
            if ( validInitialTxId.isWithinRange( receivedTxId ) )
            {
                expectedTxId = receivedTxId;
            }
            else
            {
                throw new RuntimeException(
                        format( "Expected the first received txId to be within the range: %s but got: %d", validInitialTxId, receivedTxId ) );
            }
        }
        if ( receivedTxId != expectedTxId )
        {
            throw new RuntimeException( format( "Expected txId: %d but got: %d", expectedTxId, receivedTxId ) );
        }
    }

    private boolean isFirstTx()
    {
        return expectedTxId == -1;
    }

    public long lastTx()
    {
        return lastTxId;
    }

    private static void rotateTransactionLogs( LogFiles logFiles )
    {
        try
        {
            logFiles.getLogFile().rotate();
        }
        catch ( IOException e )
        {
            throw new RuntimeException( e );
        }
    }

    @Override
    public synchronized void close() throws IOException
    {
        if ( asPartOfStoreCopy )
        {
            /* A checkpoint which points to the beginning of all the log files, meaning that
            all the streamed transactions will be applied as part of recovery. */
            long logVersion = logFiles.getLowestLogVersion();
            LogPosition checkPointPosition = new LogPosition( logVersion, LOG_HEADER_SIZE );

            log.info( "Writing checkpoint as part of store copy: " + checkPointPosition );
            writer.checkPoint( checkPointPosition );

            // * comment copied from old StoreCopyClient *
            // since we just create new log and put checkpoint into it with offset equals to
            // LOG_HEADER_SIZE we need to update last transaction offset to be equal to this newly defined max
            // offset otherwise next checkpoint that use last transaction offset will be created for non
            // existing offset that is in most of the cases bigger than new log size.
            // Recovery will treat that as last checkpoint and will not try to recover store till new
            // last closed transaction offset will not overcome old one. Till that happens it will be
            // impossible for recovery process to restore the store
            TransactionId lastCommittedTx = metaDataStore.getLastCommittedTransaction();
            metaDataStore.setLastCommittedAndClosedTransactionId( lastCommittedTx.transactionId(), lastCommittedTx.checksum(),
                    lastCommittedTx.commitTimestamp(), checkPointPosition.getByteOffset(), logVersion );
        }

        lifespan.close();

        if ( lastTxId != -1 )
        {
            metaDataStore.setLastCommittedAndClosedTransactionId( lastTxId,
                    0, currentTimeMillis(), // <-- we don't seem to care about these fields anymore
                    logPositionMarker.getByteOffset(), logPositionMarker.getLogVersion() );
        }
        metaDataStore.close();
    }
}
