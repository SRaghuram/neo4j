/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.catchup.tx;

import java.io.IOException;
import java.io.UncheckedIOException;

import org.neo4j.collection.Dependencies;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.dbms.database.DatabasePageCache;
import org.neo4j.internal.helpers.collection.LongRange;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.io.pagecache.tracing.cursor.context.EmptyVersionContextSupplier;
import org.neo4j.kernel.impl.transaction.CommittedTransactionRepresentation;
import org.neo4j.kernel.impl.transaction.log.FlushablePositionAwareChecksumChannel;
import org.neo4j.kernel.impl.transaction.log.LogPosition;
import org.neo4j.kernel.impl.transaction.log.LogPositionMarker;
import org.neo4j.kernel.impl.transaction.log.TransactionLogWriter;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryWriter;
import org.neo4j.kernel.impl.transaction.log.files.LogFiles;
import org.neo4j.kernel.impl.transaction.log.files.LogFilesBuilder;
import org.neo4j.kernel.impl.transaction.log.files.TransactionLogFilesHelper;
import org.neo4j.kernel.lifecycle.Lifespan;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;
import org.neo4j.storageengine.api.StorageEngineFactory;
import org.neo4j.storageengine.api.TransactionId;
import org.neo4j.storageengine.api.TransactionMetaDataStore;

import static java.lang.String.format;
import static java.lang.System.currentTimeMillis;
import static org.neo4j.configuration.GraphDatabaseSettings.logical_log_rotation_threshold;
import static org.neo4j.configuration.GraphDatabaseSettings.preallocate_logical_logs;
import static org.neo4j.configuration.GraphDatabaseSettings.transaction_logs_root_path;
import static org.neo4j.kernel.impl.transaction.log.entry.LogHeaderReader.readLogHeader;
import static org.neo4j.kernel.impl.transaction.log.entry.LogVersions.CURRENT_FORMAT_LOG_HEADER_SIZE;

public class TransactionLogCatchUpWriter implements TxPullResponseListener, AutoCloseable
{
    private static final String TRANSACTION_LOG_CATCHUP_TAG = "transactionLogCatchup";
    private final Lifespan lifespan = new Lifespan();
    private final Log log;
    private final boolean asPartOfStoreCopy;
    private final TransactionLogWriter writer;
    private final LogFiles logFiles;
    private final TransactionMetaDataStore metaDataStore;
    private final DatabasePageCache databasePageCache;
    private final boolean rotateTxLogWhilePulling;
    private final PageCacheTracer pageCacheTracer;
    private final LongRange validInitialTxId;
    private final FlushablePositionAwareChecksumChannel logChannel;
    private final LogPositionMarker logPositionMarker = new LogPositionMarker();

    private long lastTxId = -1;
    private long expectedTxId = -1;

    TransactionLogCatchUpWriter( DatabaseLayout databaseLayout, FileSystemAbstraction fs, PageCache pageCache, Config config, LogProvider logProvider,
            StorageEngineFactory storageEngineFactory, LongRange validInitialTxId, boolean asPartOfStoreCopy, boolean keepTxLogsInStoreDir,
            boolean rotateTxLogWhilePulling, PageCacheTracer pageCacheTracer ) throws IOException
    {
        this.log = logProvider.getLog( getClass() );
        this.asPartOfStoreCopy = asPartOfStoreCopy;
        this.rotateTxLogWhilePulling = rotateTxLogWhilePulling;
        this.pageCacheTracer = pageCacheTracer;
        final Config configWithoutSpecificStoreFormat = configWithoutSpecificStoreFormat( config );
        databasePageCache = new DatabasePageCache( pageCache, EmptyVersionContextSupplier.EMPTY );
        Dependencies dependencies = new Dependencies();
        dependencies.satisfyDependencies( databaseLayout, fs, databasePageCache, configWithoutSpecificStoreFormat );
        metaDataStore =
                storageEngineFactory.transactionMetaDataStore( fs, databaseLayout, configWithoutSpecificStoreFormat, databasePageCache, pageCacheTracer );
        LogPosition startPosition = getLastClosedTransactionPosition( databaseLayout, metaDataStore, fs );
        LogFilesBuilder logFilesBuilder = LogFilesBuilder
                .builder( databaseLayout, fs )
                .withDependencies( dependencies )
                .withLastCommittedTransactionIdSupplier( () -> validInitialTxId.from() - 1 )
                .withConfig( customisedConfig( config, keepTxLogsInStoreDir, rotateTxLogWhilePulling, asPartOfStoreCopy ) )
                .withLogVersionRepository( metaDataStore )
                .withTransactionIdStore( metaDataStore )
                .withStoreId( metaDataStore.getStoreId() )
                .withCommandReaderFactory( storageEngineFactory.commandReaderFactory() )
                .withLastClosedTransactionPositionSupplier( () -> startPosition );
        this.logFiles = logFilesBuilder.build();
        this.lifespan.add( logFiles );
        this.logChannel = logFiles.getLogFile().getWriter();
        this.writer = new TransactionLogWriter( new LogEntryWriter( logChannel ) );
        this.validInitialTxId = validInitialTxId;
    }

    private static LogPosition getLastClosedTransactionPosition( DatabaseLayout databaseLayout, TransactionMetaDataStore metaDataStore,
            FileSystemAbstraction fs ) throws IOException
    {
        var logFilesHelper = new TransactionLogFilesHelper( fs, databaseLayout.getTransactionLogsDirectory() );
        var logFile = logFilesHelper.getLogFileForVersion( metaDataStore.getCurrentLogVersion() );
        return fs.fileExists( logFile ) ? readLogHeader( fs, logFile ).getStartPosition() : new LogPosition( 0, CURRENT_FORMAT_LOG_HEADER_SIZE );
    }

    private static Config configWithoutSpecificStoreFormat( Config config )
    {
        return Config.newBuilder().fromConfig( config ).set( GraphDatabaseSettings.record_format, null ).build();
    }

    private static Config customisedConfig( Config original, boolean keepTxLogsInStoreDir, boolean rotateTxLogWhilePulling, boolean asPartOfStoreCopy )
    {
        Config.Builder builder = Config.newBuilder();
        if ( !keepTxLogsInStoreDir && original.isExplicitlySet( transaction_logs_root_path ) )
        {
            builder.set( transaction_logs_root_path, original.get( transaction_logs_root_path ) );
        }
        if ( rotateTxLogWhilePulling && original.isExplicitlySet( logical_log_rotation_threshold ) )
        {
            builder.set( logical_log_rotation_threshold, original.get( logical_log_rotation_threshold ) );
        }
        if ( asPartOfStoreCopy )
        {
            builder.set( preallocate_logical_logs, false );
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
        if ( rotateTxLogWhilePulling && logFiles.getLogFile().rotationNeeded() )
        {
            rotateTransactionLogs( logFiles );
        }

        validateReceivedTxId( receivedTxId );

        lastTxId = receivedTxId;
        expectedTxId++;

        try
        {
            logChannel.getCurrentPosition( logPositionMarker );
            writer.append( tx.getTransactionRepresentation(), lastTxId, tx.getStartEntry().getPreviousChecksum() );
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
            throw new UncheckedIOException( e );
        }
    }

    @Override
    public synchronized void close() throws IOException
    {
        var cursorTracer = pageCacheTracer.createPageCursorTracer( TRANSACTION_LOG_CATCHUP_TAG );
        if ( asPartOfStoreCopy )
        {
            /* A checkpoint which points to the beginning of all the log files, meaning that
            all the streamed transactions will be applied as part of recovery. */
            long logVersion = logFiles.getLowestLogVersion();
            LogPosition checkPointPosition = logFiles.extractHeader( logVersion ).getStartPosition();

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
                    lastCommittedTx.commitTimestamp(), checkPointPosition.getByteOffset(), logVersion, cursorTracer );
        }

        lifespan.close();

        if ( lastTxId != -1 )
        {
            metaDataStore.setLastCommittedAndClosedTransactionId( lastTxId,
                    0, currentTimeMillis(), // <-- we don't seem to care about these fields anymore
                    logPositionMarker.getByteOffset(), logPositionMarker.getLogVersion(), cursorTracer );
        }
        metaDataStore.close();
        cursorTracer.close();
        databasePageCache.close();
    }
}
