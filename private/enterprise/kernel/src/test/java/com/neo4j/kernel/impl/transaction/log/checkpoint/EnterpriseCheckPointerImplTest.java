/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.kernel.impl.transaction.log.checkpoint;

import com.neo4j.kernel.impl.enterprise.transaction.log.checkpoint.VolumetricCheckPointThreshold;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.io.fs.WritableChannel;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.kernel.KernelVersion;
import org.neo4j.kernel.impl.transaction.SimpleMetaDataProvider;
import org.neo4j.kernel.impl.transaction.TransactionRepresentation;
import org.neo4j.kernel.impl.transaction.log.LogPosition;
import org.neo4j.kernel.impl.transaction.log.LogPositionMarker;
import org.neo4j.kernel.impl.transaction.log.PhysicalTransactionRepresentation;
import org.neo4j.kernel.impl.transaction.log.TransactionLogWriter;
import org.neo4j.kernel.impl.transaction.log.checkpoint.CheckPointThreshold;
import org.neo4j.kernel.impl.transaction.log.checkpoint.CheckPointerImpl;
import org.neo4j.kernel.impl.transaction.log.checkpoint.CheckpointAppender;
import org.neo4j.kernel.impl.transaction.log.checkpoint.SimpleTriggerInfo;
import org.neo4j.kernel.impl.transaction.log.checkpoint.StoreCopyCheckPointMutex;
import org.neo4j.kernel.impl.transaction.log.files.LogFile;
import org.neo4j.kernel.impl.transaction.log.files.LogFiles;
import org.neo4j.kernel.impl.transaction.log.files.LogFilesBuilder;
import org.neo4j.kernel.impl.transaction.log.files.checkpoint.CheckpointFile;
import org.neo4j.kernel.impl.transaction.log.pruning.LogPruneStrategyFactory;
import org.neo4j.kernel.impl.transaction.log.pruning.LogPruning;
import org.neo4j.kernel.impl.transaction.log.pruning.LogPruningImpl;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.monitoring.DatabaseHealth;
import org.neo4j.storageengine.api.StorageCommand;
import org.neo4j.storageengine.api.StoreId;
import org.neo4j.storageengine.api.TransactionIdStore;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.time.SystemNanoClock;

import static java.util.Collections.singletonList;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.neo4j.internal.kernel.api.security.AuthSubject.AUTH_DISABLED;
import static org.neo4j.io.pagecache.IOLimiter.UNLIMITED;
import static org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer.NULL;
import static org.neo4j.kernel.database.DatabaseTracers.EMPTY;
import static org.neo4j.time.Clocks.nanoClock;

@TestDirectoryExtension
class EnterpriseCheckPointerImplTest
{
    @Inject
    private TestDirectory directory;

    @Test
    void shouldNotCheckpointOnVolumetricPolicyWithCheckPointInLaterVersion() throws IOException
    {
        /*
         * There was an issue where the volumetric checkpoint policy (which uses log pruning to check if check point can be made)
         * and the actual log pruning wouldn't use the same upper log version bound and so if no further write transaction was
         * committed the db would be stuck in a state of infinite checkpointing (or rather checkpoint on every check) because
         * it would look like it could do checkpoint, but log pruning would not prune any log.
         */

        // given a terribly annoying setup of the check pointer
        SimpleMetaDataProvider metaDataProvider = new SimpleMetaDataProvider();
        LogFiles logFiles = LogFilesBuilder.builder( DatabaseLayout.ofFlat( directory.homePath() ), directory.getFileSystem() )
                .withTransactionIdStore( metaDataProvider )
                .withLogVersionRepository( metaDataProvider )
                .withStoreId( new StoreId( 99 ) )
                .withKernelVersionProvider( () -> KernelVersion.V4_0 )
                .build();
        Config config = Config.defaults( GraphDatabaseSettings.keep_logical_logs, "3 files" );
        NullLogProvider logProvider = NullLogProvider.getInstance();
        LogPruning logPruning = new LogPruningImpl( directory.getFileSystem(), logFiles, logProvider, new LogPruneStrategyFactory(), nanoClock(), config );
        CheckPointThreshold threshold = new VolumetricCheckPointThreshold( logPruning );
        CheckPointerImpl.ForceOperation flusher = mock( CheckPointerImpl.ForceOperation.class );
        CheckpointFile checkpointFile = logFiles.getCheckpointFile();
        DatabaseHealth health = mock( DatabaseHealth.class );
        CheckpointAppender checkpointAppender = checkpointFile.getCheckpointAppender();
        SystemNanoClock clock = nanoClock();
        CheckPointerImpl checkPointer =
                new CheckPointerImpl( metaDataProvider, threshold, flusher, logPruning, checkpointAppender, health, logProvider, EMPTY, UNLIMITED,
                        new StoreCopyCheckPointMutex(), clock );
        LifeSupport life = new LifeSupport();
        life.add( logFiles );
        life.add( checkPointer );
        life.start();

        try
        {
            // and given this state of the transaction log stream:
            // log 0: transaction 2
            // log 1: transaction 3
            // log 2: transaction 4
            // log 3: checkpoint referencing transaction 4
            LogFile logFile = logFiles.getLogFile();
            TransactionLogWriter transactionLogWriter = logFile.getTransactionLogWriter();
            appendAndCloseTestTransaction( transactionLogWriter, metaDataProvider );
            logFile.rotate();
            appendAndCloseTestTransaction( transactionLogWriter, metaDataProvider );
            logFile.rotate();
            appendAndCloseTestTransaction( transactionLogWriter, metaDataProvider );
            logFile.rotate();
            // Write a legacy checkpoint to get the checkpoint in the transaction log files instead of the separate files.
            logFile.getTransactionLogWriter().legacyCheckPoint( logPositionOf( metaDataProvider.getLastClosedTransaction() ) );

            // when checking whether or not checkpoint is needed
            checkPointer.checkPointIfNeeded( new SimpleTriggerInfo( "Test trigger, should not actually trigger" ) );

            // then it should not be needed
            verifyZeroInteractions( flusher );

            // but when later committing another transaction
            appendAndCloseTestTransaction( transactionLogWriter, metaDataProvider );
            checkPointer.checkPointIfNeeded( new SimpleTriggerInfo( "Test trigger, this should trigger a checkpoint" ) );

            // then
            verify( flusher ).flushAndForce( any(), any() );
        }
        finally
        {
            life.shutdown();
        }
    }

    private static void appendAndCloseTestTransaction( TransactionLogWriter writer, TransactionIdStore transactionIdStore ) throws IOException
    {
        long txId = transactionIdStore.nextCommittingTransactionId();
        LogPositionMarker position = writer.getCurrentPosition( new LogPositionMarker() );
        writer.append( simpleTransaction(), txId, -1 );
        transactionIdStore.transactionClosed( txId, position.getLogVersion(), position.getByteOffset(), NULL );
    }

    private static LogPosition logPositionOf( long[] lastClosedTransaction )
    {
        return new LogPosition( lastClosedTransaction[1], lastClosedTransaction[2] );
    }

    private static TransactionRepresentation simpleTransaction()
    {
        PhysicalTransactionRepresentation tx = new PhysicalTransactionRepresentation( singletonList( testCommand() ) );
        tx.setHeader( new byte[0], -1, -1, -1, -1, AUTH_DISABLED );
        return tx;
    }

    private static StorageCommand testCommand()
    {
        return new StorageCommand()
        {
            @Override
            public void serialize( WritableChannel channel ) throws IOException
            {
                channel.putLong( 1 );
                channel.putLong( 2 );
                channel.putLong( 3 );
            }

            @Override
            public KernelVersion version()
            {
                return KernelVersion.LATEST;
            }
        };
    }
}
