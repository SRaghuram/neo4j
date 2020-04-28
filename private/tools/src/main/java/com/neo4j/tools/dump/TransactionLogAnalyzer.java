/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.tools.dump;

import com.neo4j.tools.dump.log.TransactionLogEntryCursor;
import com.neo4j.tools.util.TransactionLogUtils;

import java.io.File;
import java.io.IOException;

import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.impl.transaction.log.LogEntryCursor;
import org.neo4j.kernel.impl.transaction.log.LogPosition;
import org.neo4j.kernel.impl.transaction.log.LogPositionMarker;
import org.neo4j.kernel.impl.transaction.log.LogVersionBridge;
import org.neo4j.kernel.impl.transaction.log.LogVersionedStoreChannel;
import org.neo4j.kernel.impl.transaction.log.ReadAheadLogChannel;
import org.neo4j.kernel.impl.transaction.log.ReaderLogVersionBridge;
import org.neo4j.kernel.impl.transaction.log.entry.CheckPoint;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntry;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryCommand;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryCommit;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryReader;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryStart;
import org.neo4j.kernel.impl.transaction.log.entry.VersionAwareLogEntryReader;
import org.neo4j.kernel.impl.transaction.log.files.LogFiles;
import org.neo4j.kernel.impl.transaction.log.files.LogFilesBuilder;
import org.neo4j.storageengine.api.CommandReaderFactory;
import org.neo4j.memory.EmptyMemoryTracker;
import org.neo4j.storageengine.api.StorageEngineFactory;

import static java.lang.String.format;
import static org.neo4j.kernel.impl.transaction.log.LogVersionBridge.NO_MORE_CHANNELS;

/**
 * Merely a utility which, given a store directory or log file, reads the transaction log(s) as a stream of transactions
 * and invokes methods on {@link Monitor}.
 */
public class TransactionLogAnalyzer
{
    /**
     * Receiving call-backs for all kinds of different events while analyzing the stream of transactions.
     */
    public interface Monitor
    {
        /**
         * Called when transitioning to a new log file, crossing a log version bridge. This is also called for the
         * first log file opened.
         *
         * @param file {@link File} pointing to the opened log file.
         * @param logVersion log version.
         */
        default void logFile( File file, long logVersion ) throws IOException
        {   // no-op by default
        }

        default void endLogFile()
        {
        }

        /**
         * A complete transaction with {@link LogEntryStart}, one or more {@link LogEntryCommand} and {@link LogEntryCommit}.
         *
         * @param transactionEntries the log entries making up the transaction, including start/commit entries.
         */
        default void transaction( LogEntry[] transactionEntries )
        {   // no-op by default
        }

        /**
         * {@link CheckPoint} log entry in between transactions.
         *
         * @param checkpoint the {@link CheckPoint} log entry.
         * @param checkpointEntryPosition {@link LogPosition} of the checkpoint entry itself.
         */
        default void checkpoint( CheckPoint checkpoint, LogPosition checkpointEntryPosition )
        {   // no-op by default
        }
    }

    public static Monitor all( Monitor... monitors )
    {
        return new CombinedMonitor( monitors );
    }

    public static void analyze( FileSystemAbstraction fileSystem, File storeDirOrLogFile, Monitor monitor ) throws IOException
    {
        analyze( fileSystem, storeDirOrLogFile, StorageEngineFactory.selectStorageEngine().commandReaderFactory(), monitor );
    }

    /**
     * Analyzes transactions found in log file(s) specified by {@code storeDirOrLogFile} calling methods on the supplied
     * {@link Monitor} for each encountered data item.
     *
     * @param fileSystem {@link FileSystemAbstraction} to find the files on.
     * @param storeDirOrLogFile {@link File} pointing either to a directory containing transaction log files, or directly
     * pointing to a single transaction log file to analyze.
     * @param commandReaderFactory {@link CommandReaderFactory} to use.
     * @param monitor {@link Monitor} receiving call-backs for all {@link Monitor#transaction(LogEntry[]) transactions},
     * {@link Monitor#checkpoint(CheckPoint, LogPosition) checkpoints} and {@link Monitor#logFile(File, long) log file transitions}
     * encountered during the analysis.
     * @throws IOException on I/O error.
     */
    public static void analyze( FileSystemAbstraction fileSystem, File storeDirOrLogFile, CommandReaderFactory commandReaderFactory,
            Monitor monitor ) throws IOException
    {
        File firstFile;
        LogVersionBridge bridge;
        ReadAheadLogChannel channel;
        LogEntryReader entryReader;
        LogPositionMarker positionMarker;
        LogFiles logFiles;
        if ( storeDirOrLogFile.isDirectory() )
        {
            // Use natural log version bridging if a directory is supplied
            logFiles = LogFilesBuilder.logFilesBasedOnlyBuilder( storeDirOrLogFile, fileSystem )
                    .withCommandReaderFactory( commandReaderFactory )
                    .build();
            bridge = new ReaderLogVersionBridge( logFiles )
            {
                @Override
                public LogVersionedStoreChannel next( LogVersionedStoreChannel channel ) throws IOException
                {
                    LogVersionedStoreChannel next = super.next( channel );
                    if ( next != channel )
                    {
                        monitor.endLogFile();
                        monitor.logFile( logFiles.getLogFileForVersion( next.getVersion() ), next.getVersion() );
                    }
                    return next;
                }
            };
            long lowestLogVersion = logFiles.getLowestLogVersion();
            if ( lowestLogVersion < 0 )
            {
                throw new IllegalStateException( format( "Transaction logs at '%s' not found.", storeDirOrLogFile ) );
            }
            firstFile = logFiles.getLogFileForVersion( lowestLogVersion );
            monitor.logFile( firstFile, lowestLogVersion );
        }
        else
        {
            // Use no bridging, simply reading this single log file if a file is supplied
            firstFile = storeDirOrLogFile;
            logFiles = LogFilesBuilder.logFilesBasedOnlyBuilder( storeDirOrLogFile.getParentFile(), fileSystem )
                    .withCommandReaderFactory( commandReaderFactory )
                    .build();
            monitor.logFile( firstFile, logFiles.getLogVersion( firstFile ) );
            bridge = NO_MORE_CHANNELS;
        }

        channel = new ReadAheadLogChannel( TransactionLogUtils.openVersionedChannel( fileSystem, firstFile, logFiles.getChannelNativeAccessor() ), bridge,
                EmptyMemoryTracker.INSTANCE );
        StorageEngineFactory storageEngineFactory = StorageEngineFactory.selectStorageEngine();
        entryReader = new VersionAwareLogEntryReader( storageEngineFactory.commandReaderFactory() );
        positionMarker = new LogPositionMarker();
        try ( TransactionLogEntryCursor cursor = new TransactionLogEntryCursor( new LogEntryCursor( entryReader, channel ) ) )
        {
            channel.getCurrentPosition( positionMarker );
            while ( cursor.next() )
            {
                LogEntry[] tx = cursor.get();
                if ( tx.length == 1 && tx[0] instanceof CheckPoint )
                {
                    monitor.checkpoint( (CheckPoint) tx[0], positionMarker.newPosition() );
                }
                else
                {
                    monitor.transaction( tx );
                }
            }
        }
        monitor.endLogFile();
    }

    private static class CombinedMonitor implements Monitor
    {
        private final Monitor[] monitors;

        CombinedMonitor( Monitor[] monitors )
        {
            this.monitors = monitors;
        }

        @Override
        public void logFile( File file, long logVersion ) throws IOException
        {
            for ( Monitor monitor : monitors )
            {
                monitor.logFile( file, logVersion );
            }
        }

        @Override
        public void transaction( LogEntry[] transactionEntries )
        {
            for ( Monitor monitor : monitors )
            {
                monitor.transaction( transactionEntries );
            }
        }

        @Override
        public void checkpoint( CheckPoint checkpoint, LogPosition checkpointEntryPosition )
        {
            for ( Monitor monitor : monitors )
            {
                monitor.checkpoint( checkpoint, checkpointEntryPosition );
            }
        }
    }
}
