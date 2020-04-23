/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.tools.util;

import java.io.File;
import java.io.IOException;

import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.StoreChannel;
import org.neo4j.kernel.impl.transaction.log.LogEntryCursor;
import org.neo4j.kernel.impl.transaction.log.LogVersionBridge;
import org.neo4j.kernel.impl.transaction.log.LogVersionedStoreChannel;
import org.neo4j.kernel.impl.transaction.log.PhysicalLogVersionedStoreChannel;
import org.neo4j.kernel.impl.transaction.log.ReadAheadLogChannel;
import org.neo4j.kernel.impl.transaction.log.ReadableLogChannel;
import org.neo4j.kernel.impl.transaction.log.ReaderLogVersionBridge;
import org.neo4j.kernel.impl.transaction.log.entry.LogHeader;
import org.neo4j.kernel.impl.transaction.log.entry.VersionAwareLogEntryReader;
import org.neo4j.kernel.impl.transaction.log.files.ChannelNativeAccessor;
import org.neo4j.kernel.impl.transaction.log.files.LogFiles;
import org.neo4j.storageengine.api.CommandReaderFactory;

import static java.util.Objects.requireNonNull;
import static org.neo4j.io.memory.ByteBuffers.allocate;
import static org.neo4j.kernel.impl.transaction.log.entry.LogHeaderReader.readLogHeader;
import static org.neo4j.kernel.impl.transaction.log.entry.LogVersions.CURRENT_FORMAT_LOG_HEADER_SIZE;
import static org.neo4j.memory.EmptyMemoryTracker.INSTANCE;

public class TransactionLogUtils
{
    /**
     * Opens a {@link LogEntryCursor} over all log files found in the storeDirectory
     *
     * @param fs {@link FileSystemAbstraction} to find {@code storeDirectory} in.
     * @param logFiles the physical log files to read from
     */
    public static LogEntryCursor openLogs( final FileSystemAbstraction fs, LogFiles logFiles, CommandReaderFactory commandReaderFactory )
            throws IOException
    {
        File firstFile = logFiles.getLogFileForVersion( logFiles.getLowestLogVersion() );
        return openLogEntryCursor( fs, firstFile, new ReaderLogVersionBridge( logFiles ), logFiles.getChannelNativeAccessor(), commandReaderFactory );
    }

    /**
     * Opens a {@link LogEntryCursor} for requested file
     *
     * @param fileSystem to find {@code file} in.
     * @param file file to open
     * @param readerLogVersionBridge log version bridge to use
     */
    public static LogEntryCursor openLogEntryCursor( FileSystemAbstraction fileSystem, File file,
            LogVersionBridge readerLogVersionBridge, ChannelNativeAccessor nativeAccessor, CommandReaderFactory commandReaderFactory ) throws IOException
    {
        LogVersionedStoreChannel channel = openVersionedChannel( fileSystem, file, nativeAccessor );
        ReadableLogChannel logChannel = new ReadAheadLogChannel( channel, readerLogVersionBridge, INSTANCE );
        return new LogEntryCursor( new VersionAwareLogEntryReader( commandReaderFactory ), logChannel );
    }

    /**
     * Opens a file in given {@code fileSystem} as a {@link LogVersionedStoreChannel}.
     *
     * @param fileSystem {@link FileSystemAbstraction} containing the file to open.
     * @param file file to open as a channel.
     * @return {@link LogVersionedStoreChannel} for the file. Its version is determined by its log header.
     * @throws IOException on I/O error.
     */
    public static PhysicalLogVersionedStoreChannel openVersionedChannel( FileSystemAbstraction fileSystem, File file,
            ChannelNativeAccessor nativeAccessor ) throws IOException
    {
        StoreChannel fileChannel = fileSystem.read( file );
        LogHeader logHeader = readLogHeader( allocate( CURRENT_FORMAT_LOG_HEADER_SIZE ), fileChannel, true, file );
        requireNonNull( logHeader, "There is no log header in log file '" + file + "', so it is likely a pre-allocated empty log file." );
        return new PhysicalLogVersionedStoreChannel( fileChannel, logHeader.getLogVersion(), logHeader.getLogFormatVersion(), file, nativeAccessor );
    }
}
