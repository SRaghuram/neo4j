/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.tools.dump;

import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.time.Clock;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.neo4j.configuration.Config;
import org.neo4j.internal.helpers.Args;
import org.neo4j.internal.nativeimpl.AbsentNativeAccess;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.kernel.database.DatabaseTracers;
import org.neo4j.kernel.impl.transaction.log.LogEntryCursor;
import org.neo4j.kernel.impl.transaction.log.LogPosition;
import org.neo4j.kernel.impl.transaction.log.ReadAheadLogChannel;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntry;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryDetachedCheckpoint;
import org.neo4j.kernel.impl.transaction.log.entry.VersionAwareLogEntryReader;
import org.neo4j.kernel.impl.transaction.log.files.RangeLogVersionVisitor;
import org.neo4j.kernel.impl.transaction.log.files.TransactionLogFilesContext;
import org.neo4j.kernel.impl.transaction.log.files.TransactionLogFilesHelper;
import org.neo4j.kernel.impl.transaction.log.files.checkpoint.CheckpointLogChannelAllocator;
import org.neo4j.logging.NullLog;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.memory.EmptyMemoryTracker;
import org.neo4j.monitoring.DatabaseHealth;
import org.neo4j.monitoring.Monitors;
import org.neo4j.monitoring.PanicEventGenerator;

import static org.apache.commons.compress.utils.FileNameUtils.getExtension;
import static org.neo4j.kernel.impl.transaction.log.LogVersionBridge.NO_MORE_CHANNELS;
import static org.neo4j.kernel.impl.transaction.log.entry.CheckpointLogVersionSelector.INSTANCE;
import static org.neo4j.kernel.impl.transaction.log.files.TransactionLogFilesHelper.CHECKPOINT_FILE_PREFIX;
import static org.neo4j.storageengine.api.CommandReaderFactory.NO_COMMANDS;

public class CheckpointLogDump
{
    public static void main( String[] args ) throws IOException
    {
        var parsedArgs = Args.parse( args );
        validateArguments( parsedArgs );
        var checkpointLogs = getCheckpointLogsFile( parsedArgs );
        dumpCheckpoints( checkpointLogs, System.out );
    }

    static void dumpCheckpoints( File checkpointLogs, PrintStream printStream ) throws IOException
    {
        var logFolder = checkpointLogs.isFile() ? checkpointLogs.getParentFile() : checkpointLogs;

        printStream.println( "Dump all checkpoint log files in " + checkpointLogs + "." );
        var fileSystem = new DefaultFileSystemAbstraction();
        var context = createLogFileContext( fileSystem );
        var fileHelper = new TransactionLogFilesHelper( fileSystem, logFolder.toPath(), CHECKPOINT_FILE_PREFIX );
        var channelAllocator = new CheckpointLogChannelAllocator( context, fileHelper );
        var versionVisitor = new RangeLogVersionVisitor();
        fileHelper.accept( versionVisitor );
        long highestVersion = versionVisitor.getHighestVersion();
        if ( highestVersion < 0 )
        {
            printStream.println( "Checkpoint logs files not found." );
            return ;
        }

        long lowestVersion = versionVisitor.getLowestVersion();
        long currentVersion = highestVersion;

        var checkpointReader = new VersionAwareLogEntryReader( NO_COMMANDS, INSTANCE, true );
        while ( currentVersion >= lowestVersion )
        {
            if ( checkpointLogs.isFile() && !getExtension( checkpointLogs.getName() ).equals( "" + currentVersion ) )
            {
                currentVersion--;
                continue;
            }
            try ( var channel = channelAllocator.openLogChannel( currentVersion );
                    var reader = new ReadAheadLogChannel( channel, NO_MORE_CHANNELS, EmptyMemoryTracker.INSTANCE );
                    var logEntryCursor = new LogEntryCursor( checkpointReader, reader ) )
            {
                while ( logEntryCursor.next() )
                {
                    LogEntry logEntry = logEntryCursor.get();
                    if ( logEntry instanceof LogEntryDetachedCheckpoint )
                    {
                        printStream.println( logEntry );
                    }
                    else
                    {
                        throw new IllegalStateException( "Checkpoint log files should contain only checkpoint records. Unexpected record: " + logEntry );
                    }
                }
                currentVersion--;
            }
        }
    }

    private static TransactionLogFilesContext createLogFileContext( DefaultFileSystemAbstraction fileSystem )
    {
        return new TransactionLogFilesContext( new AtomicLong( 0 ), new AtomicBoolean(), null,
                () -> -1, () -> -1, () -> LogPosition.UNSPECIFIED, null,
                fileSystem, NullLogProvider.getInstance(), DatabaseTracers.EMPTY, null, new AbsentNativeAccess(), EmptyMemoryTracker.INSTANCE, new Monitors(),
                false, new DatabaseHealth( PanicEventGenerator.NO_OP, NullLog.getInstance() ), false,
                Clock.systemUTC(), Config.defaults() );
    }

    private static File getCheckpointLogsFile( Args args )
    {
        return new File( args.orphans().get( 0 ) );
    }

    private static void validateArguments( Args parsedArgs )
    {
        var fileList = parsedArgs.orphans();
        if ( fileList == null || fileList.size() != 1 )
        {
            printUsage();
            System.exit( 1 );
        }
    }

    private static void printUsage()
    {
        System.out.println( "Please provide single argument with path to a directory with checkpoint logs or to a individual checkpoint log file." );
    }
}
