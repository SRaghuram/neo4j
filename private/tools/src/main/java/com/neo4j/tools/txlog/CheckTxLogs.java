/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.tools.txlog;

import com.neo4j.tools.txlog.checktypes.CheckType;
import com.neo4j.tools.txlog.checktypes.CheckTypes;
import org.eclipse.collections.api.map.primitive.MutableLongLongMap;
import org.eclipse.collections.impl.map.mutable.primitive.LongLongHashMap;

import java.io.IOException;
import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.neo4j.internal.helpers.Args;
import org.neo4j.internal.recordstorage.Command;
import org.neo4j.internal.recordstorage.RecordStorageCommandReaderFactory;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.impl.store.record.AbstractBaseRecord;
import org.neo4j.kernel.impl.transaction.log.LogEntryCursor;
import org.neo4j.kernel.impl.transaction.log.LogPosition;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntry;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryCommand;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryCommit;
import org.neo4j.kernel.impl.transaction.log.files.LogFile;
import org.neo4j.kernel.impl.transaction.log.files.LogFiles;
import org.neo4j.kernel.impl.transaction.log.files.LogFilesBuilder;
import org.neo4j.kernel.impl.transaction.log.files.checkpoint.CheckpointInfo;
import org.neo4j.storageengine.api.StorageCommand;
import org.neo4j.storageengine.api.StorageEngineFactory;

import static com.neo4j.tools.util.TransactionLogUtils.openLogs;
import static org.neo4j.storageengine.api.TransactionIdStore.BASE_TX_ID;

/**
 * Tool that verifies consistency of transaction logs.
 *
 * Transaction log is considered consistent when every command's before state is the same as after state for
 * corresponding record in previously committed transaction.
 *
 * Tool expects a single argument - directory with transaction logs.
 * It then simply iterates over all commands in those logs, compares before state for current record with previously
 * seen after state and stores after state for current record, if before state is consistent.
 */
public class CheckTxLogs
{
    private static final String HELP_FLAG = "help";
    private static final String VALIDATE_CHECKPOINTS_FLAG = "validate-checkpoints";
    private static final String CHECKS = "checks";
    private static final String SEPARATOR = ",";

    private final PrintStream out;
    private final FileSystemAbstraction fs;

    public CheckTxLogs( PrintStream out, FileSystemAbstraction fs )
    {
        this.out = out;
        this.fs = fs;
    }

    public static void main( String[] args ) throws Exception
    {
        PrintStream out = System.out;
        Args arguments = Args.withFlags( HELP_FLAG, VALIDATE_CHECKPOINTS_FLAG ).parse( args );
        if ( arguments.getBoolean( HELP_FLAG ) )
        {
            printUsageAndExit( out );
        }

        boolean validateCheckPoints = arguments.getBoolean( VALIDATE_CHECKPOINTS_FLAG );
        List<CheckType<?,?>> checkTypes = parseChecks( arguments );
        Path dir = parseDir( out, arguments );

        boolean success;
        try ( FileSystemAbstraction fs = new DefaultFileSystemAbstraction() )
        {
            LogFiles logFiles = LogFilesBuilder.logFilesBasedOnlyBuilder( dir, fs )
                    .withCommandReaderFactory( RecordStorageCommandReaderFactory.INSTANCE )
                    .build();
            LogFile logFile = logFiles.getLogFile();
            int numberOfLogFilesFound = (int) (logFile.getHighestLogVersion() - logFile.getLowestLogVersion() + 1);
            out.println( "Found " + numberOfLogFilesFound + " log files to verify in " + dir.toAbsolutePath().normalize() );

            CheckTxLogs tool = new CheckTxLogs( out, fs );
            PrintingInconsistenciesHandler handler = new PrintingInconsistenciesHandler( out );
            success = tool.scan( logFiles, handler, checkTypes );

            if ( validateCheckPoints )
            {
                success &= tool.validateCheckPoints( logFiles, handler );
            }
        }

        if ( !success )
        {
            System.exit( 1 );
        }
    }

    @SuppressWarnings( "unused" ) // used by recovery-robustness
    public boolean checkAll( Path storeDirectory ) throws IOException
    {
        LogFiles logFiles = LogFilesBuilder.logFilesBasedOnlyBuilder( storeDirectory, fs )
                .withCommandReaderFactory( RecordStorageCommandReaderFactory.INSTANCE )
                .build();
        InconsistenciesHandler handler = new PrintingInconsistenciesHandler( out );
        boolean success = scan( logFiles, handler, CheckTypes.CHECK_TYPES );
        success &= validateCheckPoints( logFiles, handler );
        return success;
    }

    boolean validateCheckPoints( LogFiles logFiles, InconsistenciesHandler handler ) throws IOException
    {
        var logFile = logFiles.getLogFile();
        final long lowestLogVersion = logFile.getLowestLogVersion();
        final long highestLogVersion = logFile.getHighestLogVersion();
        boolean success = true;
        final MutableLongLongMap logFileSizes = new LongLongHashMap();
        for ( long i = lowestLogVersion; i <= highestLogVersion; i++ )
        {
            logFileSizes.put( i, fs.getFileSize( logFile.getLogFileForVersion( i ) ) );
        }

        List<CheckpointInfo> checkpointInfos = logFiles.getCheckpointFile().reachableCheckpoints();
        for ( CheckpointInfo checkpointInfo : checkpointInfos )
        {
            LogPosition logPosition = checkpointInfo.getTransactionLogPosition();
            if ( logPosition.getLogVersion() < lowestLogVersion )
            {
                continue;
            }
            long size = logFileSizes.getIfAbsent( logPosition.getLogVersion(), -1 );
            if ( logPosition.getByteOffset() < 0 || size < 0 || logPosition.getByteOffset() > size )
            {
                handler.reportInconsistentCheckPoint( checkpointInfo.getCheckpointEntryPosition().getLogVersion(), logPosition, size );
                success = false;
            }
        }
        return success;
    }

    LogEntryCursor openLogEntryCursor( LogFiles logFiles ) throws IOException
    {
        return openLogs( fs, logFiles, StorageEngineFactory.selectStorageEngine().commandReaderFactory() );
    }

    boolean scan( LogFiles logFiles, InconsistenciesHandler handler, List<CheckType<?,?>> checkTypes )
            throws IOException
    {
        boolean success = true;
        boolean checkTxIds = true;
        for ( CheckType<?,?> checkType : checkTypes )
        {
            success &= scan( logFiles, handler, checkType, checkTxIds );
            checkTxIds = false;
        }
        return success;
    }

    private static class CommandAndLogVersion
    {
        StorageCommand command;
        long logVersion;

        CommandAndLogVersion( StorageCommand command, long logVersion )
        {
            this.command = command;
            this.logVersion = logVersion;
        }
    }

    private <C extends Command, R extends AbstractBaseRecord> boolean scan( LogFiles logFiles,
            InconsistenciesHandler handler, CheckType<C,R> check, boolean checkTxIds ) throws IOException
    {
        out.println( "Checking logs for " + check.name() + " inconsistencies" );
        CommittedRecords<R> state = new CommittedRecords<>( check );

        List<CommandAndLogVersion> txCommands = new ArrayList<>();
        boolean validLogs = true;
        long commandsRead = 0;
        long lastSeenTxId = BASE_TX_ID;
        try ( LogEntryCursor logEntryCursor = openLogEntryCursor( logFiles ) )
        {
            while ( logEntryCursor.next() )
            {
                LogEntry entry = logEntryCursor.get();
                if ( entry instanceof LogEntryCommand )
                {
                    StorageCommand command = ((LogEntryCommand) entry).getCommand();
                    if ( check.commandClass().isInstance( command ) )
                    {
                        long logVersion = logEntryCursor.getCurrentLogVersion();
                        txCommands.add( new CommandAndLogVersion( command, logVersion ) );
                    }
                }
                else if ( entry instanceof LogEntryCommit )
                {
                    long txId = ((LogEntryCommit) entry).getTxId();
                    if ( checkTxIds )
                    {
                        validLogs &= checkNoDuplicatedTxsInTheLog( lastSeenTxId, txId, handler );
                        lastSeenTxId = txId;
                    }
                    for ( CommandAndLogVersion txCommand : txCommands )
                    {
                        validLogs &= checkAndHandleInconsistencies( txCommand, check, state, txId, handler );
                    }
                    txCommands.clear();
                }
                commandsRead++;
            }
        }
        out.println( "Processed " + commandsRead + " commands" );
        out.println( state );

        if ( !txCommands.isEmpty() )
        {
            out.println( "Found " + txCommands.size() + " uncommitted commands at the end." );
            for ( CommandAndLogVersion txCommand : txCommands )
            {
                validLogs &= checkAndHandleInconsistencies( txCommand, check, state, -1, handler );
            }
            txCommands.clear();
        }

        return validLogs;
    }

    private boolean checkNoDuplicatedTxsInTheLog( long lastTxId, long currentTxId, InconsistenciesHandler handler )
    {
        boolean isValid = lastTxId <= BASE_TX_ID || lastTxId + 1 == currentTxId;
        if ( !isValid )
        {
            handler.reportInconsistentTxIdSequence( lastTxId, currentTxId );
        }
        return isValid;
    }

    private <C extends Command, R extends AbstractBaseRecord> boolean checkAndHandleInconsistencies(
            CommandAndLogVersion txCommand, CheckType<C,R> check, CommittedRecords<R> state, long txId,
            InconsistenciesHandler handler )
    {
        C command = check.commandClass().cast( txCommand.command );

        R before = check.before( command );
        R after = check.after( command );

        assert before.getId() == after.getId();

        RecordInfo<R> lastSeen = state.get( after.getId() );

        boolean isValidRecord = (lastSeen == null) || check.equal( before, lastSeen.record() );
        if ( !isValidRecord )
        {
            handler.reportInconsistentCommand( lastSeen, new RecordInfo<>( before, txCommand.logVersion, txId ) );
        }

        state.put( after, txCommand.logVersion, txId );

        return isValidRecord;
    }

    private static List<CheckType<?,?>> parseChecks( Args arguments )
    {
        String checks = arguments.get( CHECKS );
        if ( checks == null )
        {
            return CheckTypes.CHECK_TYPES;
        }

        return Stream.of( checks.split( SEPARATOR ) ).map( CheckTypes::fromName ).collect( Collectors.toList() );
    }

    private static Path parseDir( PrintStream printStream, Args args )
    {
        if ( args.orphans().size() != 1 )
        {
            printUsageAndExit( printStream );
        }
        Path dir = Path.of( args.orphans().get( 0 ) );
        if ( !Files.isDirectory( dir ) )
        {
            printStream.println( "Invalid directory: '" + dir + "'" );
            printUsageAndExit( printStream );
        }
        return dir;
    }

    private static void printUsageAndExit( PrintStream out )
    {
        out.println( "Tool expects single argument - directory with tx logs" );
        out.println( "Usage:" );
        out.println( "\t./checkTxLogs [options] <directory>" );
        out.println( "Options:" );
        out.println( "\t--help\t\tprints this description" );
        out.println( "\t--checks='checkname[,...]'\t\tthe list of checks to perform. Checks available: " +
                CheckTypes.CHECK_TYPES.stream().map( CheckType::name )
                        .collect( Collectors.joining( SEPARATOR ) ) );
        System.exit( 1 );
    }
}
