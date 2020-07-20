/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.tools.dump;

import com.neo4j.tools.dump.TransactionLogAnalyzer.Monitor;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.file.Path;
import java.util.TimeZone;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.regex.Pattern;

import org.neo4j.internal.helpers.Args;
import org.neo4j.internal.recordstorage.Command;
import org.neo4j.internal.recordstorage.Command.NodeCommand;
import org.neo4j.internal.recordstorage.Command.PropertyCommand;
import org.neo4j.internal.recordstorage.Command.RelationshipCommand;
import org.neo4j.internal.recordstorage.Command.RelationshipGroupCommand;
import org.neo4j.internal.recordstorage.Command.SchemaRuleCommand;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.impl.transaction.log.LogPosition;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntry;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryCommand;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryCommit;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryInlinedCheckPoint;
import org.neo4j.kernel.impl.transaction.log.entry.LogHeader;
import org.neo4j.kernel.impl.transaction.log.entry.LogHeaderReader;
import org.neo4j.storageengine.api.StorageCommand;

import static java.util.TimeZone.getTimeZone;
import static org.neo4j.internal.helpers.Format.DEFAULT_TIME_ZONE;
import static org.neo4j.memory.EmptyMemoryTracker.INSTANCE;

/**
 * Tool to represent logical logs in readable format for further analysis.
 */
public class DumpLogicalLog
{
    private static final String TO_FILE = "tofile";
    private static final String TX_FILTER = "txfilter";
    private static final String CC_FILTER = "ccfilter";
    private static final String LENIENT = "lenient";

    private final FileSystemAbstraction fileSystem;

    public DumpLogicalLog( FileSystemAbstraction fileSystem )
    {
        this.fileSystem = fileSystem;
    }

    public void dump( String filenameOrDirectory, PrintStream out,
            Predicate<LogEntry[]> filter, Function<LogEntry,String> serializer ) throws IOException
    {
        TransactionLogAnalyzer.analyze( fileSystem, Path.of( filenameOrDirectory ), new Monitor()
        {
            private Path path;
            private LogEntryCommit firstTx;
            private LogEntryCommit lastTx;

            @Override
            public void logFile( Path path, long logVersion ) throws IOException
            {
                this.path = path;
                LogHeader logHeader = LogHeaderReader.readLogHeader( fileSystem, path, INSTANCE );
                out.println( "=== " + path.toAbsolutePath() + "[" + logHeader + "] ===" );
            }

            @Override
            public void endLogFile()
            {
                if ( lastTx != null )
                {
                    out.println( "=== END " + path.toAbsolutePath() + ", firstTx=" + firstTx + ", lastTx=" + lastTx + " ===" );
                    firstTx = null;
                    lastTx = null;
                }
            }

            @Override
            public void transaction( LogEntry[] transactionEntries )
            {
                lastTx = (LogEntryCommit) transactionEntries[transactionEntries.length - 1];
                if ( firstTx == null )
                {
                    firstTx = lastTx;
                }

                if ( filter == null || filter.test( transactionEntries ) )
                {
                    for ( LogEntry entry : transactionEntries )
                    {
                        out.println( serializer.apply( entry ) );
                    }
                }
            }

            @Override
            public void checkpoint( LogEntryInlinedCheckPoint checkpoint, LogPosition checkpointEntryPosition )
            {
                if ( filter == null || filter.test( new LogEntry[] {checkpoint} ) )
                {
                    out.println( serializer.apply( checkpoint ) );
                }
            }
        } );
    }

    private static class TransactionRegexCriteria implements Predicate<LogEntry[]>
    {
        private final Pattern pattern;
        private final TimeZone timeZone;

        TransactionRegexCriteria( String regex, TimeZone timeZone )
        {
            this.pattern = Pattern.compile( regex );
            this.timeZone = timeZone;
        }

        @Override
        public boolean test( LogEntry[] transaction )
        {
            for ( LogEntry entry : transaction )
            {
                if ( pattern.matcher( entry.toString( timeZone ) ).find() )
                {
                    return true;
                }
            }
            return false;
        }
    }

    public static class ConsistencyCheckOutputCriteria implements Predicate<LogEntry[]>, Function<LogEntry,String>
    {
        private final TimeZone timeZone;
        private final InconsistentRecords inconsistencies;

        public ConsistencyCheckOutputCriteria( String ccFile, TimeZone timeZone ) throws IOException
        {
            this.timeZone = timeZone;
            inconsistencies = new InconsistentRecords();
            new InconsistencyReportReader( inconsistencies ).read( new File( ccFile ) );
        }

        @Override
        public boolean test( LogEntry[] transaction )
        {
            for ( LogEntry logEntry : transaction )
            {
                if ( matches( logEntry ) )
                {
                    return true;
                }
            }
            return false;
        }

        private boolean matches( LogEntry logEntry )
        {
            if ( logEntry instanceof LogEntryCommand )
            {
                return matches( ((LogEntryCommand) logEntry).getCommand() );
            }
            return false;
        }

        private boolean matches( StorageCommand command )
        {
            InconsistentRecords.Type type = mapCommandToType( command );
            // For the time being we can assume BaseCommand here
            return type != null && inconsistencies.containsId( type, ((Command) command).getKey() );
        }

        private InconsistentRecords.Type mapCommandToType( StorageCommand command )
        {
            if ( command instanceof NodeCommand )
            {
                return InconsistentRecords.Type.NODE;
            }
            if ( command instanceof RelationshipCommand )
            {
                return InconsistentRecords.Type.RELATIONSHIP;
            }
            if ( command instanceof PropertyCommand )
            {
                return InconsistentRecords.Type.PROPERTY;
            }
            if ( command instanceof RelationshipGroupCommand )
            {
                return InconsistentRecords.Type.RELATIONSHIP_GROUP;
            }
            if ( command instanceof SchemaRuleCommand )
            {
                return InconsistentRecords.Type.SCHEMA_INDEX;
            }
            return null; // means ignore this command
        }

        @Override
        public String apply( LogEntry logEntry )
        {
            String result = logEntry.toString( timeZone );
            if ( matches( logEntry ) )
            {
                result += "  <----";
            }
            return result;
        }
    }

    /**
     * Usage: [--txfilter "regex"] [--ccfilter cc-report-file] [--tofile] storeDirOrFile1 storeDirOrFile2 ...
     *
     * --txfilter
     * Will match regex against each {@link LogEntry} and if there is a match,
     * include transaction containing the LogEntry in the dump.
     * regex matching is done with {@link Pattern}
     *
     * --ccfilter
     * Will look at an inconsistency report file from consistency checker and
     * include transactions that are relevant to them
     *
     * --tofile
     * Redirects output to dump-logical-log.txt in the store directory
     */
    public static void main( String[] args ) throws IOException
    {
        Args arguments = Args.withFlags( TO_FILE, LENIENT ).parse( args );
        TimeZone timeZone = parseTimeZoneConfig( arguments );
        Predicate<LogEntry[]> filter = parseFilter( arguments, timeZone );
        Function<LogEntry,String> serializer = parseSerializer( filter, timeZone );
        try ( FileSystemAbstraction fileSystem = new DefaultFileSystemAbstraction();
              Printer printer = getPrinter( arguments ) )
        {
            for ( String fileAsString : arguments.orphans() )
            {
                PrintStream out = printer.getFor( fileAsString );
                new DumpLogicalLog( fileSystem ).dump( fileAsString, out, filter, serializer );
            }
        }
    }

    @SuppressWarnings( "unchecked" )
    private static Function<LogEntry,String> parseSerializer( Predicate<LogEntry[]> filter, TimeZone timeZone )
    {
        if ( filter instanceof Function )
        {
            return (Function<LogEntry,String>) filter;
        }
        return logEntry -> logEntry.toString( timeZone );
    }

    private static Predicate<LogEntry[]> parseFilter( Args arguments, TimeZone timeZone ) throws IOException
    {
        String regex = arguments.get( TX_FILTER );
        if ( regex != null )
        {
            return new TransactionRegexCriteria( regex, timeZone );
        }
        String cc = arguments.get( CC_FILTER );
        if ( cc != null )
        {
            return new ConsistencyCheckOutputCriteria( cc, timeZone );
        }
        return null;
    }

    public static Printer getPrinter( Args args )
    {
        boolean toFile = args.getBoolean( TO_FILE, false, true );
        return toFile ? new FilePrinter() : SYSTEM_OUT_PRINTER;
    }

    public interface Printer extends AutoCloseable
    {
        PrintStream getFor( String file ) throws FileNotFoundException;

        @Override
        void close();
    }

    private static final Printer SYSTEM_OUT_PRINTER = new Printer()
    {
        @Override
        public PrintStream getFor( String file )
        {
            return System.out;
        }

        @Override
        public void close()
        {   // Don't close System.out
        }
    };

    private static class FilePrinter implements Printer
    {
        private File directory;
        private PrintStream out;

        @Override
        public PrintStream getFor( String file ) throws FileNotFoundException
        {
            File absoluteFile = new File( file ).getAbsoluteFile();
            File dir = absoluteFile.isDirectory() ? absoluteFile : absoluteFile.getParentFile();
            if ( !dir.equals( directory ) )
            {
                close();
                File dumpFile = new File( dir, "dump-logical-log.txt" );
                System.out.println( "Redirecting the output to " + dumpFile.getPath() );
                out = new PrintStream( dumpFile );
                directory = dir;
            }
            return out;
        }

        @Override
        public void close()
        {
            if ( out != null )
            {
                out.close();
            }
        }
    }

    public static TimeZone parseTimeZoneConfig( Args arguments )
    {
        return getTimeZone( arguments.get( "timezone", DEFAULT_TIME_ZONE.getID() ) );
    }
}
