/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.tools.rawstorereader;

import com.neo4j.tools.util.TransactionLogUtils;

import java.io.Console;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.cursor.IOCursor;
import org.neo4j.internal.id.DefaultIdGeneratorFactory;
import org.neo4j.internal.id.IdGeneratorFactory;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.StoreChannel;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.memory.ByteBuffers;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer;
import org.neo4j.kernel.impl.store.InvalidRecordException;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.store.RecordStore;
import org.neo4j.kernel.impl.store.StoreFactory;
import org.neo4j.kernel.impl.store.record.AbstractBaseRecord;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntry;
import org.neo4j.kernel.impl.transaction.log.files.ChannelNativeAccessor;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.storageengine.api.StorageEngineFactory;
import org.neo4j.string.HexString;

import static org.neo4j.index.internal.gbptree.RecoveryCleanupWorkCollector.immediate;
import static org.neo4j.kernel.impl.pagecache.ConfigurableStandalonePageCacheFactory.createPageCache;
import static org.neo4j.kernel.impl.scheduler.JobSchedulerFactory.createInitialisedScheduler;
import static org.neo4j.kernel.impl.store.record.RecordLoad.FORCE;
import static org.neo4j.kernel.impl.transaction.log.LogVersionBridge.NO_MORE_CHANNELS;
import static org.neo4j.memory.EmptyMemoryTracker.INSTANCE;

/**
 * Tool to read raw data from various stores.
 */
public class RsdrMain
{

    private static final Console console = System.console();
    private static final Pattern readCommandPattern = Pattern.compile( "r" + // 'r' means read command
            "((?<lower>\\d+)?,(?<upper>\\d+)?)?\\s+" + // optional record id range bounds, followed by whitespace
            "(?<fname>[\\w.]+)" + // files are a sequence of word characters or literal '.'
            "(\\s*\\|\\s*(?<regex>.+))?" // a pipe signifies a regex to filter records by
    );

    private RsdrMain()
    {
    }

    public static void main( String[] args ) throws IOException
    {
        try ( FileSystemAbstraction fileSystem = new DefaultFileSystemAbstraction() )
        {
            console.printf( "Neo4j Raw Store Diagnostics Reader%n" );

            if ( args.length != 1 || !fileSystem.isDirectory( Path.of( args[0] ) ) )
            {
                console.printf( "Usage: rsdr <store directory>%n" );
                return;
            }

            Path databaseDirectory = Path.of( args[0] );
            DatabaseLayout databaseLayout = DatabaseLayout.ofFlat( databaseDirectory );

            Config config = buildConfig();
            JobScheduler jobScheduler = createInitialisedScheduler();
            try ( PageCache pageCache = createPageCache( fileSystem, config, jobScheduler, PageCacheTracer.NULL ) )
            {
                Path neoStore = databaseLayout.metadataStore();
                StoreFactory factory = openStore( fileSystem, neoStore, config, pageCache );
                NeoStores neoStores = factory.openAllNeoStores();
                interact( fileSystem, neoStores, databaseLayout );
            }
        }
    }

    private static Config buildConfig()
    {
        return Config.newBuilder()
                .set( GraphDatabaseSettings.read_only, true )
                .set( GraphDatabaseSettings.pagecache_memory, "64M" ).build();
    }

    private static StoreFactory openStore( FileSystemAbstraction fileSystem, Path storeDir, Config config,
            PageCache pageCache )
    {
        IdGeneratorFactory idGeneratorFactory = new DefaultIdGeneratorFactory( fileSystem, immediate() );
        NullLogProvider logProvider = NullLogProvider.getInstance();
        return new StoreFactory( DatabaseLayout.ofFlat( storeDir ), config, idGeneratorFactory, pageCache, fileSystem, logProvider,
                PageCacheTracer.NULL );
    }

    private static void interact( FileSystemAbstraction fileSystem, NeoStores neoStores, DatabaseLayout databaseLayout ) throws IOException
    {
        printHelp();

        String cmd;
        do
        {
            cmd = console.readLine( "neo? " );
        } while ( execute( fileSystem, cmd, neoStores, databaseLayout ) );
        System.exit( 0 );
    }

    private static void printHelp()
    {
        console.printf( "Usage:%n" +
                "  h            print this message%n" +
                "  l            list store files in store%n" +
                "  r f          read all records in store file 'f'%n" +
                "  r5,10 f      read record 5 through 10 in store file 'f'%n" +
                "  r f | rx     read records and filter through regex 'rx'%n" +
                "  q            quit%n" );
    }

    private static boolean execute( FileSystemAbstraction fileSystem, String cmd, NeoStores neoStores, DatabaseLayout databaseLayout )
            throws IOException
    {
        if ( cmd == null || cmd.equals( "q" ) )
        {
            return false;
        }
        else if ( cmd.equals( "h" ) )
        {
            printHelp();
        }
        else if ( cmd.equals( "l" ) )
        {
            listFiles( fileSystem, databaseLayout );
        }
        else if ( cmd.startsWith( "r" ) )
        {
            read( fileSystem, cmd, neoStores, databaseLayout );
        }
        else if ( !cmd.trim().isEmpty() )
        {
            console.printf( "unrecognized command%n" );
        }
        return true;
    }

    private static void listFiles( FileSystemAbstraction fileSystem, DatabaseLayout databaseLayout ) throws IOException
    {
        Path databaseDirectory = databaseLayout.databaseDirectory();
        Path[] listing = fileSystem.listFiles( databaseDirectory );
        for ( Path file : listing )
        {
            console.printf( "%s%n", file.getFileName() );
        }
    }

    private static void read( FileSystemAbstraction fileSystem, String cmd, NeoStores neoStores, DatabaseLayout databaseLayout ) throws IOException
    {
        Matcher matcher = readCommandPattern.matcher( cmd );
        if ( matcher.find() )
        {
            String lower = matcher.group( "lower" );
            String upper = matcher.group( "upper" );
            String fname = matcher.group( "fname" );
            String regex = matcher.group( "regex" );
            Pattern pattern = regex != null ? Pattern.compile( regex ) : null;
            long fromId = lower != null ? Long.parseLong( lower ) : 0L;
            long toId = upper != null ? Long.parseLong( upper ) : Long.MAX_VALUE;

            RecordStore store = getStore( fname, neoStores );
            if ( store != null )
            {
                readStore( fileSystem, store, fromId, toId, pattern );
                return;
            }

            IOCursor<LogEntry> cursor = getLogCursor( fileSystem, fname, databaseLayout );
            if ( cursor != null )
            {
                readLog( cursor, fromId, toId, pattern );
                cursor.close();
                return;
            }

            console.printf( "don't know how to read '%s'%n", fname );
        }
        else
        {
            console.printf( "bad read command format%n" );
        }
    }

    private static void readStore( FileSystemAbstraction fileSystem,
            RecordStore store, long fromId, long toId, Pattern pattern ) throws IOException
    {
        toId = Math.min( toId, store.getHighId() );
        try ( StoreChannel channel = fileSystem.read( store.getStorageFile() ) )
        {
            int recordSize = store.getRecordSize();
            ByteBuffer buf = ByteBuffers.allocate( recordSize, INSTANCE );
            for ( long i = fromId; i <= toId; i++ )
            {
                buf.clear();
                long offset = recordSize * i;
                int count = channel.read( buf, offset );
                if ( count == -1 )
                {
                    break;
                }
                byte[] bytes = new byte[count];
                buf.clear();
                buf.get( bytes );
                String hex = HexString.encodeHexString( bytes );
                int paddingNeeded = (recordSize * 2 - Math.max( count * 2, 0 )) + 1;
                String format = "%s %6s 0x%08X %s%" + paddingNeeded + "s%s%n";
                String str;
                String use;

                try
                {
                    AbstractBaseRecord record = store.getRecord( i, store.newRecord(), FORCE, PageCursorTracer.NULL );
                    use = record.inUse() ? "+" : "-";
                    str = record.toString();
                }
                catch ( InvalidRecordException e )
                {
                    str = new String( bytes, 0, count, StandardCharsets.US_ASCII );
                    use = "?";
                }

                if ( pattern == null || pattern.matcher( str ).find() )
                {
                    console.printf( format, use, i, offset, hex, " ", str );
                }
            }
        }
    }

    private static RecordStore getStore( String fname, NeoStores neoStores )
    {
        switch ( fname )
        {
        case "neostore.nodestore.db":
            return neoStores.getNodeStore();
        case "neostore.labeltokenstore.db":
            return neoStores.getLabelTokenStore();
        case "neostore.propertystore.db.index":
            return neoStores.getPropertyKeyTokenStore();
        case "neostore.propertystore.db":
            return neoStores.getPropertyStore();
        case "neostore.relationshipgroupstore.db":
            return neoStores.getRelationshipGroupStore();
        case "neostore.relationshipstore.db":
            return neoStores.getRelationshipStore();
        case "neostore.relationshiptypestore.db":
            return neoStores.getRelationshipTypeTokenStore();
        case "neostore.schemastore.db":
            return neoStores.getSchemaStore();
        default:
            return null;
        }
    }

    private static IOCursor<LogEntry> getLogCursor( FileSystemAbstraction fileSystem, String fname,
            DatabaseLayout databaseLayout ) throws IOException
    {
        return TransactionLogUtils.openLogEntryCursor( fileSystem, databaseLayout.databaseDirectory().resolve( fname ),
                NO_MORE_CHANNELS, ChannelNativeAccessor.EMPTY_ACCESSOR, StorageEngineFactory.selectStorageEngine().commandReaderFactory() );
    }

    private static void readLog(
            IOCursor<LogEntry> cursor,
            final long fromLine,
            final long toLine,
            final Pattern pattern ) throws IOException
    {
        long lineCount = -1;
        while ( cursor.next() )
        {
            LogEntry logEntry = cursor.get();
            lineCount++;
            if ( lineCount > toLine )
            {
                return;
            }
            if ( lineCount < fromLine )
            {
                continue;
            }
            String str = logEntry.toString();
            if ( pattern == null || pattern.matcher( str ).find() )
            {
                console.printf( "%s%n", str );
            }
        }
    }
}
