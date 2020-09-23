/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.tools.dump;

import java.io.PrintStream;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.function.Function;
import java.util.function.Supplier;

import org.neo4j.common.HexPrinter;
import org.neo4j.configuration.Config;
import org.neo4j.internal.id.DefaultIdGeneratorFactory;
import org.neo4j.internal.recordstorage.SchemaRuleAccess;
import org.neo4j.internal.recordstorage.StoreTokens;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.layout.DatabaseFile;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.kernel.impl.store.CommonAbstractStore;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.store.NodeStore;
import org.neo4j.kernel.impl.store.RecordStore;
import org.neo4j.kernel.impl.store.SchemaStore;
import org.neo4j.kernel.impl.store.StoreFactory;
import org.neo4j.kernel.impl.store.StoreType;
import org.neo4j.kernel.impl.store.TokenStore;
import org.neo4j.kernel.impl.store.record.AbstractBaseRecord;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.store.record.SchemaRecord;
import org.neo4j.kernel.impl.store.record.TokenRecord;
import org.neo4j.logging.LogProvider;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.logging.log4j.Log4jLogProvider;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.token.TokenHolders;

import static java.lang.Long.parseLong;
import static org.neo4j.index.internal.gbptree.RecoveryCleanupWorkCollector.immediate;
import static org.neo4j.io.pagecache.impl.muninn.StandalonePageCacheFactory.createPageCache;
import static org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer.NULL;
import static org.neo4j.kernel.impl.scheduler.JobSchedulerFactory.createInitialisedScheduler;
import static org.neo4j.kernel.impl.store.record.RecordLoad.FORCE;

/**
 * Tool to dump content of specified store into readable format for further analysis.
 * @param <RECORD> type of record to dump
 * @param <STORE> type of store to dump
 */
public class DumpStore<RECORD extends AbstractBaseRecord, STORE extends RecordStore<RECORD>>
{
    private static class IdRange
    {
        private final long startId;
        private final long endId;

        IdRange( long startId, long endId )
        {
            this.startId = startId;
            this.endId = endId;
        }

        static IdRange parse( String idString )
        {
            if ( idString.contains( "-" ) )
            {
                String[] parts = idString.split( "-" );
                return new IdRange( parseLong( parts[0] ), parseLong( parts[1] ) );
            }

            long id = parseLong( idString );
            return new IdRange( id, id + 1 );
        }
    }

    public static void main( String... args ) throws Exception
    {
        if ( args == null || args.length == 0 )
        {
            System.err.println( "SYNTAX: [file[:id[,id]*]]+" );
            System.err.println( "where 'id' can be single id or range like: lowId-highId" );
            return;
        }

        var cacheTracer = PageCacheTracer.NULL;
        try ( DefaultFileSystemAbstraction fs = new DefaultFileSystemAbstraction();
              JobScheduler scheduler = createInitialisedScheduler();
              PageCache pageCache = createPageCache( fs, scheduler, cacheTracer ) )
        {
            final DefaultIdGeneratorFactory idGeneratorFactory = new DefaultIdGeneratorFactory( fs, immediate() );
            Function<Path,StoreFactory> createStoreFactory = file -> new StoreFactory( DatabaseLayout.ofFlat( file.getParent() ),
                    Config.defaults(), idGeneratorFactory, pageCache, fs, logProvider(), cacheTracer );

            for ( String arg : args )
            {
                dumpFile( createStoreFactory, arg );
            }
        }
    }

    private static void dumpFile( Function<Path, StoreFactory> createStoreFactory, String fileName ) throws Exception
    {
        Path file = Path.of( fileName );
        IdRange[] ids = null; // null means all possible ids

        if ( Files.isRegularFile( file ) )
        {
                /* If file exists, even with : in its path, then accept it straight off. */
        }
        else if ( !Files.isDirectory( file ) && file.getFileName().toString().indexOf( ':' ) != -1 )
        {
                /* Now we know that it is not a directory either, and that the last component
                   of the path contains a colon, thus it is very likely an attempt to use the
                   id-specifying syntax. */

            int idStart = fileName.lastIndexOf( ':' );

            String[] idStrings = fileName.substring( idStart + 1 ).split( "," );
            ids = new IdRange[idStrings.length];
            for ( int i = 0; i < ids.length; i++ )
            {
                ids[i] = IdRange.parse( idStrings[i] );
            }
            file = Path.of( fileName.substring( 0, idStart ) );

            if ( !Files.isRegularFile( file ) )
            {
                throw new IllegalArgumentException( "No such file: " + fileName );
            }
        }
        DatabaseFile databaseFile = DatabaseFile.fileOf( file.getFileName().toString() ).orElseThrow( illegalArgumentExceptionSupplier( fileName ) );
        StoreType storeType = StoreType.typeOf( databaseFile ).orElseThrow( illegalArgumentExceptionSupplier( fileName ) );
        try ( NeoStores neoStores = createStoreFactory.apply( file ).openNeoStores( storeType ) )
        {
            switch ( storeType )
            {
            case META_DATA:
                dumpMetaDataStore( neoStores );
                break;
            case NODE:
                dumpNodeStore( neoStores, ids );
                break;
            case RELATIONSHIP:
                dumpRelationshipStore( neoStores, ids );
                break;
            case PROPERTY:
                dumpPropertyStore( neoStores, ids );
                break;
            case SCHEMA:
                dumpSchemaStore( neoStores, ids );
                break;
            case PROPERTY_KEY_TOKEN:
                dumpPropertyKeys( neoStores, ids );
                break;
            case LABEL_TOKEN:
                dumpLabels( neoStores, ids );
                break;
            case RELATIONSHIP_TYPE_TOKEN:
                dumpRelationshipTypes( neoStores, ids );
                break;
            case RELATIONSHIP_GROUP:
                dumpRelationshipGroups( neoStores, ids );
                break;
            default:
                throw new IllegalArgumentException( "Unsupported store type: " + storeType );
            }
        }
    }

    private static Supplier<IllegalArgumentException> illegalArgumentExceptionSupplier( String fileName )
    {
        return () -> new IllegalArgumentException( "Not a store file: " + fileName );
    }

    private static void dumpMetaDataStore( NeoStores neoStores )
    {
        neoStores.getMetaDataStore().logRecords( System.out::println );
    }

    private static LogProvider logProvider()
    {
        return Boolean.getBoolean( "logger" ) ? new Log4jLogProvider( System.out )
                                              : NullLogProvider.getInstance();
    }

    private static <R extends AbstractBaseRecord, S extends RecordStore<R>> void dump(
            IdRange[] ids, S store ) throws Exception
    {
        new DumpStore<R,S>( System.out ).dump( store, ids );
    }

    private static void dumpPropertyKeys( NeoStores neoStores, IdRange[] ids ) throws Exception
    {
        dumpTokens( neoStores.getPropertyKeyTokenStore(), ids );
    }

    private static void dumpLabels( NeoStores neoStores, IdRange[] ids ) throws Exception
    {
        dumpTokens( neoStores.getLabelTokenStore(), ids );
    }

    private static void dumpRelationshipTypes( NeoStores neoStores, IdRange[] ids ) throws Exception
    {
        dumpTokens( neoStores.getRelationshipTypeTokenStore(), ids );
    }

    private static <R extends TokenRecord> void dumpTokens(
            final TokenStore<R> store, IdRange[] ids ) throws Exception
    {
        try ( store )
        {
            new DumpStore<R,TokenStore<R>>( System.out )
            {
                @Override
                protected Object transform( R record )
                {
                    if ( record.inUse() )
                    {
                        store.ensureHeavy( record, NULL );
                        return record.getId() + ": \"" + store.getStringFor( record, NULL ) + "\": " + record;
                    }
                    return null;
                }
            }.dump( store, ids );
        }
    }

    private static void dumpRelationshipGroups( NeoStores neoStores, IdRange[] ids ) throws Exception
    {
        dump( ids, neoStores.getRelationshipGroupStore() );
    }

    private static void dumpRelationshipStore( NeoStores neoStores, IdRange[] ids ) throws Exception
    {
        dump( ids, neoStores.getRelationshipStore() );
    }

    private static void dumpPropertyStore( NeoStores neoStores, IdRange[] ids ) throws Exception
    {
        dump( ids, neoStores.getPropertyStore() );
    }

    private static void dumpSchemaStore( NeoStores neoStores, IdRange[] ids ) throws Exception
    {
        try ( SchemaStore store = neoStores.getSchemaStore() )
        {
            TokenHolders tokenHolders = StoreTokens.readOnlyTokenHolders( neoStores, NULL );
            final SchemaRuleAccess schemaRuleAccess = SchemaRuleAccess.getSchemaRuleAccess( store, tokenHolders );
            new DumpStore<SchemaRecord,SchemaStore>( System.out )
            {
                @Override
                protected Object transform( SchemaRecord record ) throws Exception
                {
                    return record.inUse() ? schemaRuleAccess.loadSingleSchemaRule( record.getId(), NULL ) : null;
                }
            }.dump( store, ids );
        }
    }

    private static void dumpNodeStore( NeoStores neoStores, IdRange[] ids ) throws Exception
    {
        new DumpStore<NodeRecord,NodeStore>( System.out )
        {
            @Override
            protected Object transform( NodeRecord record )
            {
                return record.inUse() ? record : "";
            }
        }.dump( neoStores.getNodeStore(), ids );
    }

    private final PrintStream out;
    private final HexPrinter printer;

    protected DumpStore( PrintStream out )
    {
        this.out = out;
        this.printer = new HexPrinter( out ).withBytesGroupingFormat( 16, 4, "  " ).withLineNumberDigits( 8 );
    }

    public final void dump( STORE store, IdRange[] ids ) throws Exception
    {
        int size = store.getRecordSize();
        long highId = store.getHighId();
        out.println( "store.getRecordSize() = " + size );
        out.println( "store.getHighId() = " + highId );
        out.println( "<dump>" );
        long used = 0;

        if ( ids == null )
        {
            for ( long id = 0; id < highId; id++ )
            {
                boolean inUse = dumpRecord( store, size, id );

                if ( inUse )
                {
                    used++;
                }
            }
        }
        else
        {
            for ( IdRange range : ids )
            {
                for ( long id = range.startId; id < range.endId; id++ )
                {
                    dumpRecord( store, size, id );
                }
            }
        }
        out.println( "</dump>" );

        if ( ids == null )
        {
            out.printf( "used = %s / highId = %s (%.2f%%)%n", used, highId, used * 100.0 / highId );
        }
    }

    private boolean dumpRecord( STORE store, int size, long id ) throws Exception
    {
        RECORD record = store.getRecord( id, store.newRecord(), FORCE, NULL );
        Object transform = transform( record );
        if ( transform != null )
        {
            if ( !"".equals( transform ) )
            {
                out.println( transform );
            }
        }
        else
        {
            out.print( record );
            // TODO Hmm, please don't do this
            byte[] rawRecord = ((CommonAbstractStore)store).getRawRecordData( id, NULL );
            dumpHex( record, ByteBuffer.wrap( rawRecord ), id, size );
        }
        return record.inUse();
    }

    void dumpHex( RECORD record, ByteBuffer buffer, long id, int size )
    {
        printer.withLineNumberOffset( id * size );
        if ( record.inUse() )
        {
            printer.append( buffer );
        }
        else if ( allZero( buffer ) )
        {
            out.printf( ": all zeros @ 0x%x - 0x%x", id * size, (id + 1) * size );
        }
        else
        {
            printer.append( buffer );
        }
        out.printf( "%n" );
    }

    private boolean allZero( ByteBuffer buffer )
    {
        for ( int i = 0; i < buffer.limit(); i++ )
        {
            if ( buffer.get( i ) != 0 )
            {
                return false;
            }
        }
        return true;
    }

    protected Object transform( RECORD record ) throws Exception
    {
        return record.inUse() ? record : null;
    }
}
