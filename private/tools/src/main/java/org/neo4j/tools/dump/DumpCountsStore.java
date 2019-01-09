/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.tools.dump;

import java.io.File;
import java.io.IOException;
import java.io.PrintStream;

import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.tracing.cursor.context.EmptyVersionContextSupplier;
import org.neo4j.kernel.api.StatementConstants;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.api.CountsVisitor;
import org.neo4j.kernel.impl.core.TokenHolders;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.store.SchemaRuleAccess;
import org.neo4j.kernel.impl.store.StoreFactory;
import org.neo4j.kernel.impl.store.counts.CountsTracker;
import org.neo4j.kernel.impl.store.counts.ReadOnlyCountsTracker;
import org.neo4j.kernel.impl.store.id.DefaultIdGeneratorFactory;
import org.neo4j.kernel.impl.store.kvstore.HeaderField;
import org.neo4j.kernel.impl.store.kvstore.Headers;
import org.neo4j.kernel.impl.store.kvstore.MetadataVisitor;
import org.neo4j.kernel.impl.store.kvstore.ReadableBuffer;
import org.neo4j.kernel.impl.store.kvstore.UnknownKey;
import org.neo4j.kernel.lifecycle.Lifespan;
import org.neo4j.logging.LogProvider;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.scheduler.JobScheduler;

import static org.neo4j.io.pagecache.impl.muninn.StandalonePageCacheFactory.createPageCache;
import static org.neo4j.kernel.impl.scheduler.JobSchedulerFactory.createInitialisedScheduler;
import static org.neo4j.tools.dump.SimpleSchemaRuleCache.token;

/**
 * Tool that will dump content of count store content into a simple string representation for further analysis.
 */
public class DumpCountsStore implements CountsVisitor, MetadataVisitor, UnknownKey.Visitor
{
    public static void main( String... args ) throws Exception
    {
        if ( args.length != 1 )
        {
            System.err.println( "Expecting exactly one argument describing the path to the store" );
            System.exit( 1 );
        }
        try ( FileSystemAbstraction fileSystem = new DefaultFileSystemAbstraction() )
        {
            dumpCountsStore( fileSystem, new File( args[0] ), System.out );
        }
    }

    public static void dumpCountsStore( FileSystemAbstraction fs, File path, PrintStream out ) throws Exception
    {
        try ( JobScheduler jobScheduler = createInitialisedScheduler();
              PageCache pages = createPageCache( fs, jobScheduler );
              Lifespan life = new Lifespan() )
        {
            NullLogProvider logProvider = NullLogProvider.getInstance();
            Config config = Config.defaults();
            if ( fs.isDirectory( path ) )
            {
                DatabaseLayout databaseLayout = DatabaseLayout.of( path );
                StoreFactory factory = new StoreFactory( databaseLayout, Config.defaults(), new DefaultIdGeneratorFactory( fs ),
                        pages, fs, logProvider, EmptyVersionContextSupplier.EMPTY );
                CountsTracker counts = new ReadOnlyCountsTracker( logProvider, fs, pages, config, databaseLayout );
                life.add( counts );

                NeoStores neoStores = factory.openAllNeoStores();

                TokenHolders tokenHolders = TokenHolders.readOnlyTokenHolders( neoStores );
                SchemaRuleAccess schemaStorage = SchemaRuleAccess.getSchemaRuleAccess( neoStores.getSchemaStore(), tokenHolders );
                counts.accept( new DumpCountsStore( out, new SimpleSchemaRuleCache( neoStores, schemaStorage ) ) );
            }
            else
            {
                VisitableCountsTracker tracker = new VisitableCountsTracker(
                        logProvider, fs, pages, config, DatabaseLayout.of( path.getParentFile() ) );
                if ( fs.fileExists( path ) )
                {
                    tracker.visitFile( path, new DumpCountsStore( out ) );
                }
                else
                {
                    life.add( tracker ).accept( new DumpCountsStore( out ) );
                }
            }
        }
    }

    private final PrintStream out;
    private final SimpleSchemaRuleCache schema;

    DumpCountsStore( PrintStream out )
    {
        this( out, null );
    }

    DumpCountsStore( PrintStream out, SimpleSchemaRuleCache schema )
    {
        this.out = out;
        this.schema = schema;
    }

    @Override
    public void visitMetadata( File file, Headers headers, int entryCount )
    {
        out.printf( "Counts Store:\t%s%n", file );
        for ( HeaderField<?> headerField : headers.fields() )
        {
            out.printf( "%s:\t%s%n", headerField.toString(), headers.get( headerField ) );
        }
        out.printf( "\tentries:\t%d%n", entryCount );
        out.println( "Entries:" );
    }

    @Override
    public void visitNodeCount( int labelId, long count )
    {
        out.printf( "\tNode[(%s)]:\t%d%n", labels( new int[]{labelId} ), count );
    }

    @Override
    public void visitRelationshipCount( int startLabelId, int typeId, int endLabelId, long count )
    {
        out.printf( "\tRelationship[(%s)-%s->(%s)]:\t%d%n",
                    labels( new int[]{startLabelId} ), relationshipType( typeId ), labels( new int[]{endLabelId} ),
                    count );
    }

    @Override
    public boolean visitUnknownKey( ReadableBuffer key, ReadableBuffer value )
    {
        out.printf( "\t%s:\t%s%n", key, value );
        return true;
    }

    private String labels( int[] ids )
    {
        return schema.tokens( schema.labelTokens, "label", ids );
    }

    private String relationshipType( int id )
    {
        if ( id == StatementConstants.ANY_RELATIONSHIP_TYPE )
        {
            return "";
        }
        return token( new StringBuilder().append( '[' ),
                schema != null ? schema.relationshipTypeTokens.get( id ) : null, ":", "type", id ).append( ']' ).toString();
    }

    private static class VisitableCountsTracker extends CountsTracker
    {
        VisitableCountsTracker( LogProvider logProvider, FileSystemAbstraction fs,
                PageCache pages, Config config, DatabaseLayout databaseLayout )
        {
            super( logProvider, fs, pages, config, databaseLayout, EmptyVersionContextSupplier.EMPTY );
        }

        @Override
        public void visitFile( File path, CountsVisitor visitor ) throws IOException
        {
            super.visitFile( path, visitor );
        }
    }
}
