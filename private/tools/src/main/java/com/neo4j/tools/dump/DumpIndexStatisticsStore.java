/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.tools.dump;

import java.io.PrintStream;
import java.nio.file.Path;

import org.neo4j.configuration.Config;
import org.neo4j.internal.id.DefaultIdGeneratorFactory;
import org.neo4j.internal.recordstorage.SchemaRuleAccess;
import org.neo4j.internal.recordstorage.StoreTokens;
import org.neo4j.internal.schema.SchemaDescriptor;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.kernel.impl.api.index.stats.IndexStatisticsStore;
import org.neo4j.kernel.impl.api.index.stats.IndexStatisticsVisitor;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.store.StoreFactory;
import org.neo4j.kernel.lifecycle.Lifespan;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.token.TokenHolders;

import static org.neo4j.index.internal.gbptree.RecoveryCleanupWorkCollector.immediate;
import static org.neo4j.io.pagecache.impl.muninn.StandalonePageCacheFactory.createPageCache;
import static org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer.NULL;
import static org.neo4j.kernel.impl.scheduler.JobSchedulerFactory.createInitialisedScheduler;

public class DumpIndexStatisticsStore
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
            dumpIndexStatisticsStore( fileSystem, Path.of( args[0] ), System.out );
        }
    }

    private static void dumpIndexStatisticsStore( FileSystemAbstraction fs, Path path, PrintStream out ) throws Exception
    {
        IndexStatisticsStore indexStatisticsStore;
        SimpleSchemaRuleCache schema = null;
        var cacheTracer = PageCacheTracer.NULL;
        try ( JobScheduler jobScheduler = createInitialisedScheduler();
              PageCache pageCache = createPageCache( fs, jobScheduler, cacheTracer );
              Lifespan life = new Lifespan() )
        {
            NullLogProvider logProvider = NullLogProvider.getInstance();
            if ( fs.isDirectory( path ) )
            {
                DatabaseLayout databaseLayout = DatabaseLayout.ofFlat( path );
                indexStatisticsStore = new IndexStatisticsStore( pageCache, databaseLayout, immediate(), true, cacheTracer );
                StoreFactory factory = new StoreFactory( databaseLayout, Config.defaults(), new DefaultIdGeneratorFactory( fs, immediate() ),
                        pageCache, fs, logProvider, PageCacheTracer.NULL );
                NeoStores neoStores = factory.openAllNeoStores();
                TokenHolders tokenHolders = StoreTokens.readOnlyTokenHolders( neoStores, NULL );
                SchemaRuleAccess schemaStorage = SchemaRuleAccess.getSchemaRuleAccess( neoStores.getSchemaStore(), tokenHolders );
                schema = new SimpleSchemaRuleCache( neoStores, schemaStorage );
            }
            else
            {
                indexStatisticsStore = new IndexStatisticsStore( pageCache, path, immediate(), true, cacheTracer );
            }
            life.add( indexStatisticsStore );
            indexStatisticsStore.visit( new IndexStatsVisitor( out, schema ), NULL );
        }
    }

    private static class IndexStatsVisitor implements IndexStatisticsVisitor
    {
        private final PrintStream out;
        private final SimpleSchemaRuleCache schema;

        IndexStatsVisitor( PrintStream out, SimpleSchemaRuleCache schema )
        {
            this.out = out;
            this.schema = schema;
        }

        private String getTokenIds( SchemaDescriptor descriptor )
        {
            String tokenIds;
            switch ( descriptor.entityType() )
            {
            case NODE:
                tokenIds = schema.tokens( schema.labelTokens, "label", descriptor.getEntityTokenIds() );
                break;
            case RELATIONSHIP:
                tokenIds = schema.tokens( schema.relationshipTypeTokens, "relationshipType", descriptor.getEntityTokenIds() );
                break;
            default:
                throw new IllegalStateException( "Indexing is not supported for EntityType: " + descriptor.entityType() );
            }
            return tokenIds;
        }

        @Override
        public void visitIndexStatistics( long indexId, long sampleUniqueValues, long sampleSize, long updatesCount, long indexSize )
        {
            if ( schema != null )
            {
                SchemaDescriptor descriptor = schema.indexes.get( indexId ).schema();
                String tokenIds = getTokenIds( descriptor );
                out.printf( "\tIndexStatistics[(%s {%s})]:\tunique=%d, sampleSize=%d, updates=%d, indexSize=%d%n", tokenIds,
                        schema.tokens( schema.propertyKeyTokens, "propertyKey", descriptor.getPropertyIds() ),
                        sampleUniqueValues, sampleSize, updatesCount, indexSize );
            }
            else
            {
                out.printf( "\tIndexStatistics[%d]:\tunique=%d, sampleSize=%d, updates=%d, indexSize=%d%n",
                        indexId, sampleUniqueValues, sampleSize, updatesCount, indexSize );
            }
        }
    }
}
