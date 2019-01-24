/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.tools.dump;

import java.io.File;
import java.io.PrintStream;

import org.neo4j.internal.recordstorage.RecordStorageEngineFactory;
import org.neo4j.internal.recordstorage.SchemaRuleAccess;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.tracing.cursor.context.EmptyVersionContextSupplier;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.api.index.stats.IndexStatisticsStore;
import org.neo4j.kernel.impl.api.index.stats.IndexStatisticsVisitor;
import org.neo4j.kernel.impl.core.TokenHolders;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.store.StoreFactory;
import org.neo4j.kernel.impl.store.id.DefaultIdGeneratorFactory;
import org.neo4j.kernel.lifecycle.Lifespan;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.storageengine.api.schema.SchemaDescriptor;

import static org.neo4j.index.internal.gbptree.RecoveryCleanupWorkCollector.immediate;
import static org.neo4j.io.pagecache.impl.muninn.StandalonePageCacheFactory.createPageCache;
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
            dumpIndexStatisticsStore( fileSystem, new File( args[0] ), System.out );
        }
    }

    private static void dumpIndexStatisticsStore( FileSystemAbstraction fs, File path, PrintStream out ) throws Exception
    {
        IndexStatisticsStore indexStatisticsStore;
        SimpleSchemaRuleCache schema = null;
        try ( JobScheduler jobScheduler = createInitialisedScheduler();
                PageCache pageCache = createPageCache( fs, jobScheduler );
                Lifespan life = new Lifespan() )
        {
            NullLogProvider logProvider = NullLogProvider.getInstance();
            if ( fs.isDirectory( path ) )
            {
                DatabaseLayout databaseLayout = DatabaseLayout.of( path );
                indexStatisticsStore = new IndexStatisticsStore( pageCache, databaseLayout, immediate() );
                StoreFactory factory = new StoreFactory( databaseLayout, Config.defaults(), new DefaultIdGeneratorFactory( fs ),
                        pageCache, fs, logProvider, EmptyVersionContextSupplier.EMPTY );
                NeoStores neoStores = factory.openAllNeoStores();
                TokenHolders tokenHolders = RecordStorageEngineFactory.readOnlyTokenHolders( neoStores );
                SchemaRuleAccess schemaStorage = SchemaRuleAccess.getSchemaRuleAccess( neoStores.getSchemaStore(), tokenHolders );
                schema = new SimpleSchemaRuleCache( neoStores, schemaStorage );
            }
            else
            {
                indexStatisticsStore = new IndexStatisticsStore( pageCache, path, immediate() );
            }
            life.add( indexStatisticsStore );
            indexStatisticsStore.visit( new IndexStatsVisitor( out, schema ) );
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

        @Override
        public void visitIndexStatistics( long indexId, long updates, long size )
        {
            if ( schema != null )
            {
                SchemaDescriptor descriptor = schema.indexes.get( indexId ).schema();
                String tokenIds = getTokenIds( descriptor );
                out.printf( "\tIndexStatistics[(%s {%s})]:\tupdates=%d, size=%d%n", tokenIds,
                        schema.tokens( schema.propertyKeyTokens, "propertyKey", descriptor.getPropertyIds() ), updates, size );
            }
            else
            {
                out.printf( "\tIndexStatistics[%d]:\tupdates=%d, size=%d%n", indexId, updates, size );
            }
        }

        @Override
        public void visitIndexSample( long indexId, long unique, long size )
        {
            if ( schema != null )
            {
                SchemaDescriptor descriptor = schema.indexes.get( indexId ).schema();
                String tokenIds = getTokenIds( descriptor );
                out.printf( "\tIndexSample[(%s {%s})]:\tunique=%d, size=%d%n", tokenIds,
                        schema.tokens( schema.propertyKeyTokens, "propertyKey", descriptor.getPropertyIds() ), unique, size );
            }
            else
            {
                out.printf( "\tIndexSample[%d]:\tunique=%d, size=%d%n", indexId, unique, size );
            }
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
    }
}
