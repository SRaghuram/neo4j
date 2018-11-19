/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.tools.dump;

import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.neo4j.internal.kernel.api.NamedToken;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.tracing.cursor.context.EmptyVersionContextSupplier;
import org.neo4j.kernel.api.StatementConstants;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.api.CountsVisitor;
import org.neo4j.kernel.impl.storageengine.impl.recordstorage.IndexDescriptor;
import org.neo4j.kernel.impl.storageengine.impl.recordstorage.StoreIndexDescriptor;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.store.SchemaRuleAccess;
import org.neo4j.kernel.impl.store.StoreFactory;
import org.neo4j.kernel.impl.store.TokenStore;
import org.neo4j.kernel.impl.store.counts.CountsTracker;
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
import org.neo4j.storageengine.api.schema.SchemaDescriptor;

import static org.neo4j.io.pagecache.impl.muninn.StandalonePageCacheFactory.createPageCache;
import static org.neo4j.kernel.impl.scheduler.JobSchedulerFactory.createInitialisedScheduler;

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

                NeoStores neoStores = factory.openAllNeoStores();
                SchemaRuleAccess schemaStorage = SchemaRuleAccess.getSchemaRuleAccess( neoStores.getSchemaStore() );
                neoStores.getCounts().accept( new DumpCountsStore( out, neoStores, schemaStorage ) );
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

    DumpCountsStore( PrintStream out )
    {
        this( out, Collections.emptyMap(), Collections.emptyList(), Collections.emptyList(), Collections.emptyList() );
    }

    DumpCountsStore( PrintStream out, NeoStores neoStores, SchemaRuleAccess schemaRuleAccess )
    {
        this( out, getAllIndexesFrom( schemaRuleAccess ),
              allTokensFrom( neoStores.getLabelTokenStore() ),
              allTokensFrom( neoStores.getRelationshipTypeTokenStore() ),
              allTokensFrom( neoStores.getPropertyKeyTokenStore() ) );
    }

    private final PrintStream out;
    private final Map<Long,IndexDescriptor> indexes;
    private final List<NamedToken> labels;
    private final List<NamedToken> relationshipTypes;
    private final List<NamedToken> propertyKeys;

    private DumpCountsStore( PrintStream out, Map<Long,IndexDescriptor> indexes, List<NamedToken> labels,
                             List<NamedToken> relationshipTypes, List<NamedToken> propertyKeys )
    {
        this.out = out;
        this.indexes = indexes;
        this.labels = labels;
        this.relationshipTypes = relationshipTypes;
        this.propertyKeys = propertyKeys;
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
    public void visitIndexStatistics( long indexId, long updates, long size )
    {
        SchemaDescriptor schema = indexes.get( indexId ).schema();
        String tokenIds;
        switch ( schema.entityType() )
        {
        case NODE:
            tokenIds = labels( schema.getEntityTokenIds() );
            break;
        case RELATIONSHIP:
            tokenIds = relationshipTypes( schema.getEntityTokenIds() );
            break;
        default:
            throw new IllegalStateException( "Indexing is not supported for EntityType: " + schema.entityType() );
        }
        out.printf( "\tIndexStatistics[(%s {%s})]:\tupdates=%d, size=%d%n", tokenIds, propertyKeys( schema.getPropertyIds() ), updates, size );
    }

    @Override
    public void visitIndexSample( long indexId, long unique, long size )
    {
        SchemaDescriptor schema = indexes.get( indexId ).schema();
        String tokenIds;
        switch ( schema.entityType() )
        {
        case NODE:
            tokenIds = labels( schema.getEntityTokenIds() );
            break;
        case RELATIONSHIP:
            tokenIds = relationshipTypes( schema.getEntityTokenIds() );
            break;
        default:
            throw new IllegalStateException( "Indexing is not supported for EntityType: " + schema.entityType() );
        }
        out.printf( "\tIndexSample[(%s {%s})]:\tunique=%d, size=%d%n", tokenIds, propertyKeys( schema.getPropertyIds() ), unique, size );
    }

    @Override
    public boolean visitUnknownKey( ReadableBuffer key, ReadableBuffer value )
    {
        out.printf( "\t%s:\t%s%n", key, value );
        return true;
    }

    private String labels( int[] ids )
    {
        if ( ids.length == 1 )
        {
            if ( ids[0] == StatementConstants.ANY_LABEL )
            {
                return "";
            }
        }
        StringBuilder builder = new StringBuilder();
        for ( int i = 0; i < ids.length; i++ )
        {
            if ( i > 0 )
            {
                builder.append( "," );
            }
            token( builder, labels, ":", "label", ids[i] ).toString();
        }
        return builder.toString();
    }

    private String propertyKeys( int[] ids )
    {
        StringBuilder builder = new StringBuilder();
        for ( int i = 0; i < ids.length; i++ )
        {
            if ( i > 0 )
            {
                builder.append( "," );
            }
            token( builder, propertyKeys, "", "key", ids[i] );
        }
        return builder.toString();
    }

    private String relationshipTypes( int[] ids )
    {
        if ( ids.length == 1 )
        {
            if ( ids[0] == StatementConstants.ANY_LABEL )
            {
                return "";
            }
        }
        StringBuilder builder = new StringBuilder();
        for ( int i = 0; i < ids.length; i++ )
        {
            if ( i > 0 )
            {
                builder.append( "," );
            }
            return token( new StringBuilder().append( '[' ), relationshipTypes, ":", "type", i ).append( ']' ).toString();
        }
        return builder.toString();
    }

    private String relationshipType( int id )
    {
        if ( id == StatementConstants.ANY_RELATIONSHIP_TYPE )
        {
            return "";
        }
        return token( new StringBuilder().append( '[' ), relationshipTypes, ":", "type", id ).append( ']' ).toString();
    }

    private static StringBuilder token( StringBuilder result, List<NamedToken> tokens, String pre, String handle, int id )
    {
        NamedToken token = null;
        // search backwards for the token
        for ( int i = (id < tokens.size()) ? id : tokens.size() - 1; i >= 0; i-- )
        {
            token = tokens.get(i);
            if ( token.id() == id )
            {
                break; // found
            }
            if ( token.id() < id )
            {
                token = null; // not found
                break;
            }
        }
        if ( token != null )
        {
            String name = token.name();
            result.append( pre ).append( name )
                  .append( " [" ).append( handle ).append( "Id=" ).append( token.id() ).append( ']' );
        }
        else
        {
            result.append( handle ).append( "Id=" ).append( id );
        }
        return result;
    }

    private static List<NamedToken> allTokensFrom( TokenStore<?> store )
    {
        try ( TokenStore<?> tokens = store )
        {
            return tokens.getTokens();
        }
    }

    private static Map<Long,IndexDescriptor> getAllIndexesFrom( SchemaRuleAccess schemaRuleAccess )
    {
        HashMap<Long,IndexDescriptor> indexes = new HashMap<>();
        Iterator<StoreIndexDescriptor> indexRules = schemaRuleAccess.indexesGetAll();
        while ( indexRules.hasNext() )
        {
            StoreIndexDescriptor rule = indexRules.next();
            indexes.put( rule.getId(), rule );
        }
        return indexes;
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
