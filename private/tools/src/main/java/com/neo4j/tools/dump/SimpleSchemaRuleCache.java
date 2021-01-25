/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.tools.dump;

import org.eclipse.collections.api.map.primitive.IntObjectMap;
import org.eclipse.collections.api.map.primitive.LongObjectMap;
import org.eclipse.collections.impl.map.mutable.primitive.IntObjectHashMap;
import org.eclipse.collections.impl.map.mutable.primitive.LongObjectHashMap;

import java.util.Iterator;
import java.util.List;

import org.neo4j.internal.recordstorage.SchemaRuleAccess;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.token.api.NamedToken;

import static org.neo4j.internal.kernel.api.TokenRead.ANY_LABEL;
import static org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer.NULL;

class SimpleSchemaRuleCache
{
    final LongObjectMap<IndexDescriptor> indexes;
    final IntObjectMap<NamedToken> labelTokens;
    final IntObjectMap<NamedToken> relationshipTypeTokens;
    final IntObjectMap<NamedToken> propertyKeyTokens;

    SimpleSchemaRuleCache( NeoStores neoStores, SchemaRuleAccess schemaRuleAccess )
    {
        indexes = getAllIndexesFrom( schemaRuleAccess );
        labelTokens = tokens( neoStores.getLabelTokenStore().getTokens( NULL ) );
        relationshipTypeTokens = tokens( neoStores.getRelationshipTypeTokenStore().getTokens( NULL ) );
        propertyKeyTokens = tokens( neoStores.getPropertyKeyTokenStore().getTokens( NULL ) );
    }

    String tokens( IntObjectMap<NamedToken> tokens, String type, int[] ids )
    {
        if ( ids.length == 1 )
        {
            if ( ids[0] == ANY_LABEL )
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
            token( builder, tokens.get( ids[i] ), ":", type, ids[i] );
        }
        return builder.toString();
    }

    static StringBuilder token( StringBuilder result, NamedToken token, String pre, String handle, int id )
    {
        if ( token != null )
        {
            String name = token.name();
            result.append( pre ).append( name ).append( " [" ).append( handle ).append( "Id=" ).append( token.id() ).append( ']' );
        }
        else
        {
            result.append( handle ).append( "Id=" ).append( id );
        }
        return result;
    }

    private static LongObjectMap<IndexDescriptor> getAllIndexesFrom( SchemaRuleAccess schemaRuleAccess )
    {
        LongObjectHashMap<IndexDescriptor> indexes = new LongObjectHashMap<>();
        Iterator<IndexDescriptor> indexRules = schemaRuleAccess.indexesGetAll( NULL );
        while ( indexRules.hasNext() )
        {
            IndexDescriptor rule = indexRules.next();
            indexes.put( rule.getId(), rule );
        }
        return indexes;
    }

    private IntObjectMap<NamedToken> tokens( List<NamedToken> tokens )
    {
        IntObjectHashMap<NamedToken> map = new IntObjectHashMap<>( tokens.size() );
        tokens.forEach( token -> map.put( token.id(), token ) );
        return map;
    }
}
