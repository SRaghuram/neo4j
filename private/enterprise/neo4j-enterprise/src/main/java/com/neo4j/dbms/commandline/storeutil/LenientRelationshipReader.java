/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.dbms.commandline.storeutil;

import java.io.IOException;

import org.neo4j.consistency.RecordType;
import org.neo4j.internal.batchimport.input.Group;
import org.neo4j.internal.batchimport.input.InputEntityVisitor;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer;
import org.neo4j.kernel.impl.store.PropertyStore;
import org.neo4j.kernel.impl.store.RelationshipStore;
import org.neo4j.kernel.impl.store.record.RecordLoad;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;
import org.neo4j.token.TokenHolders;
import org.neo4j.token.api.TokenHolder;

class LenientRelationshipReader extends LenientStoreInputChunk
{
    private final RelationshipStore relationshipStore;
    private final RelationshipRecord record;
    private final StoreCopyFilter.TokenLookup tokenLookup;

    LenientRelationshipReader( StoreCopyStats stats, RelationshipStore relationshipStore, PropertyStore propertyStore, TokenHolders tokenHolders,
            StoreCopyFilter storeCopyFilter, PageCacheTracer pageCacheTracer )
    {
        super( stats, propertyStore, tokenHolders, storeCopyFilter, relationshipStore, pageCacheTracer );
        this.relationshipStore = relationshipStore;
        this.record = relationshipStore.newRecord();
        TokenHolder tokenHolder = tokenHolders.relationshipTypeTokens();
        tokenLookup = id -> tokenHolder.getTokenById( id ).name();
    }

    @Override
    void readAndVisit( long id, InputEntityVisitor visitor, PageCursorTracer cursorTracer ) throws IOException
    {
        relationshipStore.getRecordByCursor( id, record, RecordLoad.LENIENT_CHECK, cursor );
        if ( record.inUse() )
        {
            relationshipStore.ensureHeavy( record, cursorTracer );
            int relType = record.getType();
            String relName = storeCopyFilter.filterRelationship( relType, tokenLookup );
            if ( relName != null )
            {
                visitor.type( relName );
                visitor.startId( record.getFirstNode(), Group.GLOBAL );
                visitor.endId( record.getSecondNode(), Group.GLOBAL );
                visitPropertyChainNoThrow( visitor, record, RecordType.RELATIONSHIP, new long[] {relType} );
                visitor.endOfEntity();
            }
            else
            {
                stats.removed.increment();
            }
        }
        else
        {
            stats.unused.increment();
        }
    }

    @Override
    String recordType()
    {
        return "Relationship";
    }
}
