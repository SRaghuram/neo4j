/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
import org.neo4j.kernel.impl.store.NodeStore;
import org.neo4j.kernel.impl.store.PropertyStore;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.store.record.RecordLoad;
import org.neo4j.token.TokenHolders;
import org.neo4j.token.api.TokenHolder;

import static org.neo4j.kernel.impl.store.NodeLabelsField.parseLabelsField;

class LenientNodeReader extends LenientStoreInputChunk
{
    private final NodeStore nodeStore;
    private final NodeRecord record;
    private final StoreCopyFilter.TokenLookup tokenLookup;

    LenientNodeReader( StoreCopyStats stats, NodeStore nodeStore, PropertyStore propertyStore, TokenHolders tokenHolders, StoreCopyFilter storeCopyFilter,
            PageCacheTracer pageCacheTracer )
    {
        super( stats, propertyStore, tokenHolders, storeCopyFilter, nodeStore, pageCacheTracer );
        this.nodeStore = nodeStore;
        this.record = nodeStore.newRecord();
        TokenHolder tokenHolder = tokenHolders.labelTokens();
        tokenLookup = id -> tokenHolder.getTokenById( id ).name();
    }

    @Override
    void readAndVisit( long id, InputEntityVisitor visitor, PageCursorTracer cursorTracer ) throws IOException
    {
        nodeStore.getRecordByCursor( id, record, RecordLoad.LENIENT_CHECK, cursor );
        if ( record.inUse() )
        {
            nodeStore.ensureHeavy( record, cursorTracer );
            long[] labelIds = parseLabelsField( record ).get( nodeStore, cursorTracer );
            if ( !storeCopyFilter.shouldDeleteNode( labelIds ) )
            {
                String[] labels = storeCopyFilter.filterLabels( labelIds, tokenLookup );
                visitor.id( id, Group.GLOBAL ); // call to id(long) will not use the remapper
                visitor.labels( labels );
                visitPropertyChainNoThrow( visitor, record, RecordType.NODE, labelIds );
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
        return "Node";
    }
}
