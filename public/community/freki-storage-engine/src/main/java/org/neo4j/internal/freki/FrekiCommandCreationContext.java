/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.internal.freki;

import org.apache.commons.lang3.mutable.MutableInt;
import org.eclipse.collections.api.map.primitive.MutableLongObjectMap;
import org.eclipse.collections.impl.factory.primitive.LongObjectMaps;

import org.neo4j.internal.id.IdGenerator;
import org.neo4j.internal.id.IdGeneratorFactory;
import org.neo4j.internal.id.IdType;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.storageengine.api.CommandCreationContext;

import static org.neo4j.internal.freki.FrekiMainStoreCursor.NULL;
import static org.neo4j.internal.freki.Header.OFFSET_NEXT_INTERNAL_RELATIONSHIP_ID;
import static org.neo4j.internal.freki.Header.OFFSET_RELATIONSHIPS;
import static org.neo4j.internal.freki.MutableNodeData.ARTIFICIAL_MAX_RELATIONSHIP_COUNTER;
import static org.neo4j.internal.freki.MutableNodeData.FIRST_RELATIONSHIP_ID;
import static org.neo4j.internal.freki.MutableNodeData.externalRelationshipId;
import static org.neo4j.internal.freki.MutableNodeData.idFromRecordPointer;
import static org.neo4j.internal.freki.MutableNodeData.sizeExponentialFromRecordPointer;
import static org.neo4j.internal.freki.Record.FLAG_IN_USE;
import static org.neo4j.util.Preconditions.checkState;

class FrekiCommandCreationContext implements CommandCreationContext
{
    private final MainStores stores;
    private final IdGenerator nodes;
    private final IdGenerator labelTokens;
    private final IdGenerator relationshipTypeTokens;
    private final IdGenerator propertyKeyTokens;
    private final IdGenerator schema;
    private final PageCursorTracer cursorTracer;
    private final MemoryTracker memoryTracker; //TODO we should probably track some memory
    private MutableLongObjectMap<MutableInt> sourceNodeNextRelationshipIds = LongObjectMaps.mutable.empty();

    FrekiCommandCreationContext( MainStores stores, IdGeneratorFactory idGeneratorFactory, PageCursorTracer cursorTracer, MemoryTracker memoryTracker )
    {
        this.stores = stores;
        nodes = idGeneratorFactory.get( IdType.NODE );
        labelTokens = idGeneratorFactory.get( IdType.LABEL_TOKEN );
        relationshipTypeTokens = idGeneratorFactory.get( IdType.RELATIONSHIP_TYPE_TOKEN );
        propertyKeyTokens = idGeneratorFactory.get( IdType.PROPERTY_KEY_TOKEN );
        schema = idGeneratorFactory.get( IdType.SCHEMA );
        this.cursorTracer = cursorTracer;
        this.memoryTracker = memoryTracker;
    }

    @Override
    public long reserveNode()
    {
        return nodes.nextId( cursorTracer );
    }

    @Override
    public long reserveRelationship( long sourceNode )
    {
        // This is a bit more complicated than simply asking an ID generator for a new ID. The relationship ids are associated with
        // their source node and therefore there's some loading of node data involved.

        MutableInt nextRelationshipId = sourceNodeNextRelationshipIds.getIfAbsentPutWithKey( sourceNode, this::getNextInternalRelationshipId );

        long internalRelationshipId = nextRelationshipId.getAndIncrement();
        checkState( internalRelationshipId < ARTIFICIAL_MAX_RELATIONSHIP_COUNTER, "Relationship counter exhausted for node %d", internalRelationshipId );
        return externalRelationshipId( sourceNode, internalRelationshipId );
    }

    private MutableInt getNextInternalRelationshipId( long nodeId )
    {
        int sizeExp = 0;
        long id = nodeId;
        MutableNodeData data = new MutableNodeData( nodeId, stores.bigPropertyValueStore, cursorTracer );

        long nextInternalRelId = NULL;
        while ( nextInternalRelId == NULL )
        {
            SimpleStore store = stores.mainStore( sizeExp );
            try ( PageCursor cursor = store.openReadCursor( cursorTracer ) )
            {
                boolean x1 = sizeExp == 0;
                Record record = store.newRecord();
                if ( store.read( cursor, record, id ) && record.hasFlag( FLAG_IN_USE ) )
                {
                    Header header = data.deserialize( record );
                    int mark = data.isDense() ? OFFSET_NEXT_INTERNAL_RELATIONSHIP_ID : OFFSET_RELATIONSHIPS;
                    if ( header.hasMark( mark ) || !header.hasReferenceMark( mark ) )
                    {
                        nextInternalRelId = data.getNextInternalRelationshipId(); //either exists here or not at all
                    }
                    else
                    {
                        //load next
                        long fw = data.getLastLoadedForwardPointer();
                        sizeExp = sizeExponentialFromRecordPointer( fw );
                        id = idFromRecordPointer( fw );
                    }
                }
                else
                {
                    if ( x1 )
                    {
                        nextInternalRelId = FIRST_RELATIONSHIP_ID;
                    }
                    else
                    {
                        throw new IllegalStateException( String.format( "Broken node record. NodeId:%d SizeExp:%d Id:%d", nodeId, sizeExp, id ) );
                    }
                }
            }
        }
        return new MutableInt( nextInternalRelId );
    }

    @Override
    public long reserveSchema()
    {
        return schema.nextId( cursorTracer );
    }

    @Override
    public int reserveLabelTokenId()
    {
        return (int) labelTokens.nextId( cursorTracer );
    }

    @Override
    public int reservePropertyKeyTokenId()
    {
        return (int) propertyKeyTokens.nextId( cursorTracer );
    }

    @Override
    public int reserveRelationshipTypeTokenId()
    {
        return (int) relationshipTypeTokens.nextId( cursorTracer );
    }

    @Override
    public void reset()
    {
        if ( !sourceNodeNextRelationshipIds.isEmpty() )
        {
            // Why not just clear()? Because this command creation instance can be very long lived and so if it sees
            // at least one big transaction then this map will have to do this expensive clear (Arrays.fill() on key/value) for the rest of its days.
            sourceNodeNextRelationshipIds = LongObjectMaps.mutable.empty();
        }
    }

    @Override
    public void close()
    {
    }
}
