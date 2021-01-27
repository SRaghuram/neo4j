/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.dbms.commandline.storeutil;

import org.eclipse.collections.api.map.primitive.MutableLongObjectMap;
import org.eclipse.collections.impl.factory.primitive.LongObjectMaps;
import org.junit.jupiter.api.Test;

import org.neo4j.consistency.RecordType;
import org.neo4j.internal.batchimport.input.InputEntityVisitor;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer;
import org.neo4j.kernel.impl.store.CommonAbstractStore;
import org.neo4j.kernel.impl.store.PropertyStore;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.store.record.PropertyRecord;
import org.neo4j.token.TokenHolders;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.kernel.impl.store.record.Record.NULL_REFERENCE;

class LenientStoreInputChunkTest
{
    @Test
    void shouldHandleCircularPropertyChain()
    {
        // given
        PropertyStore propertyStore = mock( PropertyStore.class );
        when( propertyStore.newRecord() ).thenAnswer( invocationOnMock -> new PropertyRecord( -1 ) );
        MutableLongObjectMap<PropertyRecord> propertyRecords = LongObjectMaps.mutable.empty();
        long[] propertyRecordIds = new long[]{12, 13, 14, 15};
        for ( int i = 0; i < propertyRecordIds.length; i++ )
        {
            long prev = i == 0 ? NULL_REFERENCE.longValue() : propertyRecordIds[i - 1];
            long id = propertyRecordIds[i];
            long next = i == propertyRecordIds.length - 1 ? propertyRecordIds[1] : propertyRecordIds[i + 1];
            propertyRecords.put( id, new PropertyRecord( id ).initialize( true, prev, next ) );
        }
        doAnswer( invocationOnMock ->
        {
            long id = invocationOnMock.getArgument( 0 );
            PropertyRecord sourceRecord = propertyRecords.get( id );
            PropertyRecord targetRecord = invocationOnMock.getArgument( 1 );
            targetRecord.setId( id );
            targetRecord.initialize( true, sourceRecord.getPrevProp(), sourceRecord.getNextProp() );
            return null;
        } ).when( propertyStore ).getRecordByCursor( anyLong(), any(), any(), any() );

        StoreCopyStats stats = mock( StoreCopyStats.class );
        StoreCopyFilter filter = mock( StoreCopyFilter.class );
        LenientStoreInputChunk chunk = new LenientStoreInputChunk( stats, propertyStore, mock( TokenHolders.class ), filter, mock( CommonAbstractStore.class ),
                PageCacheTracer.NULL )
        {
            @Override
            void readAndVisit( long id, InputEntityVisitor visitor, PageCursorTracer cursorTracer )
            {
            }

            @Override
            String recordType()
            {
                return "test";
            }
        };

        // when
        NodeRecord primitiveRecord = new NodeRecord( 9 );
        primitiveRecord.initialize( true, propertyRecordIds[0], false, NULL_REFERENCE.longValue(), NULL_REFERENCE.longValue() );
        InputEntityVisitor visitor = mock( InputEntityVisitor.class );
        chunk.visitPropertyChainNoThrow( visitor, primitiveRecord, RecordType.NODE, new long[0] );

        // then
        verify( stats ).circularPropertyChain( any(), eq( primitiveRecord ), eq( propertyRecords.get( 15 ) ) );
    }
}
