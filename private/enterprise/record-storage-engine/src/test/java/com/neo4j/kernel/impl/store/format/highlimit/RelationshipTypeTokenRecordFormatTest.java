/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.kernel.impl.store.format.highlimit;

import org.junit.jupiter.api.Test;

import org.neo4j.internal.id.IdSequence;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.StubPageCursor;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer;
import org.neo4j.kernel.impl.store.format.RecordFormat;
import org.neo4j.kernel.impl.store.record.RelationshipTypeTokenRecord;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.neo4j.io.ByteUnit.kibiBytes;
import static org.neo4j.kernel.impl.store.NoStoreHeader.NO_STORE_HEADER;
import static org.neo4j.kernel.impl.store.record.RecordLoad.NORMAL;

class RelationshipTypeTokenRecordFormatTest
{
    @Test
    void shouldHandleRelationshipTypesBeyond2Bytes() throws Exception
    {
        // given
        RecordFormat<RelationshipTypeTokenRecord> format = HighLimit.RECORD_FORMATS.relationshipTypeToken();
        int typeId = 1 << (Short.SIZE + Byte.SIZE) - 1;
        RelationshipTypeTokenRecord record = new RelationshipTypeTokenRecord( typeId );
        int recordSize = format.getRecordSize( NO_STORE_HEADER );
        record.initialize( true, 10 );
        IdSequence doubleUnits = mock( IdSequence.class );
        PageCursor cursor = new StubPageCursor( 0, (int) kibiBytes( 8 ) );

        // when
        format.prepare( record, recordSize, doubleUnits, PageCursorTracer.NULL );
        format.write( record, cursor, recordSize, cursor.getCurrentPageSize() / recordSize );
        verifyNoMoreInteractions( doubleUnits );

        // then
        cursor.setOffset( 0 );
        RelationshipTypeTokenRecord read = new RelationshipTypeTokenRecord( typeId );
        format.read( read, cursor, NORMAL, recordSize, cursor.getCurrentPageSize() / recordSize );
        assertEquals( record, read );
    }

    @Test
    void shouldReport3BytesMaxIdForRelationshipTypes()
    {
        // given
        RecordFormat<RelationshipTypeTokenRecord> format = HighLimit.RECORD_FORMATS.relationshipTypeToken();

        // when
        long maxId = format.getMaxId();

        // then
        assertEquals( (1 << HighLimitFormatSettings.RELATIONSHIP_TYPE_TOKEN_MAXIMUM_ID_BITS) - 1, maxId );
    }
}
