/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.internal.recordstorage;

import com.neo4j.kernel.impl.store.format.highlimit.HighLimit;
import org.eclipse.collections.api.factory.Sets;
import org.junit.jupiter.api.Test;

import java.util.stream.LongStream;

import org.neo4j.internal.recordstorage.RecordPropertyCursor;
import org.neo4j.internal.recordstorage.RecordPropertyCursorTest;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.PagedFile;
import org.neo4j.kernel.impl.store.DynamicArrayStore;
import org.neo4j.kernel.impl.store.DynamicStringStore;
import org.neo4j.kernel.impl.store.InvalidRecordException;
import org.neo4j.kernel.impl.store.PropertyStore;
import org.neo4j.kernel.impl.store.format.RecordFormats;
import org.neo4j.kernel.impl.store.record.PropertyBlock;
import org.neo4j.kernel.impl.store.record.PropertyRecord;
import org.neo4j.kernel.impl.store.record.RecordLoad;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.io.pagecache.PageCacheOpenOptions.ANY_PAGE_SIZE;
import static org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer.NULL;
import static org.neo4j.storageengine.api.LongReference.longReference;

class HighLimitRecordPropertyCursorTest extends RecordPropertyCursorTest
{
    @Override
    protected RecordFormats getRecordFormats()
    {
        return HighLimit.RECORD_FORMATS;
    }

    @Test
    void mustThrowOnPropertyRecordDecodingErrors() throws Exception
    {
        // Values small enough to be inlined into the property records, yet large enough that each requires a separate property record.
        Value[] values = new Value[] { Values.stringValue( "Hello Neighbour" ), Values.stringValue( "Hello Neighbour" ) };
        long firstPropertyId = storeValuesAsPropertyChain( creator, owner, values );

        PropertyStore propertyStore = neoStores.getPropertyStore();
        PropertyRecord record = propertyStore.newRecord();
        propertyStore.getRecord( firstPropertyId, record, RecordLoad.NORMAL, NULL );
        assertTrue( record.inUse() );
        // We'll sabotage the second record in the chain.
        propertyStore.getRecord( record.getNextProp(), record, RecordLoad.NORMAL, NULL );
        assertTrue( record.inUse() );

        // Cause a cursor exception by making the property record claim to hold more blocks than it can actually fit in a record.
        try ( PagedFile pagedFile = pageCache.map( propertyStore.getStorageFile(), 0, Sets.immutable.of( ANY_PAGE_SIZE ) );
              PageCursor cursor = pagedFile.io( 0, PagedFile.PF_SHARED_WRITE_LOCK, NULL ) )
        {
            assertTrue( cursor.next() );
            int recordSize = propertyStore.getRecordSize();
            int offset = record.getIntId() * recordSize;
            cursor.putByte( offset, (byte) 0xFF );
        }

        RecordPropertyCursor cursor = createCursor();
        cursor.initNodeProperties( longReference( firstPropertyId ) );
        assertTrue( cursor.next() );
        InvalidRecordException e = assertThrows( InvalidRecordException.class, cursor::next );
        assertThat( e ).hasMessageContaining( "claims to contain more blocks than can fit" );
    }

    @Test
    void mustThrowOnArrayDecodingErrors() throws Exception
    {
        // Value that needs to be stored in the array store.
        Value[] values = new Value[] { Values.longArray( LongStream.range( 1000, 1050 ).toArray() ) };
        long firstPropertyId = storeValuesAsPropertyChain( creator, owner, values );

        PropertyStore propertyStore = neoStores.getPropertyStore();
        PropertyRecord record = propertyStore.newRecord();
        propertyStore.getRecord( firstPropertyId, record, RecordLoad.NORMAL, NULL );
        assertTrue( record.inUse() );
        PropertyBlock block = record.getPropertyBlock( 0 );
        propertyStore.ensureHeavy( block, NULL );

        // Sabotage the dynamic record storing the array value, by making it claim to hold more data than can fit in a record.
        DynamicArrayStore arrayStore = propertyStore.getArrayStore();
        try ( PagedFile pagedFile = pageCache.map( arrayStore.getStorageFile(), 0, Sets.immutable.of( ANY_PAGE_SIZE ) );
              PageCursor cursor = pagedFile.io( 0, PagedFile.PF_SHARED_WRITE_LOCK, NULL ) )
        {
            int offset = arrayStore.getRecordSize() * block.getValueRecords().get( 0 ).getIntId();
            assertTrue( cursor.next() );
            cursor.putShort( offset + 1, (short) 0x7FFF );
        }

        RecordPropertyCursor cursor = createCursor();
        cursor.initNodeProperties( longReference( firstPropertyId ) );
        assertTrue( cursor.next() );
        var e = assertThrows( InvalidRecordException.class, cursor::propertyValue );
        assertThat( e ).hasMessageContaining( "larger than the record size" );
    }

    @Test
    void mustThrowOnStringDecodingErrors() throws Exception
    {
        // Value that needs to be stored in the array store.
        Value[] values = new Value[] { Values.stringValue( "Little timmy long strings went to the store to buy paper and glue." ) };
        long firstPropertyId = storeValuesAsPropertyChain( creator, owner, values );

        PropertyStore propertyStore = neoStores.getPropertyStore();
        PropertyRecord record = propertyStore.newRecord();
        propertyStore.getRecord( firstPropertyId, record, RecordLoad.NORMAL, NULL );
        assertTrue( record.inUse() );
        PropertyBlock block = record.getPropertyBlock( 0 );
        propertyStore.ensureHeavy( block, NULL );

        // Sabotage the dynamic record storing the string value, by making it claim to hold more data than can fit in a record.
        DynamicStringStore stringStore = propertyStore.getStringStore();
        try ( PagedFile pagedFile = pageCache.map( stringStore.getStorageFile(), 0, Sets.immutable.of( ANY_PAGE_SIZE ) );
              PageCursor cursor = pagedFile.io( 0, PagedFile.PF_SHARED_WRITE_LOCK, NULL ) )
        {
            int offset = stringStore.getRecordSize() * block.getValueRecords().get( 0 ).getIntId();
            assertTrue( cursor.next() );
            cursor.putShort( offset + 1, (short) 0x7FFF );
        }

        RecordPropertyCursor cursor = createCursor();
        cursor.initNodeProperties( longReference( firstPropertyId ) );
        assertTrue( cursor.next() );
        var e = assertThrows( InvalidRecordException.class, cursor::propertyValue );
        assertThat( e ).hasMessageContaining( "larger than the record size" );
    }
}
