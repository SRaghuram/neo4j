/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.kernel.impl.store.format.highlimit;

import org.junit.jupiter.api.Test;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.StubPageCursor;
import org.neo4j.io.pagecache.StubPagedFile;
import org.neo4j.kernel.impl.store.record.AbstractBaseRecord;
import org.neo4j.kernel.impl.store.record.RecordLoad;

import static com.neo4j.kernel.impl.store.format.highlimit.BaseHighLimitRecordFormat.HEADER_BIT_FIRST_RECORD_UNIT;
import static com.neo4j.kernel.impl.store.format.highlimit.BaseHighLimitRecordFormat.HEADER_BIT_RECORD_UNIT;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class BaseHighLimitRecordFormatTest
{
    private static final int RECORD_SIZE = 4;

    @Test
    void mustNotCheckForOutOfBoundsWhenReadingSingleRecord() throws Exception
    {
        MyRecordFormat format = new MyRecordFormat();
        StubPageCursor cursor = new StubPageCursor( 0, 3 );
        format.read( new MyRecord( 0 ), cursor, RecordLoad.NORMAL, RECORD_SIZE, cursor.getCurrentPageSize() / RECORD_SIZE );
        assertFalse( cursor.checkAndClearBoundsFlag() );
    }

    @Test
    void mustCheckForOutOfBoundsWhenReadingDoubleRecord() throws Exception
    {
        MyRecordFormat format = new MyRecordFormat();
        StubPageCursor cursor = new StubPageCursor( 0, 4 );
        cursor.putByte( 0, (byte) (HEADER_BIT_RECORD_UNIT + HEADER_BIT_FIRST_RECORD_UNIT) );
        new StubPagedFile( 3 )
        {
            @Override
            protected void prepareCursor( StubPageCursor cursor )
            {
                cursor.putByte( 0, (byte) HEADER_BIT_RECORD_UNIT );
            }
        };
        format.shortsPerRecord.add( 2 );
        format.read( new MyRecord( 0 ), cursor, RecordLoad.NORMAL, RECORD_SIZE, cursor.getCurrentPageSize() / RECORD_SIZE );
        assertTrue( cursor.checkAndClearBoundsFlag() );
    }

    @Test
    void mustNotCheckForOutOfBoundsWhenWritingSingleRecord() throws Exception
    {
        MyRecordFormat format = new MyRecordFormat();
        StubPageCursor cursor = new StubPageCursor( 0, 3 );
        MyRecord record = new MyRecord( 0 );
        record.setInUse( true );
        format.write( record, cursor, RECORD_SIZE, cursor.getCurrentPageSize() / RECORD_SIZE );
        assertFalse( cursor.checkAndClearBoundsFlag() );
    }

    @Test
    void mustCheckForOutOfBoundsWhenWritingDoubleRecord() throws Exception
    {
        MyRecordFormat format = new MyRecordFormat();
        StubPageCursor cursor = new StubPageCursor( 0, 5 );
        MyRecord record = new MyRecord( 0 );
        record.setSecondaryUnitIdOnCreate( 42 );
        record.setInUse( true );
        format.shortsPerRecord.add( 3 ); // make the write go out of bounds
        format.write( record, cursor, RECORD_SIZE, cursor.getCurrentPageSize() / RECORD_SIZE );
        assertTrue( cursor.checkAndClearBoundsFlag() );
    }

    private class MyRecordFormat extends BaseHighLimitRecordFormat<MyRecord>
    {
        private final Queue<Integer> shortsPerRecord = new ConcurrentLinkedQueue<>();

        MyRecordFormat()
        {
            super( header -> 4, 4, HighLimitFormatSettings.DEFAULT_MAXIMUM_BITS_PER_ID );
        }

        @Override
        protected void doReadInternal( MyRecord record, PageCursor cursor, int recordSize,
                                       long inUseByte, boolean inUse )
        {
            int shortsPerRecord = getShortsPerRecord();
            for ( int i = 0; i < shortsPerRecord; i++ )
            {
                short v = (short) ((cursor.getByte() & 0xFF) << 8);
                v += cursor.getByte() & 0xFF;
                record.value = v;
            }
        }

        private int getShortsPerRecord()
        {
            Integer value = shortsPerRecord.poll();
            return value == null ? 1 : value;
        }

        @Override
        protected void doWriteInternal( MyRecord record, PageCursor cursor )
        {
            int intsPerRecord = getShortsPerRecord();
            for ( int i = 0; i < intsPerRecord; i++ )
            {
                short v = record.value;
                byte a = (byte) ((v & 0x0000FF00) >> 8);
                byte b = (byte)  (v & 0x000000FF);
                cursor.putByte( a );
                cursor.putByte( b );
            }
        }

        @Override
        protected byte headerBits( MyRecord record )
        {
            return 0;
        }

        @Override
        protected boolean canUseFixedReferences( MyRecord record, int recordSize )
        {
            return false;
        }

        @Override
        protected int requiredDataLength( MyRecord record )
        {
            return 4;
        }

        @Override
        public MyRecord newRecord()
        {
            return new MyRecord( 0 );
        }
    }

    private class MyRecord extends AbstractBaseRecord
    {
        public short value;

        MyRecord( long id )
        {
            super( id );
        }
    }
}
