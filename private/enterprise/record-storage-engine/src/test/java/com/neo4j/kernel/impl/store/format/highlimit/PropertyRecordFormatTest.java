/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.kernel.impl.store.format.highlimit;

import com.neo4j.kernel.impl.store.format.highlimit.v340.PropertyRecordFormatV3_4_0;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.concurrent.ThreadLocalRandom;

import org.neo4j.io.ByteUnit;
import org.neo4j.io.pagecache.StubPageCursor;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer;
import org.neo4j.kernel.impl.store.IntStoreHeader;
import org.neo4j.kernel.impl.store.record.PropertyRecord;
import org.neo4j.kernel.impl.store.record.Record;
import org.neo4j.kernel.impl.store.record.RecordLoad;

import static com.neo4j.kernel.impl.store.format.highlimit.BaseHighLimitRecordFormat.NULL;
import static com.neo4j.kernel.impl.store.format.highlimit.PropertyRecordFormat.RECORD_SIZE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PropertyRecordFormatTest
{
    private static final int DATA_SIZE = 100;
    private static final long TOO_BIG_REFERENCE = 1L << (Integer.SIZE + (Byte.SIZE * 3));

    private PropertyRecordFormat recordFormat;
    private StubPageCursor pageCursor;
    private ConstantIdSequence idSequence;

    @BeforeEach
    void setUp()
    {
        recordFormat = new PropertyRecordFormat();
        pageCursor = new StubPageCursor( 0, (int) ByteUnit.kibiBytes( 8 ) );
        idSequence = new ConstantIdSequence();
    }

    @AfterEach
    void tearDown()
    {
        pageCursor.close();
    }

    @Test
    void writeAndReadRecordWithRelativeReferences()
    {
        int recordSize = recordFormat.getRecordSize( new IntStoreHeader( DATA_SIZE ) );
        long recordId = 0xF1F1F1F1F1F1L;
        int recordOffset = pageCursor.getOffset();

        PropertyRecord record = createRecord( recordFormat, recordId );
        recordFormat.write( record, pageCursor, recordSize, pageCursor.getCurrentPageSize() / recordSize );

        PropertyRecord recordFromStore = recordFormat.newRecord();
        recordFromStore.setId( recordId  );
        resetCursor( pageCursor, recordOffset );
        recordFormat.read( recordFromStore, pageCursor, RecordLoad.NORMAL, recordSize, pageCursor.getCurrentPageSize() / recordSize );

        // records should be the same
        assertEquals( record.getNextProp(), recordFromStore.getNextProp() );
        assertEquals( record.getPrevProp(), recordFromStore.getPrevProp() );

        // now lets try to read same data into a record with different id - we should get different absolute references
        resetCursor( pageCursor, recordOffset );
        PropertyRecord recordWithOtherId = recordFormat.newRecord();
        recordWithOtherId.setId( 1L  );
        recordFormat.read( recordWithOtherId, pageCursor, RecordLoad.NORMAL, recordSize, pageCursor.getCurrentPageSize() / recordSize );

        verifyDifferentReferences(record, recordWithOtherId);
    }

    @Test
    void readWriteFixedReferencesRecord()
    {
        PropertyRecord source = new PropertyRecord( 1 );
        PropertyRecord target = new PropertyRecord( 1 );
        source.initialize( true, randomFixedReference(), randomFixedReference() );

        writeReadRecord( source, target );

        assertTrue( target.isUseFixedReferences(), "Record should use fixed reference format." );
        verifySameReferences( source, target );
    }

    @Test
    void useFixedReferenceFormatWhenNextPropertyIsMissing()
    {
        PropertyRecord source = new PropertyRecord( 1 );
        PropertyRecord target = new PropertyRecord( 1 );
        source.initialize( true, randomFixedReference(), Record.NULL_REFERENCE.byteValue() );

        writeReadRecord( source, target );

        assertTrue( target.isUseFixedReferences(), "Record should use fixed reference format." );
        verifySameReferences( source, target );
    }

    @Test
    void useFixedReferenceFormatWhenPreviousPropertyIsMissing()
    {
        PropertyRecord source = new PropertyRecord( 1 );
        PropertyRecord target = new PropertyRecord( 1 );
        source.initialize( true, Record.NULL_REFERENCE.intValue(), randomFixedReference() );

        writeReadRecord( source, target );

        assertTrue( target.isUseFixedReferences(), "Record should use fixed reference format." );
        verifySameReferences( source, target );
    }

    @Test
    void useVariableLengthFormatWhenPreviousPropertyReferenceTooBig()
    {
        PropertyRecord source = new PropertyRecord( 1 );
        PropertyRecord target = new PropertyRecord( 1 );
        source.initialize( true, TOO_BIG_REFERENCE, randomFixedReference() );

        writeReadRecord( source, target );

        assertFalse( target.isUseFixedReferences(), "Record should use variable length reference format." );
        verifySameReferences( source, target );
    }

    @Test
    void useVariableLengthFormatWhenNextPropertyReferenceTooBig()
    {
        PropertyRecord source = new PropertyRecord( 1 );
        PropertyRecord target = new PropertyRecord( 1 );
        source.initialize( true, randomFixedReference(), TOO_BIG_REFERENCE );

        writeReadRecord( source, target );

        assertFalse( target.isUseFixedReferences(), "Record should use variable length reference format." );
        verifySameReferences( source, target );
    }

    @Test
    void useVariableLengthFormatWhenRecordSizeIsTooSmall()
    {
        PropertyRecord source = new PropertyRecord( 1 );
        PropertyRecord target = new PropertyRecord( 1 );
        source.initialize( true, randomFixedReference(), randomFixedReference() );

        writeReadRecord( source, target, PropertyRecordFormat.FIXED_FORMAT_RECORD_SIZE - 1 );

        assertFalse( target.isUseFixedReferences(), "Record should use variable length reference if format record is too small." );
        verifySameReferences( source, target);
    }

    @Test
    void useFixedReferenceFormatWhenRecordCanFitInRecordSizeRecord()
    {
        PropertyRecord source = new PropertyRecord( 1 );
        PropertyRecord target = new PropertyRecord( 1 );
        source.initialize( true, randomFixedReference(), randomFixedReference() );

        writeReadRecord( source, target, PropertyRecordFormat.FIXED_FORMAT_RECORD_SIZE );

        assertTrue( target.isUseFixedReferences(), "Record should use fixed reference if can fit in format record." );
        verifySameReferences( source, target);
    }

    @Test
    void readSingleUnitRecordStoredNotInFixedReferenceFormat()
    {
        PropertyRecord oldFormatRecord = new PropertyRecord( 1 );
        PropertyRecord newFormatRecord = new PropertyRecord( 1 );
        oldFormatRecord.initialize( true, randomFixedReference(), randomFixedReference() );

        writeRecordWithOldFormat( oldFormatRecord );

        assertFalse( oldFormatRecord.hasSecondaryUnitId(), "This should be single unit record." );
        assertTrue( oldFormatRecord.isUseFixedReferences(), "Old format is aware about fixed references." );

        recordFormat.read( newFormatRecord, pageCursor, RecordLoad.NORMAL, RECORD_SIZE, pageCursor.getCurrentPageSize() / RECORD_SIZE );
        verifySameReferences( oldFormatRecord, newFormatRecord );
    }

    @Test
    void readUnusedRecordShouldStillBeUnused()
    {
        //Given
        int recordSize = RECORD_SIZE;
        int recordsPerPage = pageCursor.getCurrentPageSize() / recordSize;
        PropertyRecord record = new PropertyRecord( 1 );
        record.setNextProp( NULL );
        record.setPrevProp( NULL );
        record.setInUse( true );
        recordFormat.write( record, pageCursor, recordSize, recordsPerPage );

        //When
        pageCursor.setOffset( 0 );
        record.setInUse( false );
        recordFormat.write( record, pageCursor, recordSize, recordsPerPage );

        //Then
        pageCursor.setOffset( 0 );
        recordFormat.read( record, pageCursor, RecordLoad.FORCE, recordSize, recordsPerPage );
        assertFalse( record.inUse() );
    }

    private void writeRecordWithOldFormat( PropertyRecord oldFormatRecord )
    {
        int oldRecordSize = PropertyRecordFormatV3_4_0.RECORD_SIZE;
        PropertyRecordFormatV3_4_0 recordFormatV30 = new PropertyRecordFormatV3_4_0();
        recordFormatV30.prepare( oldFormatRecord, oldRecordSize, idSequence, PageCursorTracer.NULL );
        recordFormatV30.write( oldFormatRecord, pageCursor, oldRecordSize, pageCursor.getCurrentPageSize() / oldRecordSize );
        pageCursor.setOffset( 0 );
    }

    private static void verifySameReferences( PropertyRecord recordA, PropertyRecord recordB )
    {
        assertEquals( recordA.getNextProp(), recordB.getNextProp() );
        assertEquals( recordA.getPrevProp(), recordB.getPrevProp() );
    }

    private static void verifyDifferentReferences( PropertyRecord recordA, PropertyRecord recordB )
    {
        assertNotEquals( recordA.getNextProp(), recordB.getNextProp() );
        assertNotEquals( recordA.getPrevProp(), recordB.getPrevProp() );
    }

    private void writeReadRecord( PropertyRecord source, PropertyRecord target )
    {
        writeReadRecord( source, target, RECORD_SIZE );
    }

    private void writeReadRecord( PropertyRecord source, PropertyRecord target, int recordSize )
    {
        recordFormat.prepare( source, recordSize, idSequence, PageCursorTracer.NULL );
        recordFormat.write( source, pageCursor, recordSize, pageCursor.getCurrentPageSize() / recordSize );
        pageCursor.setOffset( 0 );
        recordFormat.read( target, pageCursor, RecordLoad.NORMAL, recordSize, pageCursor.getCurrentPageSize() / recordSize );
    }

    private static long randomFixedReference()
    {
        return randomReference( 1L << (Integer.SIZE + (Byte.SIZE * 2)) );
    }

    private static long randomReference( long maxValue )
    {
        return ThreadLocalRandom.current().nextLong( maxValue );
    }

    private static void resetCursor( StubPageCursor cursor, int recordOffset )
    {
        cursor.setOffset( recordOffset );
    }

    private static PropertyRecord createRecord( PropertyRecordFormat format, long recordId )
    {
        PropertyRecord record = format.newRecord();
        record.setInUse( true );
        record.setId( recordId );
        record.setNextProp( 1L );
        record.setPrevProp( (Integer.MAX_VALUE + 1L) << Byte.SIZE * 3 );
        return record;
    }
}
