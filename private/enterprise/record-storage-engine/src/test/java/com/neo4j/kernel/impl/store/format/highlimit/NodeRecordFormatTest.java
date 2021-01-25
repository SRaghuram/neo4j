/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.kernel.impl.store.format.highlimit;

import com.neo4j.kernel.impl.store.format.highlimit.v340.NodeRecordFormatV3_4_0;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.concurrent.ThreadLocalRandom;

import org.neo4j.io.ByteUnit;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.store.record.Record;
import org.neo4j.kernel.impl.store.record.RecordLoad;

import static com.neo4j.kernel.impl.store.format.highlimit.NodeRecordFormat.RECORD_SIZE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class NodeRecordFormatTest
{
    private NodeRecordFormat recordFormat;
    private FixedLinkedStubPageCursor pageCursor;
    private ConstantIdSequence idSequence;

    @BeforeEach
    void setUp()
    {
        recordFormat = new NodeRecordFormat();
        pageCursor = new FixedLinkedStubPageCursor( 0, (int) ByteUnit.kibiBytes( 8 ) );
        idSequence = new ConstantIdSequence();
    }

    @AfterEach
    void tearDown()
    {
        pageCursor.close();
    }

    @Test
    void readWriteFixedReferencesRecord() throws Exception
    {
        NodeRecord source = new NodeRecord( 1 );
        NodeRecord target = new NodeRecord( 1 );
        source.initialize( true, randomFixedReference(), true, randomFixedReference(), 0L );

        writeReadRecord( source, target );

        assertTrue( target.isUseFixedReferences(), "Record should use fixed reference format." );
        verifySameReferences( source, target );
    }

    @Test
    void useFixedReferencesFormatWhenRelationshipIsMissing() throws IOException
    {
        NodeRecord source = new NodeRecord( 1 );
        NodeRecord target = new NodeRecord( 1 );
        source.initialize( true, randomFixedReference(), true, Record.NULL_REFERENCE.byteValue(), 0L );

        writeReadRecord( source, target );

        assertTrue( target.isUseFixedReferences(), "Record should use fixed reference format." );
        verifySameReferences( source, target );
    }

    @Test
    void useFixedReferencesFormatWhenPropertyIsMissing() throws IOException
    {
        NodeRecord source = new NodeRecord( 1 );
        NodeRecord target = new NodeRecord( 1 );
        source.initialize( true, Record.NULL_REFERENCE.intValue(), true, randomFixedReference(), 0L );

        writeReadRecord( source, target );

        assertTrue( target.isUseFixedReferences(), "Record should use fixed reference format." );
        verifySameReferences( source, target );
    }

    @Test
    void useVariableLengthFormatWhenRelationshipReferenceTooBig() throws IOException
    {
        NodeRecord source = new NodeRecord( 1 );
        NodeRecord target = new NodeRecord( 1 );
        source.initialize( true, 1L << 37, true, randomFixedReference(), 0L );

        writeReadRecord( source, target );

        assertFalse( target.isUseFixedReferences(), "Record should use variable length reference format." );
        verifySameReferences( source, target );
    }

    @Test
    void useVariableLengthFormatWhenPropertyReferenceTooBig() throws IOException
    {
        NodeRecord source = new NodeRecord( 1 );
        NodeRecord target = new NodeRecord( 1 );
        source.initialize( true, randomFixedReference(), true, 1L << 37, 0L );

        writeReadRecord( source, target );

        assertFalse( target.isUseFixedReferences(), "Record should use variable length reference format." );
        verifySameReferences( source, target );
    }

    @Test
    void useVariableLengthFormatWhenRecordSizeIsTooSmall() throws IOException
    {
        NodeRecord source = new NodeRecord( 1 );
        NodeRecord target = new NodeRecord( 1 );
        source.initialize( true, randomFixedReference(), true, randomFixedReference(), 0L );

        writeReadRecord( source, target, NodeRecordFormat.FIXED_FORMAT_RECORD_SIZE - 1 );

        assertFalse( target.isUseFixedReferences(), "Record should use variable length reference if format record is too small." );
        verifySameReferences( source, target );
    }

    @Test
    void useFixedReferenceFormatWhenRecordCanFitInRecordSizeRecord() throws IOException
    {
        NodeRecord source = new NodeRecord( 1 );
        NodeRecord target = new NodeRecord( 1 );
        source.initialize( true, randomFixedReference(), true, randomFixedReference(), 0L );

        writeReadRecord( source, target, NodeRecordFormat.FIXED_FORMAT_RECORD_SIZE );

        assertTrue( target.isUseFixedReferences(), "Record should use fixed reference if can fit in format record." );
        verifySameReferences( source, target );
    }

    @Test
    void readSingleUnitRecordStoredNotInFixedReferenceFormat() throws Exception
    {
        NodeRecord oldFormatRecord = new NodeRecord( 1 );
        NodeRecord newFormatRecord = new NodeRecord( 1 );
        oldFormatRecord.initialize( true, randomFixedReference(), true, randomFixedReference(), 1L );

        writeRecordWithOldFormat( oldFormatRecord );

        assertFalse( oldFormatRecord.hasSecondaryUnitId(), "This should be single unit record." );
        assertTrue( oldFormatRecord.isUseFixedReferences(), "Old format is aware about fixed references." );

        recordFormat.read( newFormatRecord, pageCursor, RecordLoad.NORMAL, RECORD_SIZE, pageCursor.getCurrentPageSize() / RECORD_SIZE );
        verifySameReferences( oldFormatRecord, newFormatRecord );
    }

    @Test
    void readDoubleUnitRecordStoredNotInFixedReferenceFormat() throws Exception
    {
        NodeRecord oldFormatRecord = new NodeRecord( 1 );
        NodeRecord newFormatRecord = new NodeRecord( 1 );
        oldFormatRecord.initialize( true, bigReference(), true, bigReference(), 1L );

        writeRecordWithOldFormat( oldFormatRecord );

        assertTrue( oldFormatRecord.hasSecondaryUnitId(), "This should be double unit record." );
        assertFalse( oldFormatRecord.isUseFixedReferences(), "Old format is not aware about fixed references." );

        recordFormat.read( newFormatRecord, pageCursor, RecordLoad.NORMAL, RECORD_SIZE, pageCursor.getCurrentPageSize() / RECORD_SIZE );
        verifySameReferences( oldFormatRecord, newFormatRecord );
    }

    private void writeRecordWithOldFormat( NodeRecord oldFormatRecord ) throws IOException
    {
        int oldRecordSize = NodeRecordFormatV3_4_0.RECORD_SIZE;
        NodeRecordFormatV3_4_0 recordFormatV30 = new NodeRecordFormatV3_4_0();
        recordFormatV30.prepare( oldFormatRecord, oldRecordSize, idSequence, PageCursorTracer.NULL );
        recordFormatV30.write( oldFormatRecord, pageCursor, oldRecordSize, pageCursor.getCurrentPageSize() / oldRecordSize );
        pageCursor.setOffset( 0 );
    }

    private static void verifySameReferences( NodeRecord recordA, NodeRecord recordB )
    {
        assertEquals( recordA.getNextProp(), recordB.getNextProp(), "Next property field should be the same" );
        assertEquals( recordA.getNextRel(), recordB.getNextRel(), "Next relationship field should be the same." );
        assertEquals( recordA.getLabelField(), recordB.getLabelField(), "Label field should be the same" );
    }

    private void writeReadRecord( NodeRecord source, NodeRecord target ) throws java.io.IOException
    {
        writeReadRecord( source, target, RECORD_SIZE );
    }

    private void writeReadRecord( NodeRecord source, NodeRecord target, int recordSize ) throws java.io.IOException
    {
        recordFormat.prepare( source, recordSize, idSequence, PageCursorTracer.NULL );
        recordFormat.write( source, pageCursor, recordSize, pageCursor.getCurrentPageSize() / recordSize );
        pageCursor.setOffset( 0 );
        recordFormat.read( target, pageCursor, RecordLoad.NORMAL, recordSize, pageCursor.getCurrentPageSize() / recordSize );
    }

    private static long randomFixedReference()
    {
        return randomReference( 1L << (Integer.SIZE + (Byte.SIZE / 2)) );
    }

    private static long randomReference( long maxValue )
    {
        return ThreadLocalRandom.current().nextLong( maxValue );
    }

    private static long bigReference()
    {
        return 1L << 57;
    }
}
