/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.kernel.impl.store.format.highlimit;

import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.kernel.impl.store.format.BaseRecordFormat;
import org.neo4j.kernel.impl.store.record.RelationshipGroupRecord;

/**
 * LEGEND:
 * V: variable between 3B-8B
 *
 * Record format:
 * 1B   header
 * 2B   relationship type
 * VB   first outgoing relationships
 * VB   first incoming relationships
 * VB   first loop relationships
 * VB   owning node
 * VB   next relationship group record
 * => 18B-43B
 *
 * Fixed reference format:
 * 1B   header
 * 1B   modifiers
 * 2B   relationship type
 * 4B   next relationship
 * 4B   first outgoing relationship
 * 4B   first incoming relationship
 * 4B   first loop
 * 4B   owning node
 * => 24B
 */
public class RelationshipGroupRecordFormat extends BaseHighLimitRecordFormat<RelationshipGroupRecord>
{
    private static final int TYPE_BYTES = 3;

    static final int RECORD_SIZE = 32;
    static final int FIXED_FORMAT_RECORD_SIZE = HEADER_BYTE +
                                                Byte.BYTES /* modifiers */ +
                                                TYPE_BYTES /* type */ +
                                                Integer.BYTES /* next */ +
                                                Integer.BYTES /* first out */ +
                                                Integer.BYTES /* first in */ +
                                                Integer.BYTES /* first loop */ +
                                                Integer.BYTES /* owning node */;

    // For the Dynamic format
    public static final int D_HAS_OUTGOING_BIT = 0b0000_1000;
    public static final int D_HAS_INCOMING_BIT = 0b0001_0000;
    public static final int D_HAS_LOOP_BIT = 0b0010_0000;
    public static final int D_HAS_NEXT_BIT = 0b0100_0000;
    // Ideally we would have wanted to have three bits here, one for each direction, like for the other versions of this format.
    // But there's only one available if we want the format to be backwards compatible. So instead this bit is set if any of
    // the directions have external degrees and if so the stored owning node will be shifted upwards and the three bits stored in its lsb.
    public static final int D_HAS_ANY_EXTERNAL_DEGREES_BIT = 0b1000_0000;
    public static final int D_HAS_DIR_EXTERNAL_DEGREES_MASK = 0b1;
    public static final int D_HAS_OUT_EXTERNAL_DEGREES_SHIFT = 0;
    public static final int D_HAS_IN_EXTERNAL_DEGREES_SHIFT = 1;
    public static final int D_HAS_LOOP_EXTERNAL_DEGREES_SHIFT = 2;
    public static final int D_OWNING_NODE_EXTERNAL_DEGREES_SHIFT = 3;

    // For the Fixed reference format
    private static final int F_NEXT_RECORD_BIT = 0b0000_0001;
    private static final int F_FIRST_OUT_BIT = 0b0000_0010;
    private static final int F_FIRST_IN_BIT = 0b0000_0100;
    private static final int F_FIRST_LOOP_BIT = 0b0000_1000;
    private static final int F_OWNING_NODE_BIT = 0b0001_0000;
    private static final int F_HAS_EXTERNAL_DEGREES_OUT_BIT = 0b0010_0000;
    private static final int F_HAS_EXTERNAL_DEGREES_IN_BIT = 0b0100_0000;
    private static final int F_HAS_EXTERNAL_DEGREES_LOOP_BIT = 0b1000_0000;

    private static final long F_ONE_BIT_OVERFLOW_BIT_MASK = 0xFFFF_FFFE_0000_0000L;
    private static final long F_HIGH_DWORD_LAST_BIT_MASK = 0x100000000L;

    public RelationshipGroupRecordFormat()
    {
        this( RECORD_SIZE );
    }

    public RelationshipGroupRecordFormat( int recordSize )
    {
        super( fixedRecordSize( recordSize ), 0, HighLimitFormatSettings.RELATIONSHIP_GROUP_MAXIMUM_ID_BITS );
    }

    @Override
    public RelationshipGroupRecord newRecord()
    {
        return new RelationshipGroupRecord( -1 );
    }

    @Override
    protected void doReadInternal( RelationshipGroupRecord record, PageCursor cursor, int recordSize, long headerByte,
                                   boolean inUse )
    {
        if ( record.isUseFixedReferences() )
        {
            // read record in fixed references format
            readFixedReferencesMethod( record, cursor, inUse );
            record.setUseFixedReferences( true );
        }
        else
        {
            int type = getType( cursor );
            long firstOut = decodeCompressedReference( cursor, headerByte, D_HAS_OUTGOING_BIT, NULL );
            long firstIn = decodeCompressedReference( cursor, headerByte, D_HAS_INCOMING_BIT, NULL );
            long firstLoop = decodeCompressedReference( cursor, headerByte, D_HAS_LOOP_BIT, NULL );
            long owningNode = decodeCompressedReference( cursor );
            long next = decodeCompressedReference( cursor, headerByte, D_HAS_NEXT_BIT, NULL );
            boolean hasExternalDegreesOut = false;
            boolean hasExternalDegreesIn = false;
            boolean hasExternalDegreesLoop = false;
            boolean hasAnyExternalDegrees = (headerByte & D_HAS_ANY_EXTERNAL_DEGREES_BIT) != 0;
            if ( hasAnyExternalDegrees )
            {
                hasExternalDegreesOut = ((owningNode >> D_HAS_OUT_EXTERNAL_DEGREES_SHIFT) & D_HAS_DIR_EXTERNAL_DEGREES_MASK) != 0;
                hasExternalDegreesIn = ((owningNode >> D_HAS_IN_EXTERNAL_DEGREES_SHIFT) & D_HAS_DIR_EXTERNAL_DEGREES_MASK) != 0;
                hasExternalDegreesLoop = ((owningNode >> D_HAS_LOOP_EXTERNAL_DEGREES_SHIFT) & D_HAS_DIR_EXTERNAL_DEGREES_MASK) != 0;
                owningNode >>= D_OWNING_NODE_EXTERNAL_DEGREES_SHIFT;
            }
            record.initialize( inUse, type, firstOut, firstIn, firstLoop, owningNode, next );
            record.setHasExternalDegreesOut( hasExternalDegreesOut );
            record.setHasExternalDegreesIn( hasExternalDegreesIn );
            record.setHasExternalDegreesLoop( hasExternalDegreesLoop );
        }
    }

    @Override
    protected byte headerBits( RelationshipGroupRecord record )
    {
        byte header = 0;
        header = set( header, D_HAS_OUTGOING_BIT, record.getFirstOut(), NULL );
        header = set( header, D_HAS_INCOMING_BIT, record.getFirstIn(), NULL );
        header = set( header, D_HAS_LOOP_BIT, record.getFirstLoop(), NULL );
        header = set( header, D_HAS_NEXT_BIT, record.getNext(), NULL );
        header = set( header, D_HAS_ANY_EXTERNAL_DEGREES_BIT, hasAnyExternalDegrees( record ) );
        return header;
    }

    // This is called if the format is the dynamic format
    @Override
    protected int requiredDataLength( RelationshipGroupRecord record )
    {
        return TYPE_BYTES + // type
               length( record.getFirstOut(), NULL ) +
               length( record.getFirstIn(), NULL ) +
               length( record.getFirstLoop(), NULL ) +
               length( owningNodeWithExternalDegreesBits( record ) ) +
               length( record.getNext(), NULL );
    }

    private long owningNodeWithExternalDegreesBits( RelationshipGroupRecord record )
    {
        long owningNode = record.getOwningNode();
        if ( hasAnyExternalDegrees( record ) )
        {
            owningNode <<= D_OWNING_NODE_EXTERNAL_DEGREES_SHIFT;
            owningNode |= record.hasExternalDegreesOut() ? (1 << D_HAS_OUT_EXTERNAL_DEGREES_SHIFT) : 0;
            owningNode |= record.hasExternalDegreesIn() ? (1 << D_HAS_IN_EXTERNAL_DEGREES_SHIFT) : 0;
            owningNode |= record.hasExternalDegreesLoop() ? (1 << D_HAS_LOOP_EXTERNAL_DEGREES_SHIFT) : 0;
        }
        return owningNode;
    }

    private boolean hasAnyExternalDegrees( RelationshipGroupRecord record )
    {
        return record.hasExternalDegreesOut() || record.hasExternalDegreesIn() || record.hasExternalDegreesLoop();
    }

    @Override
    protected void doWriteInternal( RelationshipGroupRecord record, PageCursor cursor )
    {
        if ( record.isUseFixedReferences() )
        {
            // write record in fixed references format
            writeFixedReferencesRecord( record, cursor );
        }
        else
        {
            writeType( cursor, record.getType() );
            encode( cursor, record.getFirstOut(), NULL );
            encode( cursor, record.getFirstIn(), NULL );
            encode( cursor, record.getFirstLoop(), NULL );
            encode( cursor, owningNodeWithExternalDegreesBits( record ) );
            encode( cursor, record.getNext(), NULL );
        }
    }

    @Override
    protected boolean canUseFixedReferences( RelationshipGroupRecord record, int recordSize )
    {
        return isRecordBigEnoughForFixedReferences( recordSize ) &&
                (record.getNext() == NULL || (record.getNext() & F_ONE_BIT_OVERFLOW_BIT_MASK) == 0) &&
                (record.getFirstOut() == NULL || (record.getFirstOut() & F_ONE_BIT_OVERFLOW_BIT_MASK) == 0) &&
                (record.getFirstIn() == NULL || (record.getFirstIn() & F_ONE_BIT_OVERFLOW_BIT_MASK) == 0) &&
                (record.getFirstLoop() == NULL || (record.getFirstLoop() & F_ONE_BIT_OVERFLOW_BIT_MASK) == 0) &&
                (record.getOwningNode() == NULL || (record.getOwningNode() & F_ONE_BIT_OVERFLOW_BIT_MASK) == 0);
    }

    private boolean isRecordBigEnoughForFixedReferences( int recordSize )
    {
        return FIXED_FORMAT_RECORD_SIZE <= recordSize;
    }

    private void readFixedReferencesMethod( RelationshipGroupRecord record, PageCursor cursor, boolean inUse )
    {
        // [    ,   x] high next bits
        // [    ,  x ] high firstOut bits
        // [    , x  ] high firstIn bits
        // [    ,x   ] high firstLoop bits
        // [   x,    ] high owner bits
        // [  x ,    ] has external degrees out
        // [ x  ,    ] has external degrees in
        // [x   ,    ] has external degrees loop
        long modifiers = cursor.getByte();

        int type = getType( cursor );

        long nextLowBits = cursor.getInt() & 0xFFFFFFFFL;
        long firstOutLowBits = cursor.getInt() & 0xFFFFFFFFL;
        long firstInLowBits = cursor.getInt() & 0xFFFFFFFFL;
        long firstLoopLowBits = cursor.getInt() & 0xFFFFFFFFL;
        long owningNodeLowBits = cursor.getInt() & 0xFFFFFFFFL;

        long nextMod = (modifiers & F_NEXT_RECORD_BIT) << 32;
        long firstOutMod = (modifiers & F_FIRST_OUT_BIT) << 31;
        long firstInMod = (modifiers & F_FIRST_IN_BIT) << 30;
        long firstLoopMod = (modifiers & F_FIRST_LOOP_BIT) << 29;
        long owningNodeMod = (modifiers & F_OWNING_NODE_BIT) << 28;
        boolean hasExternalDegreesOut = (modifiers & F_HAS_EXTERNAL_DEGREES_OUT_BIT) != 0;
        boolean hasExternalDegreesIn = (modifiers & F_HAS_EXTERNAL_DEGREES_IN_BIT) != 0;
        boolean hasExternalDegreesLoop = (modifiers & F_HAS_EXTERNAL_DEGREES_LOOP_BIT) != 0;

        record.initialize( inUse, type,
                BaseRecordFormat.longFromIntAndMod( firstOutLowBits, firstOutMod ),
                BaseRecordFormat.longFromIntAndMod( firstInLowBits, firstInMod ),
                BaseRecordFormat.longFromIntAndMod( firstLoopLowBits, firstLoopMod ),
                BaseRecordFormat.longFromIntAndMod( owningNodeLowBits, owningNodeMod ),
                BaseRecordFormat.longFromIntAndMod( nextLowBits, nextMod ) );
        record.setHasExternalDegreesOut( hasExternalDegreesOut );
        record.setHasExternalDegreesIn( hasExternalDegreesIn );
        record.setHasExternalDegreesLoop( hasExternalDegreesLoop );
    }

    private void writeFixedReferencesRecord( RelationshipGroupRecord record, PageCursor cursor )
    {
        long nextMod = record.getNext() == NULL ? 0 : (record.getNext() & F_HIGH_DWORD_LAST_BIT_MASK) >> 32;
        long firstOutMod = record.getFirstOut() == NULL ? 0 : (record.getFirstOut() & F_HIGH_DWORD_LAST_BIT_MASK) >> 31;
        long firstInMod = record.getFirstIn() == NULL ? 0 : (record.getFirstIn() & F_HIGH_DWORD_LAST_BIT_MASK) >> 30;
        long firstLoopMod = record.getFirstLoop() == NULL ? 0 : (record.getFirstLoop() & F_HIGH_DWORD_LAST_BIT_MASK) >> 29;
        long owningNodeMod = record.getOwningNode() == NULL ? 0 : (record.getOwningNode() & F_HIGH_DWORD_LAST_BIT_MASK) >> 28;
        long hasExternalDegreesOutMod = record.hasExternalDegreesOut() ? F_HAS_EXTERNAL_DEGREES_OUT_BIT : 0;
        long hasExternalDegreesInMod = record.hasExternalDegreesIn() ? F_HAS_EXTERNAL_DEGREES_IN_BIT : 0;
        long hasExternalDegreesLoopMod = record.hasExternalDegreesLoop() ? F_HAS_EXTERNAL_DEGREES_LOOP_BIT : 0;

        // [    ,   x] high next bits
        // [    ,  x ] high firstOut bits
        // [    , x  ] high firstIn bits
        // [    ,x   ] high firstLoop bits
        // [   x,    ] high owner bits
        // [  x ,    ] has external degrees out
        // [ x  ,    ] has external degrees in
        // [x   ,    ] has external degrees loop
        cursor.putByte( (byte) (nextMod | firstOutMod | firstInMod | firstLoopMod | owningNodeMod | hasExternalDegreesOutMod | hasExternalDegreesInMod |
                hasExternalDegreesLoopMod) );

        writeType( cursor, record.getType() );

        cursor.putInt( (int) record.getNext() );
        cursor.putInt( (int) record.getFirstOut() );
        cursor.putInt( (int) record.getFirstIn() );
        cursor.putInt( (int) record.getFirstLoop() );
        cursor.putInt( (int) record.getOwningNode() );
    }

    private int getType( PageCursor cursor )
    {
        int typeLowWord = cursor.getShort() & 0xFFFF;
        int typeHighByte = cursor.getByte() & 0xFF;
        return (typeHighByte << Short.SIZE) | typeLowWord;
    }

    private void writeType( PageCursor cursor, int type )
    {
        cursor.putShort( (short) type );
        cursor.putByte( (byte) (type >>> Short.SIZE) );
    }
}
