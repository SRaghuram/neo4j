/*
 * Copyright (c) 2002-2016 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.kernel.impl.store.format.highlimit;

import java.io.IOException;

import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.kernel.impl.store.format.BaseRecordFormat;
import org.neo4j.kernel.impl.store.record.Record;
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
 *
 * => 18B-43B
 */
class RelationshipGroupRecordFormat extends BaseHighLimitRecordFormat<RelationshipGroupRecord>
{
    static final int RECORD_SIZE = 32;

    private static final int HAS_OUTGOING_BIT = 0b0000_1000;
    private static final int HAS_INCOMING_BIT = 0b0001_0000;
    private static final int HAS_LOOP_BIT     = 0b0010_0000;
    private static final int HAS_NEXT_BIT     = 0b0100_0000;

    private static final long MSB_OUT =       0b0000_0001;
    private static final long MSB_IN  =       0b0000_0010;
    private static final long MSB_LOOP=       0b0000_0100;
    private static final long MSB_NEXT =      0b0000_1000;
    private static final long MSB_OWNER =     0b0001_0000;
    public RelationshipGroupRecordFormat()
    {
        this( RECORD_SIZE );
    }

    RelationshipGroupRecordFormat( int recordSize )
    {
        super( fixedRecordSize( recordSize ), 0 );
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
    	if (record.isFixedReference())
    	{
    		int type = cursor.getShort() & 0xFFFF;
            // [    ,   x] high firstOut bits
            // [    ,  x ] high firstIn bits
    		// [    , x  ] high firstLoop bits
            // [    ,x   ] high Owner bits
    		// [   x,    ] high next bits
            long highByte = cursor.getByte();
            long nextOutMod = (highByte & MSB_OUT) << 32;
            long nextInMod = (highByte & MSB_IN) << 31;
            long nextLoopMod = (highByte & MSB_LOOP) << 30;
            long nextMod = (highByte & MSB_NEXT) << 29;
            long ownerMod = (highByte & MSB_OWNER) << 28;
            
            long nextOutLowBits = cursor.getInt() & 0xFFFFFFFFL;
            long nextInLowBits = cursor.getInt() & 0xFFFFFFFFL;
            long nextLoopLowBits = cursor.getInt() & 0xFFFFFFFFL;
            long nextLowBits = cursor.getInt() & 0xFFFFFFFFL;
            long owningNode = cursor.getInt() & 0xFFFFFFFFL;

            record.initialize( inUse, type,
                    BaseRecordFormat.longFromIntAndMod( nextOutLowBits, nextOutMod ),
                    BaseRecordFormat.longFromIntAndMod( nextInLowBits, nextInMod ),
                    BaseRecordFormat.longFromIntAndMod( nextLoopLowBits, nextLoopMod ),
                    BaseRecordFormat.longFromIntAndMod( owningNode, ownerMod ),
                    BaseRecordFormat.longFromIntAndMod( nextLowBits, nextMod ) );              
    	}
    	else
    	{
    		record.initialize( inUse,
                    cursor.getShort() & 0xFFFF,
                    decode( cursor, headerByte, HAS_OUTGOING_BIT, NULL ),
                    decode( cursor, headerByte, HAS_INCOMING_BIT, NULL ),
                    decode( cursor, headerByte, HAS_LOOP_BIT, NULL ),
                    decode( cursor ),
                    decode( cursor, headerByte, HAS_NEXT_BIT, NULL ) );
    	}
    }

    @Override
    protected byte headerBits( RelationshipGroupRecord record )
    {
        byte header = 0;
        header = set( header, HAS_OUTGOING_BIT, record.getFirstOut(), NULL );
        header = set( header, HAS_INCOMING_BIT, record.getFirstIn(), NULL );
        header = set( header, HAS_LOOP_BIT, record.getFirstLoop(), NULL );
        header = set( header, HAS_NEXT_BIT, record.getNext(), NULL );
        return header;
    }

    @Override
    protected int requiredDataLength( RelationshipGroupRecord record )
    {
        return  2 + // type
                length( record.getFirstOut(), NULL ) +
                length( record.getFirstIn(), NULL ) +
                length( record.getFirstLoop(), NULL ) +
                length( record.getOwningNode() ) +
                length( record.getNext(), NULL );
    }

    @Override
    protected void doWriteInternal( RelationshipGroupRecord record, PageCursor cursor )
            throws IOException
    {
    	if (record.isFixedReference())
    	{
    		cursor.putShort( (short) record.getType() );
            long nextOutMod = record.getFirstOut() == Record.NO_NEXT_RELATIONSHIP.intValue() ? 0 : (record.getFirstOut() & MSB_MASK_NODE_REL_BIT) >> 32;
            long nextInMod = record.getFirstIn() == Record.NO_NEXT_RELATIONSHIP.intValue() ? 0 : (record.getFirstIn() & MSB_MASK_NODE_REL_BIT) >> 31;
            long nextLoopMod = record.getFirstLoop() == Record.NO_NEXT_RELATIONSHIP.intValue() ? 0 : (record.getFirstLoop() & MSB_MASK_NODE_REL_BIT) >> 30;
            long nextMod = record.getNext() == Record.NO_NEXT_RELATIONSHIP.intValue() ? 0 : (record.getNext() & MSB_MASK_NODE_REL_BIT) >> 29;
            long ownerMod = record.getOwningNode() == Record.NO_NEXT_RELATIONSHIP.intValue() ? 0 :(record.getOwningNode() & MSB_MASK_NODE_REL_BIT) >> 28;
            // [    ,   x] high firstOut bits
            // [    ,  x ] high firstIn bits
    		// [    , x  ] high firstLoop bits
            // [    ,x   ] high Owner bits
    		// [   x,    ] high next bits
            cursor.putByte( (byte) (nextOutMod | nextMod | nextLoopMod | nextInMod | ownerMod) );

            cursor.putInt( (int) record.getFirstOut() );
            cursor.putInt( (int) record.getFirstIn() );
            cursor.putInt( (int) record.getFirstLoop() );
            cursor.putInt( (int) record.getNext() );
            cursor.putInt( (int) record.getOwningNode() );
    	}
    	else
    	{
            cursor.putShort( (short) record.getType() );
            encode( cursor, record.getFirstOut(), NULL );
            encode( cursor, record.getFirstIn(), NULL );
            encode( cursor, record.getFirstLoop(), NULL );
            encode( cursor, record.getOwningNode() );
            encode( cursor, record.getNext(), NULL );
    	}
    }
    
    @Override
    protected boolean canBeFixedReference( RelationshipGroupRecord record )
    {
    	if ((record.getFirstOut() != -1) && ((record.getFirstOut() & MSB_MASK_PROPERTY) != 0))
    		return false;
    	if ((record.getFirstIn() != -1) && ((record.getFirstIn() & MSB_MASK_NODE_REL) != 0))
    		return false;
    	if ((record.getFirstLoop() != -1) && ((record.getFirstLoop() & MSB_MASK_PROPERTY) != 0))
    		return false;
    	if ((record.getNext() != -1) && ((record.getNext() & MSB_MASK_NODE_REL) != 0))
    		return false;
    	return true;	  					
    }
}
