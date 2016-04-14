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

    private static final int MSB_OUT =       0b0000_0001;
    private static final int MSB_IN  =       0b0000_0010;
    private static final int MSB_LOOP=       0b0000_0100;
    private static final int MSB_NEXT =      0b0000_1000;
    private static final int MSB_OWNER =     0b0001_0000;
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
    		byte msb = cursor.getByte();
    		record.initialize( inUse,
	                type,
	                Reference.readFixed(cursor, msb, MSB_OUT),
	                Reference.readFixed(cursor, msb, MSB_IN),
	                Reference.readFixed(cursor, msb, MSB_LOOP),
	                Reference.readFixed(cursor, msb, MSB_OWNER),
	                Reference.readFixed(cursor, msb, MSB_NEXT));	                
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
    		byte msb = getMSB( record );
    		cursor.putByte(msb);
    		cursor.putInt((int)record.getFirstOut());
    		cursor.putInt((int)record.getFirstIn());
    		cursor.putInt((int)record.getFirstLoop());
    		cursor.putInt((int)record.getOwningNode());
    		cursor.putInt((int)record.getNext());
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
    	long canBeFixedRef = (record.getFirstOut() & MSB_MASK_NODE_REL) |
    			(record.getFirstIn() & MSB_MASK_NODE_REL) |
    			(record.getFirstLoop() & MSB_MASK_NODE_REL) |
    			(record.getOwningNode() & MSB_MASK_NODE_REL) |
    			(record.getNext() & MSB_MASK_NODE_REL);
    	return canBeFixedRef == 0;	  			
    }
    
    @Override
    protected byte getMSB( RelationshipGroupRecord record)
    {
    	byte msb = 0;
    	msb = (record.getFirstOut() & MSB_MASK_NODE_REL) > 0 ? (byte)(msb | MSB_OUT) : msb;
    	msb = (record.getFirstIn() & MSB_MASK_NODE_REL) > 0 ? (byte)(msb | MSB_IN) : msb;
    	msb = (record.getFirstLoop() & MSB_MASK_NODE_REL) > 0 ? (byte)(msb | MSB_LOOP) : msb;
    	msb = (record.getOwningNode() & MSB_MASK_NODE_REL) > 0 ? (byte)(msb | MSB_OWNER) : msb;
    	msb = (record.getNext() & MSB_MASK_NODE_REL) > 0 ? (byte)(msb | MSB_NEXT) : msb;
    	return msb;
    }
}
