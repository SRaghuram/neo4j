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
import org.neo4j.kernel.impl.store.record.RelationshipRecord;

import static org.neo4j.kernel.impl.store.format.highlimit.Reference.toAbsolute;
import static org.neo4j.kernel.impl.store.format.highlimit.Reference.toRelative;

/**
 * LEGEND:
 * V: variable between 3B-8B
 *
 * Record format:
 * 1B   header
 * 2B   relationship type
 * VB   first property
 * VB   start node
 * VB   end node
 * VB   start node chain previous relationship
 * VB   start node chain next relationship
 * VB   end node chain previous relationship
 * VB   end node chain next relationship
 *
 * => 24B-59B
 */
class RelationshipRecordFormat extends BaseHighLimitRecordFormat<RelationshipRecord>
{
    static final int RECORD_SIZE = 32;

    private static final int FIRST_IN_FIRST_CHAIN_BIT = 0b0000_1000;
    private static final int FIRST_IN_SECOND_CHAIN_BIT = 0b0001_0000;
    private static final int HAS_FIRST_CHAIN_NEXT_BIT = 0b0010_0000;
    private static final int HAS_SECOND_CHAIN_NEXT_BIT = 0b0100_0000;
    private static final int HAS_PROPERTY_BIT = 0b1000_0000;
    
    private static final int MSB_PROPERTY =    0b0000_0011;
    private static final int MSB_START =       0b0000_0100;
    private static final int MSB_END =         0b0000_1000;
    private static final int MSB_FIRST_PREV =  0b0001_0000;
    private static final int MSB_FIRST_NEXT =  0b0010_0000;
    private static final int MSB_SECOND_PREV = 0b0100_0000;
    private static final int MSB_SECOND_NEXT = 0b1000_0000;
    public RelationshipRecordFormat()
    {
        this( RECORD_SIZE );
    }

    RelationshipRecordFormat( int recordSize )
    {
        super( fixedRecordSize( recordSize ), 0 );
    }

    @Override
    public RelationshipRecord newRecord()
    {
        return new RelationshipRecord( -1 );
    }
    
    @Override
    protected void doReadInternal( RelationshipRecord record, PageCursor cursor, int recordSize, long headerByte,
                                   boolean inUse )
    {
        int type = cursor.getShort() & 0xFFFF;
        long recordId = record.getId();
        if (record.isFixedReference())
        {
        	byte msb = cursor.getByte();
        	record.initialize( inUse,
                    Reference.readFixed( cursor, msb, MSB_PROPERTY, true),
                    Reference.readFixed( cursor, msb, MSB_START),
                    Reference.readFixed( cursor, msb, MSB_END),
                    type,
                    Reference.readFixed( cursor, msb, MSB_FIRST_PREV),
                    Reference.readFixed( cursor, msb, MSB_FIRST_NEXT),
                    Reference.readFixed( cursor, msb, MSB_SECOND_PREV),
                    Reference.readFixed( cursor, msb, MSB_SECOND_NEXT),
                    has( headerByte, FIRST_IN_FIRST_CHAIN_BIT ),
                    has( headerByte, FIRST_IN_SECOND_CHAIN_BIT ) );
        	
        } 
        else
        {
            record.initialize( inUse,
                    decode( cursor, headerByte, HAS_PROPERTY_BIT, NULL ),
                    decode( cursor ),
                    decode( cursor ),
                    type,
                    decodeAbsoluteOrRelative( cursor, headerByte, FIRST_IN_FIRST_CHAIN_BIT, recordId ),
                    decodeAbsoluteIfPresent( cursor, headerByte, HAS_FIRST_CHAIN_NEXT_BIT, recordId ),
                    decodeAbsoluteOrRelative( cursor, headerByte, FIRST_IN_SECOND_CHAIN_BIT, recordId ),
                    decodeAbsoluteIfPresent( cursor, headerByte, HAS_SECOND_CHAIN_NEXT_BIT, recordId ),
                    has( headerByte, FIRST_IN_FIRST_CHAIN_BIT ),
                    has( headerByte, FIRST_IN_SECOND_CHAIN_BIT ) );
        }       
    }

    private long decodeAbsoluteOrRelative( PageCursor cursor, long headerByte, int firstInStartBit, long recordId )
    {
        return has( headerByte, firstInStartBit ) ? decode( cursor ) : toAbsolute( decode( cursor ), recordId );
    }

    @Override
    protected byte headerBits( RelationshipRecord record )
    {
        byte header = 0;
        header = set( header, FIRST_IN_FIRST_CHAIN_BIT, record.isFirstInFirstChain() );
        header = set( header, FIRST_IN_SECOND_CHAIN_BIT, record.isFirstInSecondChain() );
        header = set( header, HAS_PROPERTY_BIT, record.getNextProp(), NULL );
        header = set( header, HAS_FIRST_CHAIN_NEXT_BIT, record.getFirstNextRel(), NULL );
        header = set( header, HAS_SECOND_CHAIN_NEXT_BIT, record.getSecondNextRel(), NULL );
        return header;
    }

    @Override
    protected int requiredDataLength( RelationshipRecord record )
    {
        long recordId = record.getId();
        return Short.BYTES + // type
               length( record.getNextProp(), NULL ) +
               length( record.getFirstNode() ) +
               length( record.getSecondNode() ) +
               length( getFirstPrevReference( record, recordId ) ) +
               getRelativeReferenceLength( record.getFirstNextRel(), recordId ) +
               length( getSecondPrevReference( record, recordId ) ) +
               getRelativeReferenceLength( record.getSecondNextRel(), recordId );
    }

    @Override
    protected void doWriteInternal( RelationshipRecord record, PageCursor cursor )
            throws IOException
    {
        cursor.putShort( (short) record.getType() );
        long recordId = record.getId();
        if (record.isFixedReference())
        {
        	byte msb = getMSB( record );
        	cursor.putByte( msb );
        	cursor.putInt((int) (record.getNextProp() & INT_MASK));
        	cursor.putInt((int) (record.getFirstNode() & INT_MASK));
        	cursor.putInt((int) (record.getSecondNode() & INT_MASK));
        	cursor.putInt((int) (record.getFirstPrevRel() & INT_MASK));
        	cursor.putInt((int) (record.getFirstNextRel() & INT_MASK));
        	cursor.putInt((int) (record.getSecondPrevRel() & INT_MASK));
        	cursor.putInt((int) (record.getSecondNextRel() & INT_MASK));
        }
        else
        {
        	encode( cursor, record.getNextProp(), NULL );
            encode( cursor, record.getFirstNode() );
            encode( cursor, record.getSecondNode() );

            encode( cursor, getFirstPrevReference( record, recordId ) );
            if ( record.getFirstNextRel() != NULL )
            {
                encode( cursor, toRelative( record.getFirstNextRel(), recordId ) );
            }
            encode( cursor, getSecondPrevReference( record, recordId ) );
            if ( record.getSecondNextRel() != NULL )
            {
                encode( cursor, toRelative( record.getSecondNextRel(), recordId ) );
	        }
        }
    }

    @Override
    protected boolean canBeFixedReference( RelationshipRecord record )
    {
    	if ((record.getNextProp() != -1) && ((record.getNextProp() & MSB_MASK_PROPERTY) != 0))
    		return false;
    	if ((record.getFirstNode() != -1) && ((record.getFirstNode() & MSB_MASK_NODE_REL) != 0))
    		return false;
    	if ((record.getSecondNode() != -1) && ((record.getSecondNode() & MSB_MASK_NODE_REL) != 0))
    		return false;
    	if ((record.getFirstPrevRel() != -1) && ((record.getFirstPrevRel() & MSB_MASK_NODE_REL) != 0))
    		return false;
    	if ((record.getFirstNextRel() != -1) && ((record.getFirstNextRel() & MSB_MASK_NODE_REL) != 0))
    		return false;
    	if ((record.getSecondPrevRel() != -1) && ((record.getSecondPrevRel() & MSB_MASK_NODE_REL) != 0))
    		return false;
    	if ((record.getSecondNextRel() != -1) && ((record.getSecondNextRel() & MSB_MASK_NODE_REL) != 0))
    		return false;
    	System.out.println("Fixed:"+record.toString());
    	return true;	  			
    }
    
    @Override
    protected byte getMSB( RelationshipRecord record)
    {
    	byte msb = 0;
    	msb = (record.getFirstNode() & MSB_MASK_NODE_REL_BIT) > 0 ? (byte)(msb | MSB_START) : msb;
    	msb = (record.getSecondNode() & MSB_MASK_NODE_REL_BIT) > 0 ? (byte)(msb | MSB_END) : msb;
    	msb = (record.getFirstPrevRel() & MSB_MASK_NODE_REL_BIT) > 0 ? (byte)(msb | MSB_FIRST_PREV) : msb;
    	msb = (record.getFirstNextRel() & MSB_MASK_NODE_REL_BIT) > 0 ? (byte)(msb | MSB_FIRST_NEXT) : msb;
    	msb = (record.getSecondPrevRel() & MSB_MASK_NODE_REL_BIT) > 0 ? (byte)(msb | MSB_SECOND_PREV) : msb;
    	msb = (record.getSecondNextRel() & MSB_MASK_NODE_REL_BIT) > 0 ? (byte)(msb | MSB_SECOND_NEXT) : msb;
    	if ((record.getNextProp() & MSB_MASK_PROPERTY_BIT) > 0)
    	{
    		int propBits = (int)((record.getNextProp() & MSB_MASK_PROPERTY_BIT)>>32);
			msb = (byte)(msb | propBits);
		}
    	return msb;
    }
    
    private long getSecondPrevReference( RelationshipRecord record, long recordId )
    {
        return record.isFirstInSecondChain() ? record.getSecondPrevRel() :
                                   toRelative( record.getSecondPrevRel(), recordId );
    }

    private long getFirstPrevReference( RelationshipRecord record, long recordId )
    {
        return record.isFirstInFirstChain() ? record.getFirstPrevRel()
                                              : toRelative( record.getFirstPrevRel(), recordId );
    }

    private int getRelativeReferenceLength( long absoluteReference, long recordId )
    {
        return absoluteReference != NULL ? length( toRelative( absoluteReference, recordId ) ) : 0;
    }

    private long decodeAbsoluteIfPresent( PageCursor cursor, long headerByte, int conditionBit, long recordId )
    {
    	return has( headerByte, conditionBit ) ? toAbsolute( decode( cursor ), recordId ) : NULL;
    }
}
