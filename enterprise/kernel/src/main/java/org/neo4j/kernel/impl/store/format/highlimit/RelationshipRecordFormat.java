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
import org.neo4j.kernel.impl.store.format.BaseRecordFormat.FIXED_REFERENCE;
import org.neo4j.kernel.impl.store.record.AbstractBaseRecord;
import org.neo4j.kernel.impl.store.record.Record;
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
    
    private static final long MSB_PROPERTY =    0b0000_0011;
    private static final long MSB_START =       0b0000_0100;
    private static final long MSB_END =         0b0000_1000;
    private static final long MSB_FIRST_PREV =  0b0001_0000;
    private static final long MSB_FIRST_NEXT =  0b0010_0000;
    private static final long MSB_SECOND_PREV = 0b0100_0000;
    private static final long MSB_SECOND_NEXT = 0b1000_0000;
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
        	// [    ,  xx] next prop higher order bits
            // [    , x  ] first node high order bits
            // [    ,x   ] second node high order bits
        	// [   x,    ] first prev high order bits
        	// [  x ,    ] first next high order bits
        	// [ x  ,    ] second prev high order bits
        	// [x   ,    ] second next high order bits
        	long nextProp = cursor.getInt() & 0xFFFF_FFFFL;
            long nextPropMod = (msb & MSB_PROPERTY) << 32;
            
            long firstNode = cursor.getInt() & 0xFFFF_FFFFL;
            long firstNodeMod = (msb & MSB_START) << 30;

            long secondNode = cursor.getInt() & 0xFFFF_FFFFL;
            long secondNodeMod = (msb & MSB_END) << 29;
            
            long firstPrevRel = cursor.getInt() & 0xFFFF_FFFFL;
            long firstPrevRelMod = (msb & MSB_FIRST_PREV) << 28;

            long firstNextRel = cursor.getInt() & 0xFFFF_FFFFL;
            long firstNextRelMod = (msb & MSB_FIRST_NEXT) << 27;

            long secondPrevRel = cursor.getInt() & 0xFFFF_FFFFL;
            long secondPrevRelMod = (msb & MSB_SECOND_PREV) << 26;

            long secondNextRel = cursor.getInt() & 0xFFFF_FFFFL;
            long secondNextRelMod = (msb & MSB_SECOND_NEXT) << 25;

            record.initialize( inUse,
                    BaseRecordFormat.longFromIntAndMod( nextProp, nextPropMod ),
                    BaseRecordFormat.longFromIntAndMod( firstNode, firstNodeMod ),
                    BaseRecordFormat.longFromIntAndMod( secondNode, secondNodeMod ),
                    type,
                    BaseRecordFormat.longFromIntAndMod( firstPrevRel, firstPrevRelMod ),
                    BaseRecordFormat.longFromIntAndMod( firstNextRel, firstNextRelMod ),
                    BaseRecordFormat.longFromIntAndMod( secondPrevRel, secondPrevRelMod ),
                    BaseRecordFormat.longFromIntAndMod( secondNextRel, secondNextRelMod ),
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
        if (!record.isFixedReference())
        {
	        header = set( header, HAS_PROPERTY_BIT, record.getNextProp(), NULL );
	        header = set( header, HAS_FIRST_CHAIN_NEXT_BIT, record.getFirstNextRel(), NULL );
	        header = set( header, HAS_SECOND_CHAIN_NEXT_BIT, record.getSecondNextRel(), NULL );
        }
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
        	long nextProp = record.getNextProp();
            long nextPropMod = nextProp == Record.NO_NEXT_PROPERTY.intValue() ? 0 : 
            								(nextProp & FIXED_REFERENCE.MSB_MASK_PROPERTY_BIT) >> 32;
            
            long firstNode = record.getFirstNode();
            short firstNodeMod = (short)((firstNode & FIXED_REFERENCE.MSB_MASK_NODE_REL_BIT) >> 30);

            long secondNode = record.getSecondNode();
            long secondNodeMod = (secondNode & FIXED_REFERENCE.MSB_MASK_NODE_REL_BIT) >> 29;

            long firstPrevRel = record.getFirstPrevRel();
            long firstPrevRelMod = firstPrevRel == Record.NO_NEXT_RELATIONSHIP.intValue() ? 0 : 
            								(firstPrevRel & FIXED_REFERENCE.MSB_MASK_NODE_REL_BIT) >> 28;

            long firstNextRel = record.getFirstNextRel();
            long firstNextRelMod = firstNextRel == Record.NO_NEXT_RELATIONSHIP.intValue() ? 0 : 
            								(firstNextRel & FIXED_REFERENCE.MSB_MASK_NODE_REL_BIT) >> 27;

            long secondPrevRel = record.getSecondPrevRel();
            long secondPrevRelMod = secondPrevRel == Record.NO_NEXT_RELATIONSHIP.intValue() ? 0 : 
            								(secondPrevRel & FIXED_REFERENCE.MSB_MASK_NODE_REL_BIT) >> 26;

            long secondNextRel = record.getSecondNextRel();
            long secondNextRelMod = secondNextRel == Record.NO_NEXT_RELATIONSHIP.intValue() ? 0 : 
            								(secondNextRel & FIXED_REFERENCE.MSB_MASK_NODE_REL_BIT) >> 25;

        	// [    ,  xx] next prop higher order bits
            // [    , x  ] first node high order bits
            // [    ,x   ] second node high order bits
        	// [   x,    ] first prev high order bits
        	// [  x ,    ] first next high order bits
        	// [ x  ,    ] second prev high order bits
        	// [x   ,    ] second next high order bits
            short msbByte = (short)(nextPropMod | firstNodeMod | secondNodeMod | firstPrevRelMod | firstNextRelMod | 
            						secondPrevRelMod | secondNextRelMod);
            cursor.putByte( (byte)msbByte );
            cursor.putInt( (int) nextProp );
            cursor.putInt( (int) firstNode );
            cursor.putInt( (int) secondNode );
            cursor.putInt( (int) firstPrevRel );
            cursor.putInt( (int) firstNextRel );
            cursor.putInt( (int) secondPrevRel );
            cursor.putInt( (int) secondNextRel );
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
    	if (!FIXED_REFERENCE.ALLOWED)
    		return false;
    	if ((record.getNextProp() != Record.NO_NEXT_PROPERTY.intValue()) && 
    			((record.getNextProp() & FIXED_REFERENCE.MSB_MASK_PROPERTY) != 0))
    		return false;
    	if ((record.getFirstPrevRel() != Record.NO_NEXT_RELATIONSHIP.intValue()) && 
    			((record.getFirstPrevRel() & FIXED_REFERENCE.MSB_MASK_NODE_REL) != 0))
    		return false;
    	if ((record.getFirstNextRel() != Record.NO_NEXT_RELATIONSHIP.intValue()) && 
    			((record.getFirstNextRel() & FIXED_REFERENCE.MSB_MASK_NODE_REL) != 0))
    		return false;
    	if ((record.getSecondPrevRel() != -Record.NO_NEXT_RELATIONSHIP.intValue()) && 
    			((record.getSecondPrevRel() & FIXED_REFERENCE.MSB_MASK_NODE_REL) != 0))
    		return false;
    	if ((record.getSecondNextRel() != -Record.NO_NEXT_RELATIONSHIP.intValue()) && 
    			((record.getSecondNextRel() & FIXED_REFERENCE.MSB_MASK_NODE_REL) != 0))
    		return false;
    	if ((record.getFirstNode() != -Record.NO_NEXT_RELATIONSHIP.intValue()) && 
    			((record.getFirstNode() & FIXED_REFERENCE.MSB_MASK_NODE_REL) != 0))
    		return false;
    	if ((record.getSecondNode() != -Record.NO_NEXT_RELATIONSHIP.intValue()) && 
    			((record.getSecondNode() & FIXED_REFERENCE.MSB_MASK_NODE_REL) != 0))
    		return false;
    	return true;	  			
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
