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
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.store.record.Record;

/**
 * LEGEND:
 * V: variable between 3B-8B
 *
 * Record format:
 * 1B   header
 * VB   first relationship
 * VB   first property
 * 5B   labels
 *
 * => 12B-22B
 */
class NodeRecordFormat extends BaseHighLimitRecordFormat<NodeRecord>
{
    static final int RECORD_SIZE = 16;

    private static final long NULL_LABELS = Record.NO_LABELS_FIELD.intValue();
    private static final int DENSE_NODE_BIT       = 0b0000_1000;
    private static final int HAS_RELATIONSHIP_BIT = 0b0001_0000;
    private static final int HAS_PROPERTY_BIT     = 0b0010_0000;
    private static final int HAS_LABELS_BIT       = 0b0100_0000;

    private static final long MSB_PROPERTY =    0b0000_0011;
    private static final long MSB_NEXTREL =     0b0000_0100;
    public NodeRecordFormat()
    {
        this( RECORD_SIZE );
    }

    NodeRecordFormat( int recordSize )
    {
        super( fixedRecordSize( recordSize ), 0 );
    }

    @Override
    public NodeRecord newRecord()
    {
        return new NodeRecord( -1 );
    }

    @Override
    protected void doReadInternal( NodeRecord record, PageCursor cursor, int recordSize, long headerByte,
                                   boolean inUse )
    {
        // Interpret the header byte
        boolean dense = has( headerByte, DENSE_NODE_BIT );
        if (record.isFixedReference())
        {
        	byte msb = cursor.getByte();
        	// [    ,  xx] next prop higher order bits
            // [    , x  ] next rel high order bit
            long nextPropMod = (msb & MSB_PROPERTY) << 32;
            long nextRelMod = (msb & MSB_NEXTREL) << 30;
            
            long nextRel = cursor.getInt() & 0xFFFF_FFFFL;          
        	long nextProp = cursor.getInt() & 0xFFFF_FFFFL;

        	// get the most significant byte of 5-byte label field 
            long hsbLabels = cursor.getByte() & 0xFF; // so that a negative byte won't fill the "extended" bits with ones.
            long lsbLabels = cursor.getInt() & 0xFFFFFFFFL;       
            long labels = lsbLabels | (hsbLabels << 32);

        	record.initialize( inUse, 
        			BaseRecordFormat.longFromIntAndMod( nextProp, nextPropMod ), 
        			dense, 
        			BaseRecordFormat.longFromIntAndMod( nextRel, nextRelMod ), 
        			labels );
        }
        else
        {
            // Now read the rest of the data. The adapter will take care of moving the cursor over to the
            // other unit when we've exhausted the first one.
            long nextRel = decode( cursor, headerByte, HAS_RELATIONSHIP_BIT, NULL );
            long nextProp = decode( cursor, headerByte, HAS_PROPERTY_BIT, NULL );
            long labelField = decode( cursor, headerByte, HAS_LABELS_BIT, NULL_LABELS );
            record.initialize( inUse, nextProp, dense, nextRel, labelField );
        }
    }

    @Override
    public int requiredDataLength( NodeRecord record )
    {
        return  length( record.getNextRel(), NULL ) +
                length( record.getNextProp(), NULL ) +
                length( record.getLabelField(), NULL_LABELS );
    }

    @Override
    protected byte headerBits( NodeRecord record )
    {
        byte header = 0;
        header = set( header, DENSE_NODE_BIT, record.isDense() );
        header = set( header, HAS_RELATIONSHIP_BIT, record.getNextRel(), NULL );
        header = set( header, HAS_PROPERTY_BIT, record.getNextProp(), NULL );
        header = set( header, HAS_LABELS_BIT, record.getLabelField(), NULL_LABELS );
        return header;
    }

    @Override
    protected void doWriteInternal( NodeRecord record, PageCursor cursor )
            throws IOException
    {
    	if (record.isFixedReference())
    	{
    		long nextProp = record.getNextProp();
            long nextPropMod = nextProp == Record.NO_NEXT_PROPERTY.intValue() ? 0 : (nextProp & MSB_MASK_PROPERTY_BIT) >> 32;
            
            long nextRel = record.getNextRel();
            long nextRelMod = nextRel == Record.NO_NEXT_RELATIONSHIP.intValue() ? 0 : (nextRel & MSB_MASK_NODE_REL_BIT) >> 30;
            // [    ,  xx] next prop higher order bits
            // [    , x  ] next rel high order bits
            short msbByte = (short) ( nextRelMod | nextPropMod );
    		cursor.putByte( (byte)msbByte );
    		
    		cursor.putInt((int)record.getNextRel());
    		cursor.putInt((int)record.getNextProp());
   		
    		long labelField = record.getLabelField();
    		// msb of labels
            cursor.putByte( (byte) ((labelField & 0xFF00000000L) >> 32) );
    		// lsb of labels
            cursor.putInt( (int) labelField );
    	}
    	else
    	{
            encode( cursor, record.getNextRel(), NULL );
            encode( cursor, record.getNextProp(), NULL );
            encode( cursor, record.getLabelField(), NULL_LABELS );
    	}
    }

    @Override
    protected boolean canBeFixedReference( NodeRecord record )
    {
    	if ((record.getNextProp() != -1) && ((record.getNextProp() & MSB_MASK_PROPERTY) != 0))
    		return false;
    	if ((record.getNextRel() != -1) && ((record.getNextRel() & MSB_MASK_NODE_REL) != 0))
    		return false;
    	return true;	  			
    }
}
