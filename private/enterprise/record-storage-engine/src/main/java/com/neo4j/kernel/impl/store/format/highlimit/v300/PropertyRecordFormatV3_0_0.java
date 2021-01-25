/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.kernel.impl.store.format.highlimit.v300;

import com.neo4j.kernel.impl.store.format.highlimit.Reference;

import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.kernel.impl.store.format.BaseOneByteHeaderRecordFormat;
import org.neo4j.kernel.impl.store.record.PropertyBlock;
import org.neo4j.kernel.impl.store.record.PropertyRecord;
import org.neo4j.kernel.impl.store.record.RecordLoad;

/**
 * LEGEND:
 * V: variable between 3B-8B
 *
 * Record format:
 * 1B   header
 * VB   previous property
 * VB   next property
 * 8B   property block
 * 8B   property block
 * 8B   property block
 * 8B   property block
 *
 * => 39B-49B
 */
public class PropertyRecordFormatV3_0_0 extends BaseOneByteHeaderRecordFormat<PropertyRecord>
{
    public static final int RECORD_SIZE = 48;

    public PropertyRecordFormatV3_0_0()
    {
        super( fixedRecordSize( RECORD_SIZE ), 0, IN_USE_BIT, HighLimitV3_0_0.DEFAULT_MAXIMUM_BITS_PER_ID, false );
    }

    @Override
    public PropertyRecord newRecord()
    {
        return new PropertyRecord( -1 );
    }

    @Override
    public void read( PropertyRecord record, PageCursor cursor, RecordLoad mode, int recordSize, int recordsPerPage )
    {
        int offset = cursor.getOffset();
        byte headerByte = cursor.getByte();
        boolean inUse = isInUse( headerByte );
        if ( mode.shouldLoad( inUse ) )
        {
            int blockCount = headerByte >>> 4;
            long recordId = record.getId();
            record.initialize( inUse,
                    Reference.toAbsolute( Reference.decode( cursor ), recordId ),
                    Reference.toAbsolute( Reference.decode( cursor ), recordId ) );
            if ( (blockCount > record.getBlockCapacity()) | (RECORD_SIZE - (cursor.getOffset() - offset) < blockCount * Long.BYTES) )
            {
                cursor.setCursorException( "PropertyRecord claims to contain more blocks than can fit in a record" );
                return;
            }
            while ( blockCount-- > 0 )
            {
                record.addLoadedBlock( cursor.getLong() );
            }
        }
    }

    @Override
    public void write( PropertyRecord record, PageCursor cursor, int recordSize, int recordsPerPage )
    {
        if ( record.inUse() )
        {
            cursor.putByte( (byte) (IN_USE_BIT | numberOfBlocks( record ) << 4) );
            long recordId = record.getId();
            Reference.encode( Reference.toRelative( record.getPrevProp(), recordId), cursor );
            Reference.encode( Reference.toRelative( record.getNextProp(), recordId), cursor );
            for ( PropertyBlock block : record )
            {
                for ( long propertyBlock : block.getValueBlocks() )
                {
                    cursor.putLong( propertyBlock );
                }
            }
        }
        else
        {
            markAsUnused( cursor );
        }
    }

    private int numberOfBlocks( PropertyRecord record )
    {
        int count = 0;
        for ( PropertyBlock block : record )
        {
            count += block.getValueBlocks().length;
        }
        return count;
    }

    @Override
    public long getNextRecordReference( PropertyRecord record )
    {
        return record.getNextProp();
    }
}
