/*
 * Copyright (c) 2002-2017 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.kernel.impl.store;

import java.util.function.ToIntFunction;

import org.neo4j.kernel.impl.store.id.BatchingIdSequence;
import org.neo4j.kernel.impl.store.record.PropertyBlock;
import org.neo4j.values.storable.Value;

import static java.lang.Math.toIntExact;

/**
 * Calculates record size that property values will occupy if encoded into a {@link PropertyStore}.
 * Contains state and is designed for multiple uses from a single thread only.
 * Does actual encoding of property values, dry-run style.
 */
class PropertyValueRecordSizeCalculator implements ToIntFunction<Value[]>
{
    private final BatchingIdSequence stringRecordIds = new BatchingIdSequence();
    private final DynamicRecordAllocator stringRecordCounter;
    private final BatchingIdSequence arrayRecordIds = new BatchingIdSequence();
    private final DynamicRecordAllocator arrayRecordCounter;
    private final DynamicStringStore stringStore;
    private final DynamicArrayStore arrayStore;
    private final PropertyStore propertyStore;

    PropertyValueRecordSizeCalculator( PropertyStore propertyStore )
    {
        this.propertyStore = propertyStore;
        this.stringStore = propertyStore.getStringStore();
        this.stringRecordCounter = new StandardDynamicRecordAllocator( stringRecordIds, stringStore.getRecordDataSize() );
        this.arrayStore = propertyStore.getArrayStore();
        this.arrayRecordCounter = new StandardDynamicRecordAllocator( arrayRecordIds, arrayStore.getRecordDataSize() );
    }

    @Override
    public int applyAsInt( Value[] values )
    {
        stringRecordIds.reset();
        arrayRecordIds.reset();

        int propertyRecordsUsed = 0;
        int freeBlocksInCurrentRecord = 0;
        for ( Value value : values )
        {
            PropertyBlock block = new PropertyBlock();
            PropertyStore.encodeValue( block, 0 /*doesn't matter*/, value, stringRecordCounter, arrayRecordCounter );
            if ( block.getValueBlocks().length > freeBlocksInCurrentRecord )
            {
                propertyRecordsUsed++;
                freeBlocksInCurrentRecord = PropertyType.getPayloadSizeLongs();
            }
            freeBlocksInCurrentRecord -= block.getValueBlocks().length;
        }

        int size = propertyRecordsUsed * propertyStore.getRecordSize();
        size += toIntExact( stringRecordIds.peek() ) * stringStore.getRecordSize();
        size += toIntExact( arrayRecordIds.peek() ) * arrayStore.getRecordSize();
        return size;
    }
}
