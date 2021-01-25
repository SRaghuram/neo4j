/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.internal.recordstorage;

import java.util.Iterator;
import java.util.function.Consumer;

import org.neo4j.internal.id.IdSequence;
import org.neo4j.internal.recordstorage.RecordAccess.RecordProxy;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer;
import org.neo4j.kernel.impl.store.DynamicRecordAllocator;
import org.neo4j.kernel.impl.store.PropertyStore;
import org.neo4j.kernel.impl.store.PropertyType;
import org.neo4j.kernel.impl.store.record.DynamicRecord;
import org.neo4j.kernel.impl.store.record.PrimitiveRecord;
import org.neo4j.kernel.impl.store.record.PropertyBlock;
import org.neo4j.kernel.impl.store.record.PropertyRecord;
import org.neo4j.kernel.impl.store.record.Record;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.values.storable.Value;

public class PropertyCreator
{
    private final DynamicRecordAllocator stringRecordAllocator;
    private final DynamicRecordAllocator arrayRecordAllocator;
    private final IdSequence propertyRecordIdGenerator;
    private final PropertyTraverser traverser;
    private final boolean allowStorePointsAndTemporal;
    private final PageCursorTracer cursorTracer;
    private final MemoryTracker memoryTracker;

    public PropertyCreator( PropertyStore propertyStore, PropertyTraverser traverser, PageCursorTracer cursorTracer, MemoryTracker memoryTracker )
    {
        this( propertyStore.getStringStore(), propertyStore.getArrayStore(), propertyStore, traverser, propertyStore.allowStorePointsAndTemporal(),
                cursorTracer, memoryTracker );
    }

    PropertyCreator( DynamicRecordAllocator stringRecordAllocator, DynamicRecordAllocator arrayRecordAllocator, IdSequence propertyRecordIdGenerator,
            PropertyTraverser traverser, boolean allowStorePointsAndTemporal, PageCursorTracer cursorTracer, MemoryTracker memoryTracker )
    {
        this.stringRecordAllocator = stringRecordAllocator;
        this.arrayRecordAllocator = arrayRecordAllocator;
        this.propertyRecordIdGenerator = propertyRecordIdGenerator;
        this.traverser = traverser;
        this.allowStorePointsAndTemporal = allowStorePointsAndTemporal;
        this.cursorTracer = cursorTracer;
        this.memoryTracker = memoryTracker;
    }

    public <P extends PrimitiveRecord> void primitiveSetProperty(
            RecordProxy<P, ?> primitiveRecordChange, int propertyKey, Value value,
            RecordAccess<PropertyRecord, PrimitiveRecord> propertyRecords )
    {
        PropertyBlock block = encodePropertyValue( propertyKey, value );
        P primitive = primitiveRecordChange.forReadingLinkage();
        assert traverser.assertPropertyChain( primitive, propertyRecords );

        // Traverse the existing property chain. Tracking two things along the way:
        // - (a) Free space for this block (candidateHost)
        // - (b) Existence of a block with the property key
        // Chain traversal can be aborted only if:
        // - (1) (b) occurs and new property block fits where the current is
        // - (2) (a) occurs and (b) has occurred, but new property block didn't fit
        // - (3) (b) occurs and (a) has occurred
        // - (4) Chain ends
        RecordProxy<PropertyRecord, PrimitiveRecord> freeHostProxy = null;
        RecordProxy<PropertyRecord, PrimitiveRecord> existingHostProxy = null;
        long prop = primitive.getNextProp();
        while ( prop != Record.NO_NEXT_PROPERTY.intValue() ) // <-- (4)
        {
            RecordProxy<PropertyRecord, PrimitiveRecord> proxy = propertyRecords.getOrLoad( prop, primitive, cursorTracer );
            PropertyRecord propRecord = proxy.forReadingLinkage();
            assert propRecord.inUse() : propRecord;

            // (a) search for free space
            if ( propRecord.hasSpaceFor( block ) )
            {
                freeHostProxy = proxy;
                if ( existingHostProxy != null )
                {
                    // (2)
                    PropertyRecord freeHost = proxy.forChangingData();
                    freeHost.addPropertyBlock( block );
                    freeHost.setChanged( primitive );
                    assert traverser.assertPropertyChain( primitive, propertyRecords );
                    return;
                }
            }

            // (b) search for existence of property key
            PropertyBlock existingBlock = propRecord.getPropertyBlock( propertyKey );
            if ( existingBlock != null )
            {
                // We found an existing property and whatever happens we have to remove the existing
                // block so that we can add the new one, where ever we decide to place it
                existingHostProxy = proxy;
                PropertyRecord existingHost = existingHostProxy.forChangingData();
                removeProperty( primitive, existingHost, existingBlock );

                // Now see if we at this point can add the new block
                if ( block.getSize() <= existingBlock.getSize() || // cheap check
                     existingHost.hasSpaceFor( block ) ) // fallback check
                {
                    // (1) yes we could add it right into the host of the existing block
                    existingHost.addPropertyBlock( block );
                    assert traverser.assertPropertyChain( primitive, propertyRecords );
                    return;
                }
                else if ( freeHostProxy != null )
                {
                    // (3) yes we could add it to a previously found host with sufficiently free space in it
                    PropertyRecord freeHost = freeHostProxy.forChangingData();
                    freeHost.addPropertyBlock( block );
                    freeHost.setChanged( primitive );
                    assert traverser.assertPropertyChain( primitive, propertyRecords );
                    return;
                }
                // else we can't add it at this point
            }

            // Continue down the chain
            prop = propRecord.getNextProp();
        }

        // At this point we haven't added the property block, although we may have found room for it
        // along the way. If we didn't then just create a new record, it's fine
        PropertyRecord freeHost;
        if ( freeHostProxy == null )
        {
            // We couldn't find free space along the way, so create a new host record
            freeHost = propertyRecords.create( propertyRecordIdGenerator.nextId( cursorTracer ), primitive, cursorTracer ).forChangingData();
            freeHost.setInUse( true );
            if ( primitive.getNextProp() != Record.NO_NEXT_PROPERTY.intValue() )
            {
                // This isn't the first property record for the entity, re-shuffle the first one so that
                // the new one becomes the first
                PropertyRecord prevProp = propertyRecords.getOrLoad( primitive.getNextProp(), primitive, cursorTracer ).forChangingLinkage();
                assert prevProp.getPrevProp() == Record.NO_PREVIOUS_PROPERTY.intValue();
                prevProp.setPrevProp( freeHost.getId() );
                freeHost.setNextProp( prevProp.getId() );
                prevProp.setChanged( primitive );
            }

            // By the way, this is the only condition where the primitive record also needs to change
            primitiveRecordChange.forChangingLinkage().setNextProp( freeHost.getId() );
        }
        else
        {
            freeHost = freeHostProxy.forChangingData();
        }

        // At this point we know that we have a host record with sufficient space in it for the block
        // to add, so simply add it
        freeHost.addPropertyBlock( block );
        assert traverser.assertPropertyChain( primitive, propertyRecords );
    }

    private void removeProperty( PrimitiveRecord primitive, PropertyRecord host, PropertyBlock block )
    {
        host.removePropertyBlock( block.getKeyIndexId() );
        host.setChanged( primitive );
        for ( DynamicRecord record : block.getValueRecords() )
        {
            assert record.inUse();
            record.setInUse( false, block.getType().intValue() );
            host.addDeletedRecord( record );
        }
    }

    public PropertyBlock encodePropertyValue( int propertyKey, Value value )
    {
        return encodeValue( new PropertyBlock(), propertyKey, value );
    }

    public PropertyBlock encodeValue( PropertyBlock block, int propertyKey, Value value )
    {
        PropertyStore.encodeValue( block, propertyKey, value, stringRecordAllocator, arrayRecordAllocator, allowStorePointsAndTemporal, cursorTracer,
                memoryTracker );
        return block;
    }

    public long createPropertyChain( PrimitiveRecord owner, Iterator<PropertyBlock> properties,
            RecordAccess<PropertyRecord, PrimitiveRecord> propertyRecords )
    {
        return createPropertyChain( owner, properties, propertyRecords, p -> {} );
    }

    private long createPropertyChain( PrimitiveRecord owner, Iterator<PropertyBlock> properties, RecordAccess<PropertyRecord,PrimitiveRecord> propertyRecords,
            Consumer<PropertyRecord> createdPropertyRecords )
    {
        if ( properties == null || !properties.hasNext() )
        {
            return Record.NO_NEXT_PROPERTY.intValue();
        }
        PropertyRecord currentRecord = propertyRecords.create( propertyRecordIdGenerator.nextId( cursorTracer ), owner, cursorTracer ).forChangingData();
        createdPropertyRecords.accept( currentRecord );
        currentRecord.setInUse( true );
        currentRecord.setCreated();
        PropertyRecord firstRecord = currentRecord;
        while ( properties.hasNext() )
        {
            PropertyBlock block = properties.next();
            if ( currentRecord.size() + block.getSize() > PropertyType.getPayloadSize() )
            {
                // Here it means the current block is done for
                PropertyRecord prevRecord = currentRecord;
                // Create new record
                long propertyId = propertyRecordIdGenerator.nextId( cursorTracer );
                currentRecord = propertyRecords.create( propertyId, owner, cursorTracer ).forChangingData();
                createdPropertyRecords.accept( currentRecord );
                currentRecord.setInUse( true );
                currentRecord.setCreated();
                // Set up links
                prevRecord.setNextProp( propertyId );
                currentRecord.setPrevProp( prevRecord.getId() );
                // Now current is ready to start picking up blocks
            }
            currentRecord.addPropertyBlock( block );
        }
        return firstRecord.getId();
    }
}
