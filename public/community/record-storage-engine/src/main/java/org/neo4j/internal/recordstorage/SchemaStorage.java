/*
 * Copyright (c) 2002-2019 "Neo4j,"
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

import org.eclipse.collections.api.block.procedure.primitive.IntObjectProcedure;
import org.eclipse.collections.api.map.primitive.IntObjectMap;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.function.Predicate;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import org.neo4j.internal.kernel.api.exceptions.schema.MalformedSchemaRuleException;
import org.neo4j.kernel.api.exceptions.schema.DuplicateSchemaRuleException;
import org.neo4j.kernel.api.exceptions.schema.SchemaRuleNotFoundException;
import org.neo4j.kernel.impl.index.schema.StoreIndexDescriptor;
import org.neo4j.kernel.impl.store.PropertyStore;
import org.neo4j.kernel.impl.store.SchemaStore;
import org.neo4j.kernel.impl.store.record.AbstractBaseRecord;
import org.neo4j.kernel.impl.store.record.ConstraintRule;
import org.neo4j.kernel.impl.store.record.PropertyBlock;
import org.neo4j.kernel.impl.store.record.PropertyRecord;
import org.neo4j.kernel.impl.store.record.Record;
import org.neo4j.kernel.impl.store.record.RecordLoad;
import org.neo4j.kernel.impl.store.record.SchemaRecord;
import org.neo4j.storageengine.api.SchemaRule;
import org.neo4j.storageengine.api.schema.ConstraintDescriptor;
import org.neo4j.storageengine.api.schema.IndexDescriptor;
import org.neo4j.storageengine.api.schema.SchemaDescriptor;
import org.neo4j.storageengine.api.schema.SchemaDescriptorSupplier;
import org.neo4j.token.api.TokenHolder;
import org.neo4j.util.VisibleForTesting;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;

public class SchemaStorage implements SchemaRuleAccess
{
    private final SchemaStore schemaStore;
    private final TokenHolder propertyKeyTokenHolder;

    public SchemaStorage( SchemaStore schemaStore, TokenHolder propertyKeyTokenHolder )
    {
        this.schemaStore = schemaStore;
        this.propertyKeyTokenHolder = propertyKeyTokenHolder;
    }

    @Override
    public long newRuleId()
    {
        return schemaStore.nextId();
    }

    @Override
    public Iterable<SchemaRule> getAll()
    {
        return streamAllSchemaRules( false )::iterator;
    }

    @Override
    public SchemaRule loadSingleSchemaRule( long ruleId ) throws MalformedSchemaRuleException
    {
        SchemaRecord record = schemaStore.newRecord();
        schemaStore.getRecord( ruleId, record, RecordLoad.NORMAL );
        return readSchemaRule( record );
    }

    @Override
    public Iterator<StoreIndexDescriptor> indexesGetAll()
    {
        return indexRules( streamAllSchemaRules( false ) ).iterator();
    }

    @Override
    public StoreIndexDescriptor[] indexGetForSchema( SchemaDescriptorSupplier supplier )
    {
        SchemaDescriptor schema = supplier.schema();
        return indexRules( streamAllSchemaRules( false ) )
                .filter( rule -> rule.schema().equals( schema ) )
                .toArray( StoreIndexDescriptor[]::new );
    }

    @Override
    public StoreIndexDescriptor[] indexGetForSchema( IndexDescriptor descriptor, boolean filterOnType )
    {
        SchemaDescriptor schema = descriptor.schema();
        return indexRules( streamAllSchemaRules( false ) )
                .filter( filterOnType ? descriptor::equals : index -> index.schema().equals( schema ) )
                .toArray( StoreIndexDescriptor[]::new );
    }

    @Override
    public StoreIndexDescriptor indexGetForName( String indexName )
    {
        return indexRules( streamAllSchemaRules( false ) )
                .filter( idx -> idx.hasUserSuppliedName() && idx.name().equals( indexName ) )
                .findAny().orElse( null );
    }

    @Override
    public ConstraintRule constraintsGetSingle( ConstraintDescriptor descriptor ) throws SchemaRuleNotFoundException, DuplicateSchemaRuleException
    {
        ConstraintRule[] rules = constraintRules( streamAllSchemaRules( false ) )
                .filter( descriptor::isSame )
                .toArray( ConstraintRule[]::new );
        if ( rules.length == 0 )
        {
            throw new SchemaRuleNotFoundException( SchemaRule.Kind.map( descriptor ), descriptor.schema() );
        }
        if ( rules.length > 1 )
        {
            throw new DuplicateSchemaRuleException( SchemaRule.Kind.map( descriptor ), descriptor.schema() );
        }
        return rules[0];
    }

    @Override
    public Iterator<ConstraintRule> constraintsGetAllIgnoreMalformed()
    {
        return constraintRules( streamAllSchemaRules( true ) ).iterator();
    }

    @Override
    public SchemaRecordChangeTranslator getSchemaRecordChangeTranslator()
    {
        return new PropertyBasedSchemaRecordChangeTranslator()
        {
            @Override
            protected IntObjectMap<Value> asMap( SchemaRule rule )
            {
                return SchemaStore.convertSchemaRuleToMap( rule, propertyKeyTokenHolder );
            }

            @Override
            protected void setConstraintIndexOwnerProperty( long constraintId, IntObjectProcedure<Value> proc )
            {
                int propertyId = SchemaStore.getOwningConstraintPropertyKeyId( propertyKeyTokenHolder );
                proc.value( propertyId, Values.longValue( constraintId ) );
            }
        };
    }

    @Override
    public void writeSchemaRule( SchemaRule rule )
    {
        IntObjectMap<Value> protoProperties = SchemaStore.convertSchemaRuleToMap( rule, propertyKeyTokenHolder );
        PropertyStore propertyStore = schemaStore.propertyStore();
        Collection<PropertyBlock> blocks = new ArrayList<>();
        protoProperties.forEachKeyValue( ( keyId, value ) ->
        {
            PropertyBlock block = new PropertyBlock();
            propertyStore.encodeValue( block, keyId, value );
            blocks.add( block );
        } );

        assert !blocks.isEmpty() : "Property blocks should have been produced for schema rule: " + rule;

        long nextPropId = Record.NO_NEXT_PROPERTY.longValue();
        PropertyRecord currRecord = newInitialisedPropertyRecord( propertyStore, rule );

        for ( PropertyBlock block : blocks )
        {
            if ( !currRecord.hasSpaceFor( block ) )
            {
                PropertyRecord nextRecord = newInitialisedPropertyRecord( propertyStore, rule );
                linkAndWritePropertyRecord( propertyStore, currRecord, nextRecord.getId(), nextPropId );
                nextPropId = currRecord.getId();
                currRecord = nextRecord;
            }
            currRecord.addPropertyBlock( block );
        }

        linkAndWritePropertyRecord( propertyStore, currRecord, Record.NO_PREVIOUS_PROPERTY.longValue(), nextPropId );
        nextPropId = currRecord.getId();

        SchemaRecord schemaRecord = schemaStore.newRecord();
        schemaRecord.initialize( true, nextPropId );
        schemaRecord.setId( rule.getId() );
        schemaStore.updateRecord( schemaRecord );
        schemaStore.setHighestPossibleIdInUse( rule.getId() );
    }

    private PropertyRecord newInitialisedPropertyRecord( PropertyStore propertyStore, SchemaRule rule )
    {
        PropertyRecord record = propertyStore.newRecord();
        record.setId( propertyStore.nextId() );
        record.setSchemaRuleId( rule.getId() );
        return record;
    }

    private void linkAndWritePropertyRecord( PropertyStore propertyStore, PropertyRecord record, long prevPropId, long nextProp )
    {
        record.setInUse( true );
        record.setPrevProp( prevPropId );
        record.setNextProp( nextProp );
        propertyStore.updateRecord( record );
        propertyStore.setHighestPossibleIdInUse( record.getId() );
    }

    @Override
    public void deleteSchemaRule( SchemaRule rule )
    {
        SchemaRecord record = schemaStore.newRecord();
        schemaStore.getRecord( rule.getId(), record, RecordLoad.FORCE );
        if ( record.inUse() )
        {
            long nextProp = record.getNextProp();
            record.setInUse( false );
            schemaStore.updateRecord( record );
            PropertyStore propertyStore = schemaStore.propertyStore();
            PropertyRecord props = propertyStore.newRecord();
            while ( nextProp != Record.NO_NEXT_PROPERTY.longValue() && propertyStore.getRecord( nextProp, props, RecordLoad.NORMAL ).inUse() )
            {
                nextProp = props.getNextProp();
                props.setInUse( false );
                propertyStore.updateRecord( props );
            }
        }
    }

    @VisibleForTesting
    Stream<SchemaRule> streamAllSchemaRules( boolean ignoreMalformed )
    {
        long startId = schemaStore.getNumberOfReservedLowIds();
        long endId = schemaStore.getHighId();
        return LongStream.range( startId, endId )
                .mapToObj( id -> schemaStore.getRecord( id, schemaStore.newRecord(), RecordLoad.FORCE ) )
                .filter( AbstractBaseRecord::inUse )
                .flatMap( record -> readSchemaRuleThrowingRuntimeException( record, ignoreMalformed ) );
    }

    private Stream<StoreIndexDescriptor> indexRules( Stream<SchemaRule> stream )
    {
        return stream
                .filter( rule -> rule instanceof StoreIndexDescriptor )
                .map( rule -> (StoreIndexDescriptor) rule );
    }

    private Stream<ConstraintRule> constraintRules( Stream<SchemaRule> stream )
    {
        return stream
                .filter( rule -> rule instanceof ConstraintRule )
                .map( rule -> (ConstraintRule) rule );
    }

    private Stream<SchemaRule> readSchemaRuleThrowingRuntimeException( SchemaRecord record, boolean ignoreMalformed )
    {
        try
        {
            return Stream.of( readSchemaRule( record ) );
        }
        catch ( MalformedSchemaRuleException e )
        {
            // In case we've raced with a record deletion, ignore malformed records that no longer appear to be in use.
            if ( !ignoreMalformed && schemaStore.isInUse( record.getId() ) )
            {
                throw new RuntimeException( e );
            }
        }
        return Stream.empty();
    }

    private SchemaRule readSchemaRule( SchemaRecord record ) throws MalformedSchemaRuleException
    {
        return SchemaStore.readSchemaRule( record, schemaStore.propertyStore(), propertyKeyTokenHolder );
    }
}
