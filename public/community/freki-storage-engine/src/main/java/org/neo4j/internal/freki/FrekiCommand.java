/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.internal.freki;

import org.eclipse.collections.api.map.primitive.IntObjectMap;
import org.eclipse.collections.api.map.primitive.MutableIntObjectMap;
import org.eclipse.collections.api.set.primitive.IntSet;
import org.eclipse.collections.api.set.primitive.MutableIntSet;
import org.eclipse.collections.api.tuple.primitive.IntObjectPair;
import org.eclipse.collections.impl.factory.primitive.IntObjectMaps;
import org.eclipse.collections.impl.factory.primitive.IntSets;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

import org.neo4j.internal.kernel.api.exceptions.schema.MalformedSchemaRuleException;
import org.neo4j.internal.schema.SchemaRule;
import org.neo4j.internal.schema.SchemaRuleMapifier;
import org.neo4j.io.fs.ReadableChannel;
import org.neo4j.io.fs.WritableChannel;
import org.neo4j.storageengine.api.StorageCommand;
import org.neo4j.string.UTF8;
import org.neo4j.token.api.NamedToken;
import org.neo4j.values.storable.BooleanValue;
import org.neo4j.values.storable.DoubleArray;
import org.neo4j.values.storable.IntArray;
import org.neo4j.values.storable.IntValue;
import org.neo4j.values.storable.LongValue;
import org.neo4j.values.storable.TextValue;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.ValueGroup;
import org.neo4j.values.storable.Values;

abstract class FrekiCommand implements StorageCommand
{
    private final byte recordType;

    FrekiCommand( byte recordType )
    {
        this.recordType = recordType;
    }

    @Override
    public void serialize( WritableChannel channel ) throws IOException
    {
        channel.put( recordType );
    }

    abstract boolean accept( FrekiTransactionApplier applier ) throws IOException;

    static class SparseNode extends FrekiCommand
    {
        static final byte TYPE = 1;

        private final Record before;
        private final Record after;

        SparseNode( Record before, Record after )
        {
            super( TYPE );
            this.before = before;
            this.after = after;
        }

        Record before()
        {
            return before;
        }

        Record after()
        {
            return after;
        }

        @Override
        boolean accept( FrekiTransactionApplier applier ) throws IOException
        {
            applier.handle( this );
            return false;
        }

        @Override
        public void serialize( WritableChannel channel ) throws IOException
        {
            super.serialize( channel );
            channel.putLong( after.id );
            before.serialize( channel );
            after.serialize( channel );
        }

        static SparseNode deserialize( ReadableChannel channel ) throws IOException
        {
            long id = channel.getLong();
            Record before = Record.deserialize( channel, id );
            Record after = Record.deserialize( channel, id );
            return new SparseNode( before, after );
        }
    }

    static class DenseNode extends FrekiCommand
    {
        static final byte TYPE = 2;

        final long nodeId;
        final boolean inUse;
        final IntObjectMap<ByteBuffer> addedProperties;
        final IntSet removedProperties;
        final IntObjectMap<DenseRelationships> createdRelationships;
        final IntObjectMap<DenseRelationships> deletedRelationships;

        DenseNode( long nodeId, boolean inUse, IntObjectMap<ByteBuffer> addedProperties, IntSet removedProperties,
                IntObjectMap<DenseRelationships> createdRelationships, IntObjectMap<DenseRelationships> deletedRelationships )
        {
            super( TYPE );
            this.nodeId = nodeId;
            this.inUse = inUse;
            this.addedProperties = addedProperties;
            this.removedProperties = removedProperties;
            this.createdRelationships = createdRelationships;
            this.deletedRelationships = deletedRelationships;
        }

        @Override
        boolean accept( FrekiTransactionApplier applier ) throws IOException
        {
            applier.handle( this );
            return false;
        }

        @Override
        public void serialize( WritableChannel channel ) throws IOException
        {
            super.serialize( channel );
            channel.putLong( nodeId );
            channel.put( (byte) (inUse ? 1 : 0) );

            // added/changed node properties
            writeProperties( channel, addedProperties );

            // removed node properties
            channel.putInt( removedProperties.size() );
            for ( int key : removedProperties.toArray() )
            {
                channel.putInt( key );
            }

            // created relationships
            writeRelationships( channel, createdRelationships, true );

            // deleted relationships
            writeRelationships( channel, deletedRelationships, false );
        }

        private void writeProperties( WritableChannel channel, IntObjectMap<ByteBuffer> properties ) throws IOException
        {
            channel.putInt( properties.size() );
            for ( IntObjectPair<ByteBuffer> property : properties.keyValuesView() )
            {
                writeProperty( channel, property.getOne(), property.getTwo() );
            }
        }

        private void writeProperty( WritableChannel channel, int key, ByteBuffer value ) throws IOException
        {
            channel.putInt( key );
            channel.putInt( value.limit() );
            channel.put( value.array(), value.limit() );
        }

        private static IntObjectMap<ByteBuffer> readProperties( ReadableChannel channel ) throws IOException
        {
            MutableIntObjectMap<ByteBuffer> properties = IntObjectMaps.mutable.empty();
            int numProperties = channel.getInt();
            for ( int i = 0; i < numProperties; i++ )
            {
                int key = channel.getInt();
                int length = channel.getInt();
                byte[] data = new byte[length];
                channel.get( data, length );
                properties.put( key, ByteBuffer.wrap( data ) );
            }
            return properties;
        }

        private static IntSet readRemovedProperties( ReadableChannel channel ) throws IOException
        {
            MutableIntSet removedProperties = IntSets.mutable.empty();
            int numProperties = channel.getInt();
            for ( int i = 0; i < numProperties; i++ )
            {
                removedProperties.add( channel.getInt() );
            }
            return null;
        }

        private void writeRelationships( WritableChannel channel, IntObjectMap<DenseRelationships> relationshipTypeMap, boolean includeProperties )
                throws IOException
        {
            channel.putInt( relationshipTypeMap.size() );
            for ( IntObjectPair<DenseRelationships> relationships : relationshipTypeMap.keyValuesView() )
            {
                channel.putInt( relationships.getOne() ); // type
                channel.putInt( relationships.getTwo().relationships.size() ); // number of relationships of this type
                for ( DenseRelationships.DenseRelationship relationship : relationships.getTwo() )
                {
                    writeRelationshipMainData( channel, relationship );
                    if ( includeProperties )
                    {
                        writeProperties( channel, relationship.properties );
                    }
                }
            }
        }

        private void writeRelationshipMainData( WritableChannel channel, DenseRelationships.DenseRelationship relationship ) throws IOException
        {
            channel.putLong( relationship.internalId );
            channel.putLong( relationship.sourceNodeId );
            channel.putLong( relationship.otherNodeId );
            channel.put( (byte) (relationship.outgoing ? 1 : 0) );
        }

        private static IntObjectMap<DenseRelationships> readRelationships( ReadableChannel channel, boolean includeProperties ) throws IOException
        {
            MutableIntObjectMap<DenseRelationships> relationships = IntObjectMaps.mutable.empty();
            int numRelationshipTypes = channel.getInt();
            for ( int i = 0; i < numRelationshipTypes; i++ )
            {
                int type = channel.getInt();
                int numRelationships = channel.getInt();
                DenseRelationships denseRelationships = new DenseRelationships( type );
                for ( int j = 0; j < numRelationships; j++ )
                {
                    long internalId = channel.getLong();
                    long sourceNodeId = channel.getLong();
                    long otherNodeId = channel.getLong();
                    boolean outgoing = channel.get() != 0;
                    IntObjectMap<ByteBuffer> properties = includeProperties ? readProperties( channel ) : IntObjectMaps.immutable.empty();
                    denseRelationships.add( internalId, sourceNodeId, otherNodeId, outgoing, properties );
                }
            }
            return relationships;
        }

        static DenseNode deserialize( ReadableChannel channel ) throws IOException
        {
            long nodeId = channel.getLong();
            boolean inUse = channel.get() != 0;

            IntObjectMap<ByteBuffer> addedProperties = readProperties( channel );
            IntSet removedProperties = readRemovedProperties( channel );
            IntObjectMap<DenseRelationships> createdRelationships = readRelationships( channel, true );
            IntObjectMap<DenseRelationships> deletedRelationships = readRelationships( channel, false );

            return new DenseNode( nodeId, inUse, addedProperties, removedProperties,createdRelationships, deletedRelationships );
        }
    }

    static class BigPropertyValue extends FrekiCommand
    {
        static final byte TYPE = 3;

        final long pointer;
        final byte[] bytes;

        BigPropertyValue( long pointer, byte[] bytes )
        {
            super( TYPE );
            this.pointer = pointer;
            this.bytes = bytes;
        }

        @Override
        boolean accept( FrekiTransactionApplier applier ) throws IOException
        {
            applier.handle( this );
            return false;
        }

        @Override
        public void serialize( WritableChannel channel ) throws IOException
        {
            super.serialize( channel );
            channel.putLong( pointer );
            channel.putInt( bytes.length );
            channel.put( bytes, bytes.length );
        }

        static BigPropertyValue deserialize( ReadableChannel channel ) throws IOException
        {
            long pointer = channel.getLong();
            byte[] data = new byte[channel.getInt()];
            channel.get( data, data.length );
            return new BigPropertyValue( pointer, data );
        }
    }

    abstract static class Token extends FrekiCommand implements StorageCommand.TokenCommand
    {

        final NamedToken token;

        Token( byte recordType, NamedToken token )
        {
            super( recordType );
            this.token = token;
        }

        @Override
        public int tokenId()
        {
            return token.id();
        }

        @Override
        public boolean isInternal()
        {
            return token.isInternal();
        }

        @Override
        public void serialize( WritableChannel channel ) throws IOException
        {
            super.serialize( channel );
            channel.putInt( token.id() );
            FrekiCommand.writeString( channel, token.name() );
        }

        static NamedToken deserializeNamedToken( ReadableChannel channel ) throws IOException
        {
            int id = channel.getInt();
            return new NamedToken( FrekiCommand.readString( channel ), id );
        }
    }

    static class LabelToken extends Token
    {
        static final byte TYPE = 10;

        LabelToken( NamedToken token )
        {
            super( TYPE, token );
        }

        @Override
        boolean accept( FrekiTransactionApplier applier ) throws IOException
        {
            applier.handle( this );
            return false;
        }

        static LabelToken deserialize( ReadableChannel channel ) throws IOException
        {
            return new LabelToken( deserializeNamedToken( channel ) );
        }
    }

    static class RelationshipTypeToken extends Token
    {
        static final byte TYPE = 11;

        RelationshipTypeToken( NamedToken token )
        {
            super( TYPE, token );
        }

        @Override
        boolean accept( FrekiTransactionApplier applier ) throws IOException
        {
            applier.handle( this );
            return false;
        }

        static RelationshipTypeToken deserialize( ReadableChannel channel ) throws IOException
        {
            return new RelationshipTypeToken( deserializeNamedToken( channel ) );
        }
    }

    static class PropertyKeyToken extends Token
    {
        static final byte TYPE = 12;

        PropertyKeyToken( NamedToken token )
        {
            super( TYPE, token );
        }

        @Override
        boolean accept( FrekiTransactionApplier applier ) throws IOException
        {
            applier.handle( this );
            return false;
        }

        static PropertyKeyToken deserialize( ReadableChannel channel ) throws IOException
        {
            return new PropertyKeyToken( deserializeNamedToken( channel ) );
        }
    }

    enum Mode //Dont change order, will break format
    {
        CREATE,
        UPDATE,
        DELETE;
    }

    static class Schema extends FrekiCommand
    {
        static final byte TYPE = 13;
        private static final byte VALUE_TYPE_STRING = 1;
        private static final byte VALUE_TYPE_BOOLEAN = 2;
        private static final byte VALUE_TYPE_INT = 3;
        private static final byte VALUE_TYPE_LONG = 4;
        private static final byte VALUE_TYPE_INT_ARRAY = 5;
        private static final byte VALUE_TYPE_DOUBLE_ARRAY = 6;

        final SchemaRule descriptor;
        final Mode mode;

        Schema( SchemaRule descriptor, Mode mode )
        {
            super( TYPE );
            this.descriptor = descriptor;
            this.mode = mode;
        }

        @Override
        boolean accept( FrekiTransactionApplier applier ) throws IOException
        {
            applier.handle( this );
            return false;
        }

        @Override
        public void serialize( WritableChannel channel ) throws IOException
        {
            super.serialize( channel );
            channel.putLong( descriptor.getId() );
            channel.put( (byte) mode.ordinal() );
            Map<String,Value> properties = SchemaRuleMapifier.mapifySchemaRule( descriptor );
            channel.putInt( properties.size() );
            for ( Map.Entry<String,Value> property : properties.entrySet() )
            {
                FrekiCommand.writeString( channel, property.getKey() );
                writeValue( channel, property.getValue() );
            }
        }

        void writeValue( WritableChannel channel, Value value ) throws IOException
        {
            // TODO copied from GBPTreeSchemaStore, this could be unified to one thing later on
            if ( value.valueGroup() == ValueGroup.TEXT )
            {
                byte[] bytes = UTF8.encode( ((TextValue) value).stringValue() );
                channel.put( VALUE_TYPE_STRING );
                channel.putInt( bytes.length );
                channel.put( bytes, bytes.length );
            }
            else if ( value.valueGroup() == ValueGroup.BOOLEAN )
            {
                channel.put( VALUE_TYPE_BOOLEAN );
                channel.put( (byte) (((BooleanValue) value).booleanValue() ? 1 : 0) );
            }
            else if ( value instanceof IntValue )
            {
                channel.put( VALUE_TYPE_INT );
                channel.putInt( ((IntValue) value).value() );
            }
            else if ( value instanceof LongValue )
            {
                channel.put( VALUE_TYPE_LONG );
                channel.putLong( ((LongValue) value).value() );
            }
            else if ( value instanceof IntArray )
            {
                channel.put( VALUE_TYPE_INT_ARRAY );
                int[] array = ((IntArray) value).asObjectCopy();
                channel.putInt( array.length );
                for ( int item : array )
                {
                    channel.putInt( item );
                }
            }
            else if ( value instanceof DoubleArray )
            {
                channel.put( VALUE_TYPE_DOUBLE_ARRAY );
                double[] array = ((DoubleArray) value).asObjectCopy();
                channel.putInt( array.length );
                for ( double item : array )
                {
                    channel.putLong( Double.doubleToLongBits( item ) );
                }
            }
            else
            {
                throw new UnsupportedOperationException( "Unsupported value type " + value + " " + value.getClass() );
            }
        }

        private static Value readValue( ReadableChannel channel ) throws IOException
        {
            Value value;
            byte type = channel.get();
            switch ( type )
            {
            case VALUE_TYPE_STRING:
                int stringLength = channel.getInt();
                byte[] stringBytes = new byte[stringLength];
                channel.get( stringBytes, stringLength );
                value = Values.stringValue( UTF8.decode( stringBytes ) );
                break;
            case VALUE_TYPE_BOOLEAN:
                byte booleanValue = channel.get();
                value = Values.booleanValue( booleanValue != 0 );
                break;
            case VALUE_TYPE_INT:
                value = Values.intValue( channel.getInt() );
                break;
            case VALUE_TYPE_LONG:
                value = Values.longValue( channel.getLong() );
                break;
            case VALUE_TYPE_INT_ARRAY:
                int intArrayLength = channel.getInt();
                int[] intArray = new int[intArrayLength];
                for ( int i = 0; i < intArrayLength; i++ )
                {
                    intArray[i] = channel.getInt();
                }
                value = Values.intArray( intArray );
                break;
            case VALUE_TYPE_DOUBLE_ARRAY:
                int doubleArrayLength = channel.getInt();
                double[] doubleArray = new double[doubleArrayLength];
                for ( int i = 0; i < doubleArrayLength; i++ )
                {
                    doubleArray[i] = Double.longBitsToDouble( channel.getLong() );
                }
                value = Values.doubleArray( doubleArray );
                break;
            default:
                throw new UnsupportedOperationException( "Unknown type " + type );
            }
            return value;
        }

        static Schema deserialize( ReadableChannel channel ) throws IOException
        {
            long descriptorId = channel.getLong();
            Mode mode = Mode.values()[channel.get()];

            int numProperties = channel.getInt();
            Map<String,Value> properties = new HashMap<>();
            for ( int i = 0; i < numProperties; i++ )
            {
                String key = FrekiCommand.readString( channel );
                Value value = readValue( channel );
                properties.put( key, value );
            }

            try
            {
                SchemaRule schemaRule = SchemaRuleMapifier.unmapifySchemaRule( descriptorId, properties );
                return new Schema( schemaRule, mode );
            }
            catch ( MalformedSchemaRuleException e )
            {
                throw new IOException( "Could not read schema", e );
            }
        }
    }

    private static void writeString( WritableChannel channel, String string ) throws IOException
    {
        byte[] data = UTF8.encode( string );
        channel.putInt( data.length );
        channel.put( data, data.length );
    }

    private static String readString( ReadableChannel channel ) throws IOException
    {
        byte[] nameBytes = new byte[channel.getInt()];
        channel.get( nameBytes, nameBytes.length );
        return UTF8.decode( nameBytes );
    }

    interface Dispatcher
    {
        void handle( SparseNode node ) throws IOException;

        void handle( DenseNode node ) throws IOException;

        void handle( BigPropertyValue value ) throws IOException;

        void handle( LabelToken token ) throws IOException;

        void handle( RelationshipTypeToken token ) throws IOException;

        void handle( PropertyKeyToken token ) throws IOException;

        void handle( Schema schema ) throws IOException;

        class Adapter implements Dispatcher
        {
            @Override
            public void handle( SparseNode node ) throws IOException
            {
            }

            @Override
            public void handle( DenseNode node ) throws IOException
            {
            }

            @Override
            public void handle( BigPropertyValue value ) throws IOException
            {
            }

            @Override
            public void handle( LabelToken token ) throws IOException
            {
            }

            @Override
            public void handle( RelationshipTypeToken token ) throws IOException
            {
            }

            @Override
            public void handle( PropertyKeyToken token ) throws IOException
            {
            }

            @Override
            public void handle( Schema schema ) throws IOException
            {
            }
        }
    }
}
