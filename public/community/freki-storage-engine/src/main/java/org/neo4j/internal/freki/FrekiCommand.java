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
import org.eclipse.collections.api.set.primitive.IntSet;
import org.eclipse.collections.api.tuple.primitive.IntObjectPair;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;

import org.neo4j.internal.schema.SchemaRule;
import org.neo4j.internal.schema.SchemaRuleMapifier;
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
            writeProperties( channel );

            // removed node properties
            channel.putInt( removedProperties.size() );
            for ( int key : removedProperties.toArray() )
            {
                channel.putInt( key );
            }

            // created relationships
            channel.putInt( createdRelationships.size() );
            for ( IntObjectPair<DenseRelationships> relationships : createdRelationships.keyValuesView() )
            {
                channel.putInt( relationships.getOne() ); // type
                channel.putInt( relationships.getTwo().relationships.size() ); // number of relationships of this type
                for ( DenseRelationships.DenseRelationship relationship : relationships.getTwo() )
                {
                    writeRelationshipMainData( channel, relationship );
                    writeProperties( channel );
                }
            }

            // deleted relationships
            channel.putInt( deletedRelationships.size() );
            for ( IntObjectPair<DenseRelationships> relationships : createdRelationships.keyValuesView() )
            {
                channel.putInt( relationships.getOne() ); // type
                channel.putInt( relationships.getTwo().relationships.size() ); // number of relationships of this type
                for ( DenseRelationships.DenseRelationship relationship : relationships.getTwo() )
                {
                    writeRelationshipMainData( channel, relationship );
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

        private void writeProperties( WritableChannel channel ) throws IOException
        {
            channel.putInt( addedProperties.size() );
            for ( IntObjectPair<ByteBuffer> property : addedProperties.keyValuesView() )
            {
                writeProperty( channel, property.getOne(), property.getTwo() );
            }
        }

        private void writeProperty( WritableChannel channel, int key, ByteBuffer value ) throws IOException
        {
            channel.putInt( key );
            channel.put( value.array(), value.limit() );
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
            byte[] name = UTF8.encode( token.name() );
            channel.putInt( name.length );
            channel.put( name, name.length );
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
    }

    enum Mode
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
            Map<String,Value> properties = SchemaRuleMapifier.mapifySchemaRule( descriptor );
            for ( Map.Entry<String,Value> property : properties.entrySet() )
            {
                writeValue( channel, property.getValue() );
            }
        }

        public void writeValue( WritableChannel channel, Value value ) throws IOException
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
