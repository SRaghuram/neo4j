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
import org.eclipse.collections.api.tuple.primitive.IntObjectPair;
import org.eclipse.collections.impl.factory.primitive.IntObjectMaps;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;

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

import static java.lang.String.format;
import static org.neo4j.internal.freki.Record.recordXFactor;
import static org.neo4j.internal.helpers.Numbers.safeCastIntToShort;

abstract class FrekiCommand implements StorageCommand
{
    private final byte commandType;

    FrekiCommand( byte commandType )
    {
        this.commandType = commandType;
    }

    @Override
    public void serialize( WritableChannel channel ) throws IOException
    {
        channel.put( commandType );
    }

    abstract boolean accept( Dispatcher applier ) throws IOException;

    static class SparseNode extends FrekiCommand
    {
        static final byte TYPE = 1;

        final long nodeId;
        final Record before;
        final Record after;

        SparseNode( long nodeId, Record before, Record after )
        {
            super( TYPE );
            this.nodeId = nodeId;
            this.before = before;
            this.after = after;
        }

        @Override
        boolean accept( Dispatcher applier ) throws IOException
        {
            applier.handle( this );
            return false;
        }

        @Override
        public void serialize( WritableChannel channel ) throws IOException
        {
            super.serialize( channel );
            channel.putLong( nodeId );
            channel.putLong( after.id );
            before.serialize( channel );
            after.serialize( channel );
        }

        @Override
        public String toString()
        {
            return format( "SparseNode[%d,x%d%n  -%s%n  +%s]", nodeId, recordXFactor( after.sizeExp() ), before, after );
        }

        static SparseNode deserialize( ReadableChannel channel ) throws IOException
        {
            long nodeId = channel.getLong();
            long id = channel.getLong();
            Record before = Record.deserialize( channel, id );
            Record after = Record.deserialize( channel, id );
            return new SparseNode( nodeId, before, after );
        }
    }

    /**
     * Note, even though this is, in essence, a logical command it still needs to be able to be reversed.
     * E.g. a changed property value must have its previous value, a removed property must have the value which was removed, a.s.o.
     * This to be able to do recovery properly.
     *
     * Property: added, changed, removed
     * Relationship: created(incl. added properties), changed properties, deleted(incl. removed properties)
     * Degrees: for every changed type/direction(out,in,loop): previous degree, increment(arbitrary +N or -N)
     *
     * There's no such thing as deleting a node, you delete all its entries explicitly and it's gone. This is a good control
     * that it's recoverable because we're not allowed to remove anything not covered by the command because otherwise we
     * cannot recreated that data when doing reverse recovery.
     */
    static class DenseNode extends FrekiCommand
    {
        static final byte TYPE = 2;

        final long nodeId;
        final IntObjectMap<DenseRelationships> relationshipUpdates;

        DenseNode( long nodeId, IntObjectMap<DenseRelationships> relationshipUpdates )
        {
            super( TYPE );
            this.nodeId = nodeId;
            this.relationshipUpdates = relationshipUpdates;
        }

        @Override
        boolean accept( Dispatcher applier ) throws IOException
        {
            applier.handle( this );
            return false;
        }

        @Override
        public void serialize( WritableChannel channel ) throws IOException
        {
            super.serialize( channel );
            channel.putLong( nodeId );

            channel.putInt( relationshipUpdates.size() );
            for ( IntObjectPair<DenseRelationships> relationships : relationshipUpdates.keyValuesView() )
            {
                // type and degrees
                DenseRelationships relationshipsOfType = relationships.getTwo();
                channel.putInt( relationships.getOne() );

                // Created
                channel.putInt( relationshipsOfType.created.size() ); // number of relationships of this type
                for ( DenseRelationships.DenseRelationship relationship : relationshipsOfType.created )
                {
                    writeRelationship( channel, relationship );
                }

                // Deleted
                channel.putInt( relationshipsOfType.deleted.size() ); // number of relationships of this type
                for ( DenseRelationships.DenseRelationship relationship : relationshipsOfType.deleted )
                {
                    writeRelationship( channel, relationship );
                }
            }
        }

        private void writeProperties( WritableChannel channel, IntObjectMap<PropertyUpdate> properties ) throws IOException
        {
            channel.putInt( properties.size() );
            for ( PropertyUpdate property : properties )
            {
                writeProperty( channel, property );
            }
        }

        private void writeProperty( WritableChannel channel, PropertyUpdate property ) throws IOException
        {
            int keyAndModeInt = property.propertyKeyId | (property.mode.ordinal() << 30);
            channel.putInt( keyAndModeInt );
            switch ( property.mode )
            {
            case UPDATE:
                writePropertyValue( channel, property.before );
                // fall-through
            case CREATE:
                writePropertyValue( channel, property.after );
                break;
            case DELETE:
                writePropertyValue( channel, property.before );
                break;
            default:
                throw new IllegalArgumentException( "Unrecognized mode " + property.mode );
            }
        }

        private void writePropertyValue( WritableChannel channel, ByteBuffer value ) throws IOException
        {
            // 2B length + data[length]
            channel.putShort( safeCastIntToShort( value.limit() ) );
            channel.put( value.array(), value.limit() );
        }

        private static IntObjectMap<PropertyUpdate> readProperties( ReadableChannel channel ) throws IOException
        {
            int numProperties = channel.getInt();
            if ( numProperties == 0 )
            {
                return IntObjectMaps.immutable.empty();
            }

            MutableIntObjectMap<PropertyUpdate> properties = IntObjectMaps.mutable.empty();
            for ( int i = 0; i < numProperties; i++ )
            {
                int keyAndMode = channel.getInt();
                int key = keyAndMode & 0x3FFFFFFF;
                PropertyUpdate update;
                switch ( FrekiCommand.MODES[keyAndMode >>> 30] )
                {
                case UPDATE:
                    update = PropertyUpdate.change( key, readProperty( channel ), readProperty( channel ) );
                    break;
                case CREATE:
                    update = PropertyUpdate.add( key, readProperty( channel ) );
                    break;
                case DELETE:
                    update = PropertyUpdate.remove( key, readProperty( channel ) );
                    break;
                default:
                    throw new IllegalArgumentException( "Unrecognized mode " + FrekiCommand.MODES[keyAndMode >>> 30] );
                }
                properties.put( key, update );
            }
            return properties;
        }

        private static ByteBuffer readProperty( ReadableChannel channel ) throws IOException
        {
            int length = channel.getShort();
            byte[] data = new byte[length];
            channel.get( data, length );
            return ByteBuffer.wrap( data );
        }

        private void writeRelationship( WritableChannel channel, DenseRelationships.DenseRelationship relationship ) throws IOException
        {
            channel.putLong( relationship.internalId );
            channel.putLong( relationship.otherNodeId );
            boolean hasProperties = !relationship.propertyUpdates.isEmpty();
            byte outgoingAndProperties = (byte) ((relationship.outgoing ? 1 : 0) | (hasProperties ? 2 : 0));
            channel.put( outgoingAndProperties );
            if ( hasProperties )
            {
                writeProperties( channel, relationship.propertyUpdates );
            }
        }

        private static void readRelationships( ReadableChannel channel, Consumer<DenseRelationships.DenseRelationship> target ) throws IOException
        {
            int createdRelationships = channel.getInt();
            for ( int j = 0; j < createdRelationships; j++ )
            {
                readRelationship( channel, target );
            }
        }

        private static void readRelationship( ReadableChannel channel, Consumer<DenseRelationships.DenseRelationship> target )
                throws IOException
        {
            long internalId = channel.getLong();
            long otherNodeId = channel.getLong();
            byte outgoingAndProperties = channel.get();
            boolean outgoing = (outgoingAndProperties & 0x1) != 0;
            boolean hasProperties = (outgoingAndProperties & 0x2) != 0;
            target.accept( new DenseRelationships.DenseRelationship( internalId, otherNodeId, outgoing,
                    hasProperties ? readProperties( channel ) : IntObjectMaps.immutable.empty() ) );
        }

        static DenseNode deserialize( ReadableChannel channel ) throws IOException
        {
            long nodeId = channel.getLong();

            MutableIntObjectMap<DenseRelationships> relationships = IntObjectMaps.mutable.empty();
            int numRelationshipTypes = channel.getInt();
            for ( int i = 0; i < numRelationshipTypes; i++ )
            {
                int type = channel.getInt();
                DenseRelationships relationshipsOfType = relationships.getIfAbsentPut( type, new DenseRelationships( nodeId, type ) );
                readRelationships( channel, relationshipsOfType::create );
                readRelationships( channel, relationshipsOfType::delete );
            }

            return new DenseNode( nodeId, relationships );
        }
    }

    static class BigPropertyValue extends FrekiCommand
    {
        static final byte TYPE = 3;

        final long pointer;
        final byte[] bytes;
        final int length;

        BigPropertyValue( long pointer, byte[] bytes )
        {
            this( pointer, bytes, bytes.length );
        }

        BigPropertyValue( long pointer, byte[] bytes, int length )
        {
            super( TYPE );
            this.pointer = pointer;
            this.bytes = bytes;
            this.length = length;
        }

        @Override
        boolean accept( Dispatcher applier ) throws IOException
        {
            applier.handle( this );
            return false;
        }

        @Override
        public void serialize( WritableChannel channel ) throws IOException
        {
            super.serialize( channel );
            channel.putLong( pointer );
            channel.putInt( length );
            channel.put( bytes, length );
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
        boolean accept( Dispatcher applier ) throws IOException
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
        boolean accept( Dispatcher applier ) throws IOException
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
        boolean accept( Dispatcher applier ) throws IOException
        {
            applier.handle( this );
            return false;
        }

        static PropertyKeyToken deserialize( ReadableChannel channel ) throws IOException
        {
            return new PropertyKeyToken( deserializeNamedToken( channel ) );
        }
    }

    enum Mode //Don't change order, will break format
    {
        CREATE,
        UPDATE,
        DELETE
    }

    static Mode[] MODES = Mode.values();

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
        boolean accept( Dispatcher applier ) throws IOException
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

    static class NodeCount extends FrekiCommand
    {
        static final byte TYPE = 14;

        final int labelId;
        final long count;

        NodeCount( int labelId, long count )
        {
            super( TYPE );
            this.labelId = labelId;
            this.count = count;
        }

        @Override
        boolean accept( Dispatcher applier ) throws IOException
        {
            applier.handle( this );
            return false;
        }

        @Override
        public void serialize( WritableChannel channel ) throws IOException
        {
            super.serialize( channel );
            channel.putInt( labelId ).putLong( count );
        }

        static NodeCount deserialize( ReadableChannel channel ) throws IOException
        {
            return new NodeCount( channel.getInt(), channel.getLong() );
        }
    }

    static class RelationshipCount extends FrekiCommand
    {
        static final byte TYPE = 15;

        final int startLabelId;
        final int typeId;
        final int endLabelId;
        final long count;

        RelationshipCount( int startLabelId, int typeId, int endLabelId, long count )
        {
            super( TYPE );
            this.startLabelId = startLabelId;
            this.typeId = typeId;
            this.endLabelId = endLabelId;
            this.count = count;
        }

        @Override
        boolean accept( Dispatcher applier ) throws IOException
        {
            applier.handle( this );
            return false;
        }

        @Override
        public void serialize( WritableChannel channel ) throws IOException
        {
            super.serialize( channel );
            channel.putInt( startLabelId ).putInt( typeId ).putInt( endLabelId ).putLong( count );
        }

        static RelationshipCount deserialize( ReadableChannel channel ) throws IOException
        {
            return new RelationshipCount( channel.getInt(), channel.getInt(), channel.getInt(), channel.getLong() );
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

        void handle( NodeCount count ) throws IOException;

        void handle( RelationshipCount count ) throws IOException;

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

            @Override
            public void handle( NodeCount count ) throws IOException
            {
            }

            @Override
            public void handle( RelationshipCount count ) throws IOException
            {
            }
        }
    }
}
