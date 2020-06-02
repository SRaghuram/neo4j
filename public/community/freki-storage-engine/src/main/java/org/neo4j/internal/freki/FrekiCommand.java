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
import org.eclipse.collections.impl.factory.primitive.IntObjectMaps;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.function.Function;
import javax.annotation.Nonnull;

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
import static org.neo4j.token.api.TokenIdPrettyPrinter.label;
import static org.neo4j.token.api.TokenIdPrettyPrinter.relationshipType;

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

    static class SparseNode extends FrekiCommand implements Iterable<RecordChange>
    {
        static final byte TYPE = 1;

        final long nodeId;
        private RecordChange lowestChange;

        SparseNode( long nodeId )
        {
            super( TYPE );
            this.nodeId = nodeId;
        }

        private SparseNode( long nodeId, RecordChange lowestChange )
        {
            this( nodeId );
            this.lowestChange = lowestChange;
        }

        RecordChange addChange( Record before, Record after )
        {
            assert before != null || after != null : "Both before and after records cannot be null";
            RecordChange change = new RecordChange( before, after );
            if ( lowestChange == null )
            {
                lowestChange = change;
            }
            else
            {
                int sizeExp = change.sizeExp();
                RecordChange prev = null;
                RecordChange candidate = change();
                while ( candidate != null && candidate.sizeExp() < sizeExp )
                {
                    prev = candidate;
                    candidate = candidate.next();
                }
                if ( prev == null ) // first
                {
                    change.next = lowestChange;
                    lowestChange = change;
                }
                else // not first: prev -> change -> candidate
                {
                    change.next = candidate;
                    prev.next = change;
                }
            }
            return change;
        }

        RecordChange change()
        {
            return lowestChange;
        }

        @Nonnull
        @Override
        public Iterator<RecordChange> iterator()
        {
            return new Iterator<>()
            {
                private RecordChange next = change();
                @Override
                public boolean hasNext()
                {
                    return next != null;
                }

                @Override
                public RecordChange next()
                {
                    RecordChange curr = next;
                    next = curr.next;
                    return curr;
                }
            };
        }

        RecordChange change( int sizeExp )
        {
            for ( RecordChange candidate : this )
            {
                if ( candidate.sizeExp() == sizeExp )
                {
                    return candidate;
                }
            }
            return null;
        }

        RecordChange change( int sizeExp, long id )
        {
            for ( RecordChange candidate : this )
            {
                if ( candidate.sizeExp() == sizeExp && candidate.recordId() == id )
                {
                    return candidate;
                }
            }
            return null;
        }

        @Override
        boolean accept( Dispatcher applier ) throws IOException
        {
            applier.handle( this );
            return false;
        }

        @Override
        public String toString()
        {
            StringBuilder toString = new StringBuilder( format( "SparseNode[%d,", nodeId ) );
            addRecordsToToString( toString, "-", change -> change.before );
            addRecordsToToString( toString, "+", change -> change.after );
            return toString.append( ']' ).toString();
        }

        private void addRecordsToToString( StringBuilder toString, String versionSign, Function<RecordChange,Record> version )
        {
            for ( RecordChange change : this )
            {
                Record record = version.apply( change );
                if ( record != null )
                {
                    toString.append( format( "%n  %s%s", versionSign, record ) );
                }
            }
        }

        @Override
        public void serialize( WritableChannel channel ) throws IOException
        {
            assert lowestChange != null : "Gotta have _something_ in a SparseNode command";
            super.serialize( channel );
            channel.put( (byte) longSizeCode( nodeId ) );
            putCompactLong( channel, nodeId );
            for ( RecordChange change : this )
            {
                change.serialize( channel );
            }
        }

        static SparseNode deserialize( ReadableChannel channel ) throws IOException
        {
            byte header = channel.get();
            long nodeId = getCompactLong( channel, header & 0x3 );
            RecordChange lowestChange = RecordChange.deserialize( channel, nodeId );
            RecordChange change = lowestChange;
            while ( change.next != null )
            {
                RecordChange nextChange = RecordChange.deserialize( channel, nodeId );
                change.next = nextChange;
                change = nextChange;
            }
            return new SparseNode( nodeId, lowestChange );
        }
    }

    static class RecordChange
    {
        private static final RecordChange DESERIALIZATION_HAS_NEXT_MARKER = new RecordChange( null, null );

        final Record before;
        final Record after;
        boolean onlyVersionChange;
        private RecordChange next;

        RecordChange( Record before, Record after )
        {
            this.before = before;
            this.after = after;
        }

        private Record anyUsed()
        {
            return after != null ? after : before;
        }

        Mode mode()
        {
            return after != null ? before == null ? Mode.CREATE : Mode.UPDATE : Mode.DELETE;
        }

        void updateVersion( byte version )
        {
            onlyVersionChange = true;
            after.setVersion( version );
        }

        byte sizeExp()
        {
            return anyUsed().sizeExp();
        }

        long recordId()
        {
            return anyUsed().id;
        }

        private void serialize( WritableChannel channel ) throws IOException
        {
            int sizeExp = sizeExp();
            long recordIdToWrite = sizeExp > 0 ? recordId() : 0;
            byte header = (byte) (longSizeCode( recordIdToWrite )
                    | (before != null ? 0x4 : 0)
                    | (after != null ? 0x8 : 0)
                    | (next != null ? 0x10 : 0)
                    | (onlyVersionChange ? 0x20 : 0));
            channel.put( header );
            putCompactLong( channel, recordIdToWrite );
            if ( onlyVersionChange )
            {
                channel.put( before.flags );
                channel.put( before.version );
                channel.put( after.flags );
                channel.put( after.version );
            }
            else
            {
                if ( before != null )
                {
                    before.serialize( channel );
                }
                if ( after != null )
                {
                    after.serialize( channel );
                }
            }
        }

        private static RecordChange deserialize( ReadableChannel channel, long nodeId ) throws IOException
        {
            byte header = channel.get();
            int idSizeCode = header & 0x3;
            long id = idSizeCode == 0 ? nodeId : getCompactLong( channel, idSizeCode );
            boolean onlyVersionChange = (header & 0x20) != 0;
            RecordChange change;
            if ( onlyVersionChange )
            {
                byte beforeFlags = channel.get();
                byte beforeVersion = channel.get();
                byte afterFlags = channel.get();
                byte afterVersion = channel.get();
                change = new RecordChange( new Record( beforeFlags, id, beforeVersion, null ), new Record( afterFlags, id, afterVersion, null ) );
            }
            else
            {
                Record before = (header & 0x4) != 0 ? Record.deserialize( channel, id ) : null;
                Record after = (header & 0x8) != 0 ? Record.deserialize( channel, id ) : null;
                change = new RecordChange( before, after );
            }
            change.onlyVersionChange = onlyVersionChange;
            if ( (header & 0x10) != 0 )
            {
                change.next = DESERIALIZATION_HAS_NEXT_MARKER;
            }
            return change;
        }

        RecordChange next()
        {
            return next;
        }
    }

    /**
     * Note, even though this is, in essence, a logical command it still needs to be able to be reversed.
     * E.g. a changed property value must have its previous value, a removed property must have the value which was removed, a.s.o.
     * This to be able to do recovery properly.
     *
     * Property: added, changed, removed
     * Relationship: created(incl. added properties), changed properties, deleted(incl. removed properties)
     *
     * There's no such thing as deleting a node, you delete all its entries explicitly and it's gone. This is a good control
     * that it's recoverable because we're not allowed to remove anything not covered by the command because otherwise we
     * cannot recreated that data when doing reverse recovery.
     */
    static class DenseNode extends FrekiCommand
    {
        static final byte TYPE = 2;

        final long nodeId;
        final TreeMap<Integer,DenseRelationships> relationshipUpdates;

        DenseNode( long nodeId, TreeMap<Integer,DenseRelationships> relationshipUpdates )
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
            byte header = (byte) (longSizeCode( nodeId ) | (intSizeCode( relationshipUpdates.size() ) << 2));
            channel.put( header );
            putCompactLong( channel, nodeId );
            putCompactInt( channel, relationshipUpdates.size() );
            for ( Map.Entry<Integer,DenseRelationships> entry : relationshipUpdates.entrySet() )
            {
                int type = entry.getKey();
                DenseRelationships relationshipsOfType = entry.getValue();
                int numRelationships = relationshipsOfType.relationships.size();
                byte typeHeader = (byte) (intSizeCode( type ) | (intSizeCode( numRelationships ) << 2));
                channel.put( typeHeader );
                putCompactInt( channel, type );
                putCompactInt( channel, numRelationships );

                for ( DenseRelationships.DenseRelationship relationship : relationshipsOfType.relationships )
                {
                    writeRelationship( channel, relationship );
                }
            }
        }

        private void writeProperty( WritableChannel channel, PropertyUpdate property ) throws IOException
        {
            int beforeSizeCode = property.mode == Mode.CREATE ? 0 : intSizeCode( property.before.limit() );
            int afterSizeCode = property.mode == Mode.DELETE ? 0 : intSizeCode( property.after.limit() );
            byte header = (byte) (intSizeCode( property.propertyKeyId ) | (property.mode.ordinal() << 2) | (beforeSizeCode << 4) | (afterSizeCode << 6));
            channel.put( header );
            putCompactInt( channel, property.propertyKeyId );
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
            putCompactInt( channel, value.limit() );
            channel.put( value.array(), value.limit() );
        }

        private static IntObjectMap<PropertyUpdate> readProperties( ReadableChannel channel, int numProperties ) throws IOException
        {
            if ( numProperties == 0 )
            {
                return IntObjectMaps.immutable.empty();
            }

            MutableIntObjectMap<PropertyUpdate> properties = IntObjectMaps.mutable.empty();
            for ( int i = 0; i < numProperties; i++ )
            {
                byte header = channel.get();
                int key = getCompactInt( channel, header & 0x3 );
                Mode mode = FrekiCommand.MODES[(header >> 2) & 0x3];
                int beforeSize = (header >> 4) & 0x3;
                int afterSize = (header >> 6) & 0x3;
                PropertyUpdate update;
                switch ( mode )
                {
                case UPDATE:
                    update = PropertyUpdate.change( key, readPropertyValue( channel, beforeSize ), readPropertyValue( channel, afterSize ) );
                    break;
                case CREATE:
                    update = PropertyUpdate.add( key, readPropertyValue( channel, afterSize ) );
                    break;
                case DELETE:
                    update = PropertyUpdate.remove( key, readPropertyValue( channel, beforeSize ) );
                    break;
                default:
                    throw new IllegalArgumentException( "Unrecognized mode " + mode );
                }
                properties.put( key, update );
            }
            return properties;
        }

        private static ByteBuffer readPropertyValue( ReadableChannel channel, int sizeCode ) throws IOException
        {
            int length = getCompactInt( channel, sizeCode );
            byte[] data = new byte[length];
            channel.get( data, length );
            return ByteBuffer.wrap( data );
        }

        private void writeRelationship( WritableChannel channel, DenseRelationships.DenseRelationship relationship ) throws IOException
        {
            int numProperties = relationship.propertyUpdates.size();
            byte header = (byte) (longSizeCode( relationship.internalId ) | (longSizeCode( relationship.otherNodeId ) << 2) |
                    (intSizeCode( numProperties ) << 4) | (relationship.outgoing ? 0x40 : 0) | (relationship.deleted ? 0x80 : 0));
            channel.put( header );
            putCompactLong( channel, relationship.internalId );
            putCompactLong( channel, relationship.otherNodeId );
            putCompactInt( channel, numProperties );
            for ( PropertyUpdate property : relationship.propertyUpdates )
            {
                writeProperty( channel, property );
            }
        }

        private static void readRelationships( ReadableChannel channel, int numRelationships, DenseRelationships target )
                throws IOException
        {
            for ( int j = 0; j < numRelationships; j++ )
            {
                target.add( readRelationship( channel ) );
            }
        }

        private static DenseRelationships.DenseRelationship readRelationship( ReadableChannel channel ) throws IOException
        {
            byte header = channel.get();
            long internalId = getCompactLong( channel, header & 0x3 );
            long otherNodeId = getCompactLong( channel, (header >> 2) & 0x3 );
            int numProperties = getCompactInt( channel, (header >> 4) & 0x3 );
            boolean outgoing = (header & 0x40) != 0;
            boolean deleted = (header & 0x80) != 0;
            return new DenseRelationships.DenseRelationship( internalId, otherNodeId, outgoing, readProperties( channel, numProperties ), deleted );
        }

        static DenseNode deserialize( ReadableChannel channel ) throws IOException
        {
            byte header = channel.get();
            long nodeId = getCompactLong( channel, header & 0x3 );
            TreeMap<Integer,DenseRelationships> relationships = new TreeMap<>();
            int numRelationshipTypes = getCompactInt( channel, (header >>> 2) & 0x3 );
            for ( int i = 0; i < numRelationshipTypes; i++ )
            {
                byte typeHeader = channel.get();
                int type = getCompactInt( channel, typeHeader & 0x3 );
                int numRelationships = getCompactInt( channel, (typeHeader >> 2) & 0x3 );
                DenseRelationships relationshipsOfType = new DenseRelationships( nodeId, type );
                relationships.put( type, relationshipsOfType );
                readRelationships( channel, numRelationships, relationshipsOfType );
            }

            return new DenseNode( nodeId, relationships );
        }

        @Override
        public String toString()
        {
            return format( "DenseNode{%d,%n%s", nodeId, updatesToString() );
        }

        private String updatesToString()
        {
            StringBuilder toString = new StringBuilder();
            for ( DenseRelationships forType : relationshipUpdates.values() )
            {
                toString.append( "  type=" ).append( forType.type );
                for ( DenseRelationships.DenseRelationship relationship : forType.relationships )
                {
                    toString.append( format( "    %s%s", relationship.deleted ? "-" : "+", relationship ) );
                }
            }
            return toString.toString();
        }
    }

    static class BigPropertyValue extends FrekiCommand
    {
        static final byte TYPE = 3;

        final List<Record> records;

        BigPropertyValue( List<Record> records )
        {
            super( TYPE );
            this.records = records;
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
            channel.put( (byte) intSizeCode( records.size() ) );
            putCompactInt( channel, records.size() );
            for ( Record record : records )
            {
                channel.put( (byte) longSizeCode( record.id ) );
                putCompactLong( channel, record.id );
                record.serialize( channel );
            }
        }

        static BigPropertyValue deserialize( ReadableChannel channel ) throws IOException
        {
            byte header = channel.get();
            int numRecords = getCompactInt( channel, header & 0x3 );
            List<Record> records = new ArrayList<>();
            for ( int i = 0; i < numRecords; i++ )
            {
                byte idHeader = channel.get();
                long id = getCompactLong( channel, idHeader & 0x3 );
                records.add( Record.deserialize( channel, id ) );
            }
            return new BigPropertyValue( records );
        }

        @Override
        public String toString()
        {
            return "BigValue{" + records + '}';
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

        @Override
        public String toString()
        {
            return format( "%s{%s}", getClass().getSimpleName(), token );
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

        @Override
        public String toString()
        {
            return format( "Schema[%s,id:%d,%s]", mode, descriptor.getId(), descriptor );
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
            byte header = (byte) (intSizeCode( labelId ) | longSizeCode( count ) << 2);
            channel.put( header );
            putCompactInt( channel, labelId );
            putCompactLong( channel, count );
        }

        @Override
        public String toString()
        {
            return String.format( "UpdateCounts[(%s) %s %d]", label( labelId ), count < 0 ? "-" : "+", Math.abs( count ) );
        }

        static NodeCount deserialize( ReadableChannel channel ) throws IOException
        {
            byte header = channel.get();
            return new NodeCount( getCompactInt( channel, header & 0x3 ), getCompactLong( channel, (header >>> 2) & 0x3 ) );
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
            byte header = (byte) (intSizeCode( startLabelId ) | (intSizeCode( typeId ) << 2) | (intSizeCode( endLabelId ) << 4) | (longSizeCode( count ) << 6));
            channel.put( header );
            putCompactInt( channel, startLabelId );
            putCompactInt( channel, typeId );
            putCompactInt( channel, endLabelId );
            putCompactLong( channel, count );
        }

        @Override
        public String toString()
        {
            return String.format( "UpdateCounts[(%s)-%s->(%s) %s %d]",
                    label( startLabelId ), relationshipType( typeId ), label( endLabelId ),
                    count < 0 ? "-" : "+", Math.abs( count ) );
        }

        static RelationshipCount deserialize( ReadableChannel channel ) throws IOException
        {
            byte header = channel.get();
            int startLabelId = getCompactInt( channel, header & 0x3 );
            int typeId = getCompactInt( channel, (header >>> 2) & 0x3 );
            int endLabelId = getCompactInt( channel, (header >>> 4) & 0x3 );
            long count = getCompactLong( channel, (header >>> 6) & 0x3 );
            return new RelationshipCount( startLabelId, typeId, endLabelId, count );
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

    private static int longSizeCode( long value )
    {
        return value > 0xFFFFFFFFL ? 3 : value > 0xFFFF ? 2 : value > 0 ? 1 : 0;
    }

    private static WritableChannel putCompactLong( WritableChannel channel, long value ) throws IOException
    {
        int size = longSizeCode( value );
        return size == 3 ? channel.putLong( value ) : size == 2 ? channel.putInt( (int) value ) : size == 1 ? channel.putShort( (short) value ) : channel;
    }

    private static long getCompactLong( ReadableChannel channel, int size ) throws IOException
    {
        return size == 3 ? channel.getLong() : size == 2 ? channel.getInt() & 0xFFFFFFFFL : size == 1 ? channel.getShort() & 0xFFFF : 0;
    }

    private static int intSizeCode( int value )
    {
        return value > 0xFFFF ? 3 : value > 0xFF ? 2 : value > 0 ? 1 : 0;
    }

    private static WritableChannel putCompactInt( WritableChannel channel, int id ) throws IOException
    {
        int size = intSizeCode( id );
        return size == 3 ? channel.putInt( id ) : size == 2 ? channel.putShort( (short) id ) : size == 1 ? channel.put( (byte) id ) : channel;
    }

    private static int getCompactInt( ReadableChannel channel, int size ) throws IOException
    {
        return size == 3 ? channel.getInt() : size == 2 ? channel.getShort() & 0xFFFF : size == 1 ? channel.get() & 0xFF : 0;
    }
}
