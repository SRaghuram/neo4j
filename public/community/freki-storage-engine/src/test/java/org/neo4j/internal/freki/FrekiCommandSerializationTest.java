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
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.IOException;
import java.nio.ByteBuffer;
<<<<<<< HEAD
=======
import java.util.ArrayList;
import java.util.List;
import java.util.TreeMap;
>>>>>>> f26a3005d9b9a7f42b480941eb059582c7469aaa

import org.neo4j.internal.schema.IndexPrototype;
import org.neo4j.internal.schema.IndexProviderDescriptor;
import org.neo4j.internal.schema.LabelSchemaDescriptor;
import org.neo4j.internal.schema.RelationTypeSchemaDescriptor;
import org.neo4j.internal.schema.SchemaDescriptor;
import org.neo4j.internal.schema.SchemaRule;
import org.neo4j.internal.schema.constraints.ConstraintDescriptorFactory;
import org.neo4j.kernel.impl.transaction.log.InMemoryClosableChannel;
import org.neo4j.storageengine.api.StorageCommand;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.RandomExtension;
import org.neo4j.test.rule.RandomRule;
import org.neo4j.token.api.NamedToken;

<<<<<<< HEAD
=======
import static java.lang.Integer.max;
import static java.util.Arrays.copyOf;
>>>>>>> f26a3005d9b9a7f42b480941eb059582c7469aaa
import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.internal.freki.FrekiCommand.MODES;
import static org.neo4j.internal.freki.InMemoryBigValueTestStore.applyToStoreImmediately;
import static org.neo4j.internal.freki.Record.FLAG_IN_USE;

@ExtendWith( RandomExtension.class )
public class FrekiCommandSerializationTest
{
    @Inject
    private RandomRule random;

    private InMemoryBigValueTestStore bigValueStore = new InMemoryBigValueTestStore();

    @Test
    void shouldReadAndWriteSparseNodeWithUnusedBeforeUsedAfterRecords() throws IOException
    {
        // given
        int sizeExp = randomSizeExp();
        long nodeId = randomLargeId();
<<<<<<< HEAD
        long id = randomLargeId();
        Record before = new Record( sizeExp, id );
        Record after = new Record( sizeExp, id );
        after.setFlag( FLAG_IN_USE, true );
        fillWithRandomData( after );

        // when/then
        shouldReadAndWriteSparseNode( nodeId, before, after );
=======
        long id = sizeExp == 0 ? nodeId : randomLargeId();
        Record after = recordWithRandomData( sizeExp, id );

        // when/then
        shouldReadAndWriteSparseNode( nodeId, new FrekiCommand.RecordChange( null, after ) );
>>>>>>> f26a3005d9b9a7f42b480941eb059582c7469aaa
    }

    @Test
    void shouldReadAndWriteSparseNodeWithUsedBeforeUsedAfterRecords() throws IOException
    {
        // given
        int sizeExp = randomSizeExp();
        long nodeId = randomLargeId();
<<<<<<< HEAD
        long id = randomLargeId();
        Record before = new Record( sizeExp, id );
        before.setFlag( FLAG_IN_USE, true );
        fillWithRandomData( before );
        Record after = new Record( sizeExp, id );
        after.setFlag( FLAG_IN_USE, true );
        fillWithRandomData( after );

        // when/then
        shouldReadAndWriteSparseNode( nodeId, before, after );
=======
        long id = sizeExp == 0 ? nodeId : randomLargeId();
        Record before = recordWithRandomData( sizeExp, id );
        Record after = recordWithRandomData( sizeExp, id );

        // when/then
        shouldReadAndWriteSparseNode( nodeId, new FrekiCommand.RecordChange( before, after ) );
>>>>>>> f26a3005d9b9a7f42b480941eb059582c7469aaa
    }

    @Test
    void shouldReadAndWriteSparseNodeWithUsedBeforeUnusedAfterRecords() throws IOException
    {
        // given
        int sizeExp = randomSizeExp();
        long nodeId = randomLargeId();
<<<<<<< HEAD
        long id = randomLargeId();
        Record before = new Record( sizeExp, id );
        before.setFlag( FLAG_IN_USE, true );
        fillWithRandomData( before );
        Record after = new Record( sizeExp, id );

        // when/then
        shouldReadAndWriteSparseNode( nodeId, before, after );
=======
        long id = sizeExp == 0 ? nodeId : randomLargeId();
        Record before = recordWithRandomData( sizeExp, id );

        // when/then
        shouldReadAndWriteSparseNode( nodeId, new FrekiCommand.RecordChange( before, null ) );
    }

    @Test
    void shouldReadAndWriteSparseNodeWithMultipleChanges() throws IOException
    {
        // given
        long nodeId = randomLargeId();
        Record x1After = recordWithRandomData( 0, nodeId );
        FrekiCommand.RecordChange x1 = new FrekiCommand.RecordChange( null, x1After );

        int sizeExp = max( 1, randomSizeExp() );
        long id = randomLargeId();
        Record before = recordWithRandomData( sizeExp, id );
        Record after = recordWithRandomData( sizeExp, id );
        FrekiCommand.RecordChange xL = new FrekiCommand.RecordChange( before, after );

        // when/then
        shouldReadAndWriteSparseNode( nodeId, x1, xL );
>>>>>>> f26a3005d9b9a7f42b480941eb059582c7469aaa
    }

    @Test
    void shouldReadAndWriteUsedDenseNode() throws IOException
    {
        // given
        long nodeId = randomLargeId();
<<<<<<< HEAD
        IntObjectMap<DenseRelationships> relationships = randomRelationships();
=======
        TreeMap<Integer,DenseRelationships> relationships = randomRelationships();
>>>>>>> f26a3005d9b9a7f42b480941eb059582c7469aaa
        FrekiCommand.DenseNode node = new FrekiCommand.DenseNode( nodeId, relationships );

        // when
        InMemoryClosableChannel channel = new InMemoryClosableChannel( 50_000 );
        node.serialize( channel );

        // then
        FrekiCommand.DenseNode readNode = readCommand( channel, FrekiCommand.DenseNode.class );
        assertThat( readNode.nodeId ).isEqualTo( node.nodeId );
        assertThat( readNode.relationshipUpdates ).isEqualTo( node.relationshipUpdates );
    }

    @Test
    void shouldReadAndWriteBigValue() throws IOException
    {
        // given
<<<<<<< HEAD
        long pointer = randomLargeId();
        byte[] data = new byte[random.nextInt( 20, 400 )];
        random.nextBytes( data );
        FrekiCommand.BigPropertyValue command = new FrekiCommand.BigPropertyValue( pointer, data );

        // when
        InMemoryClosableChannel channel = new InMemoryClosableChannel( 1_000 );
=======
        List<Record> records = new ArrayList<>();
        int numRecords = random.nextInt( 1, 3 );
        for ( int i = 0; i < numRecords; i++ )
        {
            byte[] data = new byte[random.nextInt( 20, 400 )];
            random.nextBytes( data );
            records.add( new Record( (byte) FLAG_IN_USE, randomLargeId(), ByteBuffer.wrap( data ) ) );
        }
        FrekiCommand.BigPropertyValue command = new FrekiCommand.BigPropertyValue( records );

        // when
        InMemoryClosableChannel channel = new InMemoryClosableChannel( 2_000 );
>>>>>>> f26a3005d9b9a7f42b480941eb059582c7469aaa
        command.serialize( channel );

        // then
        FrekiCommand.BigPropertyValue readCommand = readCommand( channel, FrekiCommand.BigPropertyValue.class );
<<<<<<< HEAD
        assertThat( readCommand.pointer ).isEqualTo( pointer );
        assertThat( readCommand.bytes ).isEqualTo( data );
=======
        assertThat( readCommand.records.size() ).isEqualTo( numRecords );
        for ( int i = 0; i < numRecords; i++ )
        {
            Record readRecord = readCommand.records.get( i );
            Record record = records.get( i );
            assertThat( readRecord.flags ).isEqualTo( record.flags );
            assertThat( readRecord.id ).isEqualTo( record.id );
            int length = readRecord.data().limit();
            assertThat( length ).isEqualTo( record.data().limit() );
            assertThat( copyOf( readRecord.data().array(), length ) ).isEqualTo( copyOf( record.data().array(), length ) );
        }
>>>>>>> f26a3005d9b9a7f42b480941eb059582c7469aaa
    }

    @Test
    void shouldReadAndWritePropertyKeyToken() throws Exception
    {
        shouldReadAndWriteToken( FrekiCommand.PropertyKeyToken.class );
    }

    @Test
    void shouldReadAndWriteLabelToken() throws Exception
    {
        shouldReadAndWriteToken( FrekiCommand.LabelToken.class );
    }

    @Test
    void shouldReadAndWriteRelationshipTypeToken() throws Exception
    {
        shouldReadAndWriteToken( FrekiCommand.RelationshipTypeToken.class );
    }

    @Test
    void shouldReadAndWriteSchema() throws IOException
    {
        // given
        SchemaRule rule = randomSchemaRule();
        FrekiCommand.Mode mode = random.among( FrekiCommand.Mode.values() );
        FrekiCommand.Schema command = new FrekiCommand.Schema( rule, mode );

        // when
        InMemoryClosableChannel channel = new InMemoryClosableChannel( 2_000 );
        command.serialize( channel );

        // then
        FrekiCommand.Schema readCommand = readCommand( channel, FrekiCommand.Schema.class );
        assertThat( readCommand.mode ).isEqualTo( mode );
        assertThat( readCommand.descriptor ).isEqualTo( rule );
    }

    private SchemaRule randomSchemaRule()
    {
        if ( random.nextBoolean() )
        {
            // Index
            SchemaDescriptor schema = randomSchema();
            IndexProviderDescriptor provider = randomIndexProvider();
            IndexPrototype prototype = random.nextBoolean()
                    ? IndexPrototype.forSchema( schema, provider )
                    : IndexPrototype.uniqueForSchema( schema, provider );
            prototype = prototype.withName( random.nextAlphaNumericString() );
            return prototype.materialise( randomLargeId() );
        }
        else
        {
            // Constraint
            switch ( random.nextInt( 3 ) )
            {
            case 0:
                return ConstraintDescriptorFactory.uniqueForSchema( randomNodeSchema() )
                        .withName( random.nextAlphaNumericString() )
                        .withId( randomLargeId() )
                        .withOwnedIndexId( randomLargeId() );
            case 1:
                return ConstraintDescriptorFactory.existsForSchema( randomSchema() )
                        .withName( random.nextAlphaNumericString() )
                        .withId( randomLargeId() );
            case 2:
                return ConstraintDescriptorFactory.nodeKeyForSchema( randomNodeSchema() )
                        .withName( random.nextAlphaNumericString() )
                        .withId( randomLargeId() );
            default:
                throw new UnsupportedOperationException( "Unrecognized option" );
            }
        }
    }

    private IndexProviderDescriptor randomIndexProvider()
    {
        return new IndexProviderDescriptor( random.nextAlphaNumericString( 3, 10 ), random.nextAlphaNumericString( 3, 10 ) );
    }

    private SchemaDescriptor randomSchema()
    {
        return random.nextBoolean() ? randomNodeSchema() : randomRelationshipSchema();
    }

    private RelationTypeSchemaDescriptor randomRelationshipSchema()
    {
        return SchemaDescriptor.forRelType( randomTokenId(), randomTokens( 1 ) );
    }

    private LabelSchemaDescriptor randomNodeSchema()
    {
        return SchemaDescriptor.forLabel( randomTokenId(), randomTokens( 1 ) );
    }

    <T extends FrekiCommand.Token> void shouldReadAndWriteToken( Class<T> cls ) throws Exception
    {
        // given
        NamedToken token = new NamedToken( random.nextAlphaNumericString(), randomTokenId() );
        T command = cls.getDeclaredConstructor( NamedToken.class ).newInstance( token );

        // when
        InMemoryClosableChannel channel = new InMemoryClosableChannel( 200 );
        command.serialize( channel );

        // then
        T readCommand = readCommand( channel, cls );
        assertThat( readCommand.token ).isEqualTo( token );
    }

    private int randomTokenId()
    {
        return random.nextInt( 0xFFFFFF );
    }

<<<<<<< HEAD
=======
    private Record recordWithRandomData( int sizeExp, long id )
    {
        Record after = new Record( sizeExp, id );
        after.setFlag( FLAG_IN_USE, true );
        fillWithRandomData( after );
        return after;
    }

>>>>>>> f26a3005d9b9a7f42b480941eb059582c7469aaa
    private <T extends StorageCommand> T readCommand( InMemoryClosableChannel channel, Class<T> cls ) throws IOException
    {
        StorageCommand readCommand = FrekiCommandReader.INSTANCE.read( channel );
        assertThat( readCommand ).isInstanceOf( cls );
        return (T) readCommand;
    }

    private int[] randomTokens()
    {
        return randomTokens( 0 );
    }

    private int[] randomTokens( int atLeast )
    {
        int length = random.nextInt( atLeast, 5 );
        int[] tokens = new int[length];
        int maxSkip = 100;
        for ( int i = 0, token = random.nextInt( maxSkip ); i < length; i++, token += random.nextInt( 1, maxSkip ) )
        {
            tokens[i] = token;
        }
        return tokens;
    }

    private IntObjectMap<PropertyUpdate> randomProperties()
    {
        MutableIntObjectMap<PropertyUpdate> map = IntObjectMaps.mutable.empty();
        for ( int key : randomTokens() )
        {
            ByteBuffer serializedValue = randomSerializedValue();
            PropertyUpdate update;
            switch ( random.among( MODES ) )
            {
            case CREATE:
                update = PropertyUpdate.add( key, serializedValue );
                break;
            case UPDATE:
                update = PropertyUpdate.change( key, randomSerializedValue(), serializedValue );
                break;
            case DELETE:
                update = PropertyUpdate.remove( key, serializedValue );
                break;
            default:
                throw new IllegalArgumentException( "Unrecognized mode" );
            }
            map.put( key, update );
        }
        return map;
    }

    private ByteBuffer randomSerializedValue()
    {
        ByteBuffer serializedValue = ByteBuffer.wrap( new byte[512] );
        random.nextValue().writeTo( new PropertyValueFormat( bigValueStore, applyToStoreImmediately( bigValueStore ), serializedValue ) );
        serializedValue.flip();
        return serializedValue;
    }

<<<<<<< HEAD
    private IntObjectMap<DenseRelationships> randomRelationships()
    {
        MutableIntObjectMap<DenseRelationships> map = IntObjectMaps.mutable.empty();
        for ( int type : randomTokens() )
        {
            DenseRelationships relationships = map.getIfAbsentPut( type, new DenseRelationships( 0, type ) );
            for ( int i = 0, count = random.nextInt( 1, 5 ); i < count; i++ )
            {
                relationships.create( new DenseRelationships.DenseRelationship( randomLargeId(), randomLargeId(), random.nextBoolean(), randomProperties() ) );
            }
            for ( int i = 0, count = random.nextInt( 1, 5 ); i < count; i++ )
            {
                relationships.delete( new DenseRelationships.DenseRelationship( randomLargeId(), randomLargeId(), random.nextBoolean(), randomProperties() ) );
=======
    private TreeMap<Integer,DenseRelationships> randomRelationships()
    {
        TreeMap<Integer,DenseRelationships> map = new TreeMap<>();
        for ( int type : randomTokens() )
        {
            DenseRelationships relationships = map.computeIfAbsent( type, t -> new DenseRelationships( 0, type ) );
            for ( int i = 0, count = random.nextInt( 2, 10 ); i < count; i++ )
            {
                relationships.add( new DenseRelationships.DenseRelationship( randomLargeId(), randomLargeId(), random.nextBoolean(), randomProperties(),
                        random.nextBoolean() ) );
>>>>>>> f26a3005d9b9a7f42b480941eb059582c7469aaa
            }
        }
        return map;
    }

<<<<<<< HEAD
    private void shouldReadAndWriteSparseNode( long nodeId, Record before, Record after ) throws IOException
    {
        FrekiCommand.SparseNode command = new FrekiCommand.SparseNode( nodeId, before, after );

        // when
        InMemoryClosableChannel channel = new InMemoryClosableChannel( 2_000 );
=======
    private void shouldReadAndWriteSparseNode( long nodeId, FrekiCommand.RecordChange... changes ) throws IOException
    {
        // given
        FrekiCommand.SparseNode command = new FrekiCommand.SparseNode( nodeId );
        for ( FrekiCommand.RecordChange change : changes )
        {
            FrekiCommand.RecordChange addedChange = command.addChange( change.before, change.after );
        }

        // when
        InMemoryClosableChannel channel = new InMemoryClosableChannel( 3_000 );
>>>>>>> f26a3005d9b9a7f42b480941eb059582c7469aaa
        command.serialize( channel );

        // then
        FrekiCommand.SparseNode readNode = readCommand( channel, FrekiCommand.SparseNode.class );
        assertThat( readNode.nodeId ).isEqualTo( nodeId );
<<<<<<< HEAD
        assertThat( readNode.before.hasSameContentsAs( before ) ).isTrue();
        assertThat( readNode.after.hasSameContentsAs( after ) ).isTrue();
=======
        FrekiCommand.RecordChange readChange = readNode.change();
        for ( FrekiCommand.RecordChange change : changes )
        {
            assertRecord( change.before, readChange.before );
            assertRecord( change.after, readChange.after );
            readChange = readChange.next();
        }
        assertThat( readChange ).isNull();
    }

    private void assertRecord( Record expected, Record actual )
    {
        if ( expected == null )
        {
            assertThat( actual ).isNull();
        }
        else
        {
            assertThat( actual.hasSameContentsAs( expected ) ).isTrue();
        }
>>>>>>> f26a3005d9b9a7f42b480941eb059582c7469aaa
    }

    private void fillWithRandomData( Record after )
    {
        ByteBuffer buffer = after.data();
        int length = random.nextInt( buffer.capacity() );
        byte[] data = new byte[length];
        random.nextBytes( data );
        buffer.put( data );
    }

    private int randomSizeExp()
    {
        return random.nextInt( 4 );
    }

    private long randomLargeId()
    {
        return random.nextLong( 0xFFFF_FFFFFFFFL );
    }
}
