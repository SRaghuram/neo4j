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
import org.eclipse.collections.impl.factory.primitive.IntObjectMaps;
import org.eclipse.collections.impl.factory.primitive.IntSets;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.IOException;
import java.nio.ByteBuffer;

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

import static org.assertj.core.api.Assertions.assertThat;
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
        long id = randomLargeId();
        Record before = new Record( sizeExp, id );
        Record after = new Record( sizeExp, id );
        after.setFlag( FLAG_IN_USE, true );
        fillWithRandomData( after );

        // when/then
        shouldReadAndWriteSparseNode( nodeId, before, after );
    }

    @Test
    void shouldReadAndWriteSparseNodeWithUsedBeforeUsedAfterRecords() throws IOException
    {
        // given
        int sizeExp = randomSizeExp();
        long nodeId = randomLargeId();
        long id = randomLargeId();
        Record before = new Record( sizeExp, id );
        before.setFlag( FLAG_IN_USE, true );
        fillWithRandomData( before );
        Record after = new Record( sizeExp, id );
        after.setFlag( FLAG_IN_USE, true );
        fillWithRandomData( after );

        // when/then
        shouldReadAndWriteSparseNode( nodeId, before, after );
    }

    @Test
    void shouldReadAndWriteSparseNodeWithUsedBeforeUnusedAfterRecords() throws IOException
    {
        // given
        int sizeExp = randomSizeExp();
        long nodeId = randomLargeId();
        long id = randomLargeId();
        Record before = new Record( sizeExp, id );
        before.setFlag( FLAG_IN_USE, true );
        fillWithRandomData( before );
        Record after = new Record( sizeExp, id );

        // when/then
        shouldReadAndWriteSparseNode( nodeId, before, after );
    }

    @Test
    void shouldReadAndWriteUsedDenseNode() throws IOException
    {
        // given
        long nodeId = randomLargeId();
        IntObjectMap<ByteBuffer> addedProperties = randomProperties();
        IntSet removedProperties = randomPropertyKeys();
        IntObjectMap<DenseRelationships> createdRelationships = randomRelationships( true );
        IntObjectMap<DenseRelationships> deletedRelationships = randomRelationships( false );
        FrekiCommand.DenseNode node =
                new FrekiCommand.DenseNode( nodeId, true, addedProperties, removedProperties, createdRelationships, deletedRelationships );

        // when
        InMemoryClosableChannel channel = new InMemoryClosableChannel( 5_000 );
        node.serialize( channel );

        // then
        FrekiCommand.DenseNode readNode = readCommand( channel, FrekiCommand.DenseNode.class );
        assertThat( readNode.nodeId ).isEqualTo( node.nodeId );
        assertThat( readNode.inUse ).isEqualTo( node.inUse );
        assertThat( readNode.addedProperties ).isEqualTo( node.addedProperties );
        assertThat( readNode.removedProperties ).isEqualTo( node.removedProperties );
        assertThat( readNode.createdRelationships ).isEqualTo( node.createdRelationships );
        assertThat( readNode.deletedRelationships ).isEqualTo( node.deletedRelationships );
    }

    @Test
    void shouldReadAndWriteBigValue() throws IOException
    {
        // given
        long pointer = randomLargeId();
        byte[] data = new byte[random.nextInt( 20, 400 )];
        random.nextBytes( data );
        FrekiCommand.BigPropertyValue command = new FrekiCommand.BigPropertyValue( pointer, data );

        // when
        InMemoryClosableChannel channel = new InMemoryClosableChannel( 1_000 );
        command.serialize( channel );

        // then
        FrekiCommand.BigPropertyValue readCommand = readCommand( channel, FrekiCommand.BigPropertyValue.class );
        assertThat( readCommand.pointer ).isEqualTo( pointer );
        assertThat( readCommand.bytes ).isEqualTo( data );
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

    private IntObjectMap<ByteBuffer> randomProperties()
    {
        MutableIntObjectMap<ByteBuffer> map = IntObjectMaps.mutable.empty();
        for ( int key : randomTokens() )
        {
            ByteBuffer serializedValue = ByteBuffer.wrap( new byte[512] );
            random.nextValue().writeTo( new PropertyValueFormat( bigValueStore, applyToStoreImmediately( bigValueStore ), serializedValue ) );
            serializedValue.flip();
            map.put( key, serializedValue );
        }
        return map;
    }

    private IntSet randomPropertyKeys()
    {
        return IntSets.mutable.of( randomTokens() );
    }

    private IntObjectMap<DenseRelationships> randomRelationships( boolean used )
    {
        MutableIntObjectMap<DenseRelationships> map = IntObjectMaps.mutable.empty();
        for ( int type : randomTokens() )
        {
            int count = random.nextInt( 1, 5 );
            DenseRelationships relationships = map.getIfAbsentPut( type, new DenseRelationships( type ) );
            for ( int i = 0; i < count; i++ )
            {
                relationships.add( randomLargeId(), randomLargeId(), random.nextBoolean(), used ? randomProperties() : IntObjectMaps.immutable.empty() );
            }
        }
        return map;
    }

    private void shouldReadAndWriteSparseNode( long nodeId, Record before, Record after ) throws IOException
    {
        FrekiCommand.SparseNode command = new FrekiCommand.SparseNode( nodeId, before, after );

        // when
        InMemoryClosableChannel channel = new InMemoryClosableChannel( 2_000 );
        command.serialize( channel );

        // then
        FrekiCommand.SparseNode readNode = readCommand( channel, FrekiCommand.SparseNode.class );
        assertThat( readNode.nodeId ).isEqualTo( nodeId );
        assertThat( readNode.before.hasSameContentsAs( before ) ).isTrue();
        assertThat( readNode.after.hasSameContentsAs( after ) ).isTrue();
    }

    private void fillWithRandomData( Record after )
    {
        ByteBuffer buffer = after.dataForWriting();
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
