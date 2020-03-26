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

import org.apache.commons.lang3.tuple.Triple;
import org.eclipse.collections.api.map.primitive.MutableIntObjectMap;
import org.eclipse.collections.api.multimap.MutableMultimap;
import org.eclipse.collections.api.set.primitive.MutableIntSet;
import org.eclipse.collections.api.set.primitive.MutableLongSet;
import org.eclipse.collections.impl.factory.Multimaps;
import org.eclipse.collections.impl.factory.primitive.IntObjectMaps;
import org.eclipse.collections.impl.factory.primitive.IntSets;
import org.eclipse.collections.impl.factory.primitive.LongSets;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestFactory;
import org.junit.jupiter.api.function.Executable;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.neo4j.graphdb.Direction;
import org.neo4j.storageengine.api.RelationshipDirection;
import org.neo4j.storageengine.api.RelationshipSelection;
import org.neo4j.values.storable.Value;

import static java.lang.String.format;
import static java.util.Arrays.asList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer.NULL;
import static org.neo4j.storageengine.api.RelationshipDirection.INCOMING;
import static org.neo4j.storageengine.api.RelationshipDirection.LOOP;
import static org.neo4j.storageengine.api.RelationshipDirection.OUTGOING;
import static org.neo4j.storageengine.api.RelationshipSelection.ALL_RELATIONSHIPS;
import static org.neo4j.storageengine.api.RelationshipSelection.selection;
import static org.neo4j.values.storable.Values.intValue;
import static org.neo4j.values.storable.Values.longValue;
import static org.neo4j.values.storable.Values.stringValue;

class FrekiNodeCursorTest extends FrekiCursorsTest
{
    // **** LABELS ****

    @Test
    void shouldNotSeeUnusedNode()
    {
        // given
        Node node = node();
        node.delete().store();
        FrekiNodeCursor cursor = cursorFactory.allocateNodeCursor( NULL );

        // when
        cursor.single( node.id() );
        boolean hasNext = cursor.next();

        // then
        assertFalse( hasNext );
    }

    @Test
    void shouldSeeEmptyLabelSet()
    {
        // given
        FrekiNodeCursor cursor = node().storeAndPlaceNodeCursorAt();

        // then
        assertEquals( 0, cursor.labels().length );
    }

    @Test
    void shouldSeeSingleLabel()
    {
        // given
        FrekiNodeCursor cursor = node().labels( 4 ).storeAndPlaceNodeCursorAt();

        // then
        assertArrayEquals( new long[]{4}, cursor.labels() );
    }

    @Test
    void shouldSeeMultipleLabels()
    {
        // given
        int[] labelIds = {2, 4, 100, 145, 678};
        FrekiNodeCursor cursor = node().labels( labelIds ).storeAndPlaceNodeCursorAt();

        // then
        assertArrayEquals( toLongArray( labelIds ), cursor.labels() );
    }

    @Test
    void shouldSeeMultipleUnsortedLabelsAsSorted()
    {
        // given
        int[] unsortedLabelIds = {45, 5, 3, 999, 389};
        FrekiNodeCursor cursor = node().labels( unsortedLabelIds ).storeAndPlaceNodeCursorAt();

        // then
        long[] expectedLabelIds = toLongArray( unsortedLabelIds );
        Arrays.sort( expectedLabelIds );
        assertArrayEquals( expectedLabelIds, cursor.labels() );
    }

    // **** PROPERTIES ****

    @ParameterizedTest
    @EnumSource( EntityAndPropertyConnector.class )
    void shouldSeeEmptyPropertySet( EntityAndPropertyConnector connector )
    {
        // given
        FrekiNodeCursor cursor = node().storeAndPlaceNodeCursorAt();
        assertFalse( cursor.hasProperties() );

        // when
        FrekiPropertyCursor propertyCursor = cursorFactory.allocatePropertyCursor( NULL );
        connector.connect( cursor, propertyCursor );

        // then
        assertFalse( propertyCursor.next() );
    }

    @ParameterizedTest
    @EnumSource( EntityAndPropertyConnector.class )
    void shouldSeeSingleProperty( EntityAndPropertyConnector connector )
    {
        // given
        int propertyKeyId = 67;
        Value value = intValue( 999123 );
        FrekiNodeCursor cursor = node().property( propertyKeyId, value ).storeAndPlaceNodeCursorAt();
        assertTrue( cursor.hasProperties() );
        FrekiPropertyCursor propertyCursor = cursorFactory.allocatePropertyCursor( NULL );
        connector.connect( cursor, propertyCursor );

        // when/then
        assertTrue( propertyCursor.next() );
        assertEquals( propertyKeyId, propertyCursor.propertyKey() );
        assertEquals( value, propertyCursor.propertyValue() );
        assertFalse( propertyCursor.next() );
    }

    @ParameterizedTest
    @EnumSource( EntityAndPropertyConnector.class )
    void shouldSeeMultipleProperties( EntityAndPropertyConnector connector )
    {
        // given
        MutableIntObjectMap<Value> properties = IntObjectMaps.mutable.empty();
        properties.put( 23, intValue( -200 ) );
        properties.put( 678, longValue( 98765L ) );
        properties.put( 3, stringValue( "abc" ) );
        FrekiNodeCursor cursor = node().properties( properties ).storeAndPlaceNodeCursorAt();
        assertTrue( cursor.hasProperties() );
        FrekiPropertyCursor propertyCursor = cursorFactory.allocatePropertyCursor( NULL );
        connector.connect( cursor, propertyCursor );

        // when
        MutableIntObjectMap<Value> readProperties = readProperties( propertyCursor );

        // then
        assertEquals( properties, readProperties );
    }

    @ParameterizedTest
    @EnumSource( EntityAndPropertyConnector.class )
    void shouldSeeMultiplePropertyKeysWithoutReadingValues( EntityAndPropertyConnector connector )
    {
        // given
        MutableIntObjectMap<Value> properties = IntObjectMaps.mutable.empty();
        properties.put( 23, intValue( 12345 ) );
        properties.put( 678, longValue( 98765L ) );
        properties.put( 3, stringValue( "abc" ) );
        FrekiNodeCursor cursor = node().properties( properties ).storeAndPlaceNodeCursorAt();
        assertTrue( cursor.hasProperties() );
        FrekiPropertyCursor propertyCursor = cursorFactory.allocatePropertyCursor( NULL );
        connector.connect( cursor, propertyCursor );

        // when
        MutableIntSet readPropertyKeys = IntSets.mutable.empty();
        int lastPropertyKey = -1;
        Value lastPropertyValue = null;
        for ( int i = 0; propertyCursor.next(); i++ )
        {
            readPropertyKeys.add( propertyCursor.propertyKey() );
            if ( i == properties.size() - 1 )
            {
                lastPropertyKey = propertyCursor.propertyKey();
                lastPropertyValue = propertyCursor.propertyValue();
            }
        }

        // then
        assertEquals( properties.keySet(), readPropertyKeys );
        assertEquals( properties.get( lastPropertyKey ), lastPropertyValue );
    }

    // **** RELATIONSHIPS ****

    @ParameterizedTest
    @EnumSource( AllRelationshipsConnector.class )
    void shouldSeeEmptyRelationshipSet( AllRelationshipsConnector connector )
    {
        // given
        FrekiNodeCursor cursor = node().storeAndPlaceNodeCursorAt();

        // when
        try ( FrekiRelationshipTraversalCursor relationshipCursor = cursorFactory.allocateRelationshipTraversalCursor( NULL ) )
        {
            connector.connect( cursor, relationshipCursor, ALL_RELATIONSHIPS );

            // then
            assertFalse( relationshipCursor.next() );
        }
    }

    @ParameterizedTest
    @EnumSource( AllRelationshipsConnector.class )
    void shouldSeeSingleRelationship( AllRelationshipsConnector connector )
    {
        // given
        int type = 99;
        Node otherNode = node();
        Node node = node();
        node.relationship( type, otherNode );
        FrekiNodeCursor cursor = node.storeAndPlaceNodeCursorAt();
        Record otherNodeRecord = otherNode.store();

        // when
        try ( FrekiRelationshipTraversalCursor relationshipCursor = cursorFactory.allocateRelationshipTraversalCursor( NULL ) )
        {
            connector.connect( cursor, relationshipCursor, ALL_RELATIONSHIPS );

            // then
            assertTrue( relationshipCursor.next() );
            assertEquals( type, relationshipCursor.type() );
            assertEquals( cursor.entityReference(), relationshipCursor.sourceNodeReference() );
            assertEquals( otherNodeRecord.id, relationshipCursor.targetNodeReference() );
            assertFalse( relationshipCursor.next() );
        }
    }

    @ParameterizedTest
    @EnumSource( AllRelationshipsConnector.class )
    void shouldSeeMultipleRelationshipOfSingleType( AllRelationshipsConnector connector )
    {
        // given
        int type = 44;
        Node node1 = node();
        Node node2 = node();
        Node node3 = node();
        node1.relationship( type, node2 );
        node3.relationship( type, node1 );
        FrekiNodeCursor cursor = node1.storeAndPlaceNodeCursorAt();
        Record node2Record = node2.store();
        Record node3Record = node3.store();

        // when
        MutableLongSet otherNodes;
        try ( FrekiRelationshipTraversalCursor relationshipCursor = cursorFactory.allocateRelationshipTraversalCursor( NULL ) )
        {
            connector.connect( cursor, relationshipCursor, ALL_RELATIONSHIPS );

            // then
            otherNodes = LongSets.mutable.empty();
            while ( relationshipCursor.next() )
            {
                assertEquals( type, relationshipCursor.type() );
                assertEquals( cursor.entityReference(), relationshipCursor.originNodeReference() );
                otherNodes.add( relationshipCursor.neighbourNodeReference() );
            }
        }
        assertEquals( LongSets.mutable.of( node2Record.id, node3Record.id ), otherNodes );
    }

    @ParameterizedTest
    @EnumSource( AllRelationshipsConnector.class )
    void shouldSeeSingleRelationshipOfMultipleTypes( AllRelationshipsConnector connector )
    {
        // given
        int type1 = 11;
        int type2 = 22;
        Node node1 = node();
        Node node2 = node();
        Node node3 = node();
        node1.relationship( type1, node2 );
        node1.relationship( type2, node3 );
        FrekiNodeCursor cursor = node1.storeAndPlaceNodeCursorAt();
        Record node2Record = node2.store();
        Record node3Record = node3.store();

        // when
        try ( FrekiRelationshipTraversalCursor relationshipCursor = cursorFactory.allocateRelationshipTraversalCursor( NULL ) )
        {
            connector.connect( cursor, relationshipCursor, ALL_RELATIONSHIPS );

            // then
            assertTrue( relationshipCursor.next() );
            assertEquals( type1, relationshipCursor.type() );
            assertEquals( node2Record.id, relationshipCursor.neighbourNodeReference() );

            assertTrue( relationshipCursor.next() );
            assertEquals( type2, relationshipCursor.type() );
            assertEquals( node3Record.id, relationshipCursor.neighbourNodeReference() );

            assertFalse( relationshipCursor.next() );
        }
    }

    @ParameterizedTest
    @EnumSource( AllRelationshipsConnector.class )
    void shouldSeeMultipleRelationshipOfMultipleTypes( AllRelationshipsConnector connector )
    {
        // given
        int type1 = 11;
        int type2 = 22;
        Node node1 = node();
        Node node2 = node();
        Node node3 = node();
        Node node4 = node();
        Node node5 = node();
        node1.relationship( type1, node2 );
        node3.relationship( type1, node1 );
        node1.relationship( type2, node4 );
        node5.relationship( type2, node1 );
        FrekiNodeCursor cursor = node1.storeAndPlaceNodeCursorAt();
        Record node2Record = node2.store();
        Record node3Record = node3.store();
        Record node4Record = node4.store();
        Record node5Record = node5.store();

        // when
        MutableIntObjectMap<MutableLongSet> otherNodes;
        try ( FrekiRelationshipTraversalCursor relationshipCursor = cursorFactory.allocateRelationshipTraversalCursor( NULL ) )
        {
            connector.connect( cursor, relationshipCursor, ALL_RELATIONSHIPS );

            // then
            otherNodes = IntObjectMaps.mutable.empty();
            while ( relationshipCursor.next() )
            {
                otherNodes.getIfAbsentPut( relationshipCursor.type(), LongSets.mutable::empty ).add( relationshipCursor.neighbourNodeReference() );
            }
        }
        MutableIntObjectMap<MutableLongSet> expectedOtherNodes = IntObjectMaps.mutable.empty();
        expectedOtherNodes.put( type1, LongSets.mutable.of( node2Record.id, node3Record.id ) );
        expectedOtherNodes.put( type2, LongSets.mutable.of( node4Record.id, node5Record.id ) );
        assertEquals( expectedOtherNodes, otherNodes );
    }

    @ParameterizedTest
    @MethodSource( "allRelationshipsAndPropertyConnector" )
    void shouldSeeSingleRelationshipWithSingleProperty( AllRelationshipsConnector relationshipsConnector, EntityAndPropertyConnector propertyConnector )
    {
        // given
        int type = 99;
        MutableIntObjectMap<Value> properties = IntObjectMaps.mutable.empty();
        properties.put( 987, intValue( 12345 ) );
        Node otherNode = node();
        Node node = node();
        node.relationship( type, otherNode, properties );
        FrekiNodeCursor cursor = node.storeAndPlaceNodeCursorAt();
        otherNode.store();

        // when
        try ( FrekiRelationshipTraversalCursor relationshipCursor = cursorFactory.allocateRelationshipTraversalCursor( NULL ) )
        {
            relationshipsConnector.connect( cursor, relationshipCursor, ALL_RELATIONSHIPS );
            assertTrue( relationshipCursor.next() );

            // then
            FrekiPropertyCursor propertyCursor = cursorFactory.allocatePropertyCursor( NULL );
            propertyConnector.connect( relationshipCursor, propertyCursor );
            assertEquals( properties, readProperties( propertyCursor ) );

            assertFalse( relationshipCursor.next() );
        }
    }

    @ParameterizedTest
    @MethodSource( "allRelationshipsAndPropertyConnector" )
    void shouldSeeSingleRelationshipWithMultipleProperties( AllRelationshipsConnector relationshipsConnector, EntityAndPropertyConnector propertyConnector )
    {
        // given
        int type = 99;
        MutableIntObjectMap<Value> properties = IntObjectMaps.mutable.empty();
        properties.put( 111, longValue( 43879348578L ) );
        properties.put( 222, stringValue( "a-to-z" ) );
        Node otherNode = node();
        Node node = node();
        node.relationship( type, otherNode, properties );
        FrekiNodeCursor cursor = node.storeAndPlaceNodeCursorAt();
        otherNode.store();

        // when
        try ( FrekiRelationshipTraversalCursor relationshipCursor = cursorFactory.allocateRelationshipTraversalCursor( NULL ) )
        {
            relationshipsConnector.connect( cursor, relationshipCursor, ALL_RELATIONSHIPS );
            assertTrue( relationshipCursor.next() );

            // then
            FrekiPropertyCursor propertyCursor = cursorFactory.allocatePropertyCursor( NULL );
            propertyConnector.connect( relationshipCursor, propertyCursor );
            assertEquals( properties, readProperties( propertyCursor ) );

            assertFalse( relationshipCursor.next() );
        }
    }

    @ParameterizedTest
    @MethodSource( "allRelationshipsAndPropertyConnector" )
    void shouldSeeMultipleRelationshipAllWithProperties( AllRelationshipsConnector relationshipsConnector, EntityAndPropertyConnector propertyConnector )
    {
        // given
        int type = 99;
        MutableIntObjectMap<Value> properties = IntObjectMaps.mutable.empty();
        properties.put( 111, intValue( 5 ) );
        properties.put( 222, stringValue( "OMG" ) );
        Node otherNode = node();
        Node thirdNode = node();
        Node node = node();
        node.relationship( type, otherNode, properties );
        node.relationship( type, thirdNode, properties );
        FrekiNodeCursor cursor = node.storeAndPlaceNodeCursorAt();
        otherNode.store();
        int numRelationships;
        try ( FrekiRelationshipTraversalCursor relationshipCursor = cursorFactory.allocateRelationshipTraversalCursor( NULL ) )
        {
            relationshipsConnector.connect( cursor, relationshipCursor, ALL_RELATIONSHIPS );

            // when/then
            FrekiPropertyCursor propertyCursor = cursorFactory.allocatePropertyCursor( NULL );
            numRelationships = 0;
            while ( relationshipCursor.next() )
            {
                propertyConnector.connect( relationshipCursor, propertyCursor );
                assertEquals( properties, readProperties( propertyCursor ) );
                numRelationships++;
            }
        }
        assertEquals( 2, numRelationships );
    }

    @ParameterizedTest
    @MethodSource( "allRelationshipsAndPropertyConnector" )
    void shouldSeeMultipleRelationshipSomeWithProperties( AllRelationshipsConnector relationshipsConnector, EntityAndPropertyConnector propertyConnector )
    {
        // given
        int type = 99;
        MutableIntObjectMap<Value> properties = IntObjectMaps.mutable.empty();
        properties.put( 111, intValue( 5 ) );
        properties.put( 222, stringValue( "OMG" ) );
        Node otherNode = node();
        Node thirdNode = node();
        Node node = node();
        node.relationship( type, otherNode, properties );
        node.relationship( type, thirdNode );
        FrekiNodeCursor cursor = node.storeAndPlaceNodeCursorAt();
        Record otherNodeRecord = otherNode.store();
        thirdNode.store();
        int numRelationships;
        try ( FrekiRelationshipTraversalCursor relationshipCursor = cursorFactory.allocateRelationshipTraversalCursor( NULL ) )
        {
            relationshipsConnector.connect( cursor, relationshipCursor, ALL_RELATIONSHIPS );

            // when/then
            FrekiPropertyCursor propertyCursor = cursorFactory.allocatePropertyCursor( NULL );
            numRelationships = 0;
            while ( relationshipCursor.next() )
            {
                propertyConnector.connect( relationshipCursor, propertyCursor );
                assertEquals( relationshipCursor.neighbourNodeReference() == otherNodeRecord.id ? properties : IntObjectMaps.mutable.empty(),
                        readProperties( propertyCursor ) );
                numRelationships++;
            }
        }
        assertEquals( 2, numRelationships );
    }

    @ParameterizedTest
    @EnumSource( AllRelationshipsConnector.class )
    void shouldSeeRelationshipsInDifferentDirections( AllRelationshipsConnector relationshipsConnector )
    {
        // given
        int type = 99;
        Node node = node();
        Node otherNode = node();
        node.relationship( type, otherNode ); // OUTGOING
        otherNode.relationship( type, node ); // INCOMING
        node.relationship( type, node );      // LOOP
        otherNode.store();
        FrekiNodeCursor cursor = node.storeAndPlaceNodeCursorAt();
        Set<RelationshipDirection> expectedDirections;
        try ( FrekiRelationshipTraversalCursor relationshipCursor = cursorFactory.allocateRelationshipTraversalCursor( NULL ) )
        {
            relationshipsConnector.connect( cursor, relationshipCursor, ALL_RELATIONSHIPS );

            // when/then
            expectedDirections = new HashSet<>( asList( OUTGOING, INCOMING, LOOP ) );
            while ( relationshipCursor.next() )
            {
                RelationshipDirection direction = relationshipCursor.sourceNodeReference() == cursor.entityReference() ?
                                                  relationshipCursor.targetNodeReference() == cursor.entityReference() ? LOOP : OUTGOING : INCOMING;
                assertTrue( expectedDirections.remove( direction ) );
            }
        }
        assertTrue( expectedDirections.isEmpty() );
    }

    @Test
    void shouldFindAllRelationshipsInScan()
    {
        // given
        int type = 99;
        node().store();
        node().store();
        Node node = node();
        Node otherNode = node();
        node.relationship( type, otherNode ); // OUTGOING
        node().store();
        node().store();
        otherNode.relationship( type, node ); // INCOMING
        node.relationship( type, node );      // LOOP
        otherNode.store();
        node.storeAndPlaceNodeCursorAt();

        MutableMultimap<Long,Long> createdRels = Multimaps.mutable.set.empty();
        createdRels.put( node.id(), node.id() );
        createdRels.put( node.id(), otherNode.id() );
        createdRels.put( otherNode.id(), node.id() );

        MutableMultimap<Long,Long> actualRels = Multimaps.mutable.set.empty();
        try ( FrekiRelationshipScanCursor relationshipCursor = cursorFactory.allocateRelationshipScanCursor( NULL ) )
        {
            relationshipCursor.scan();
            while ( relationshipCursor.next() )
            {
                actualRels.put( relationshipCursor.sourceNodeReference(), relationshipCursor.targetNodeReference() );
            }
        }

        assertEquals( createdRels, actualRels );
    }

    @Test
    void shouldFindNoRelationshipsInScan()
    {
        // given
        for ( int i = 0; i < 20; i++ )
        {
            node().store();
        }

        try ( FrekiRelationshipScanCursor relationshipCursor = cursorFactory.allocateRelationshipScanCursor( NULL ) )
        {
            relationshipCursor.scan();
            assertFalse( relationshipCursor.next() );
        }
    }

    @Test
    void shouldFindAllRelationshipsOfTypeInScan()
    {
        // given
        int type1 = 3;
        int type2 = 4;
        int type3 = 4;
        Node node = node();
        Node otherNode = node();
        node.relationship( type1, otherNode ); // OUTGOING
        otherNode.relationship( type2, node ); // INCOMING
        node.relationship( type2, node );      // LOOP
        node.relationship( type3, node );      // LOOP
        otherNode.store();
        node.storeAndPlaceNodeCursorAt();

        MutableMultimap<Long,Long> createdRels = Multimaps.mutable.set.empty();
        createdRels.put( node.id(), node.id() );
        createdRels.put( otherNode.id(), node.id() );

        MutableMultimap<Long,Long> actualRels = Multimaps.mutable.set.empty();
        try ( FrekiRelationshipScanCursor relationshipCursor = cursorFactory.allocateRelationshipScanCursor( NULL ) )
        {
            relationshipCursor.scan( type2 );
            while ( relationshipCursor.next() )
            {
                actualRels.put( relationshipCursor.sourceNodeReference(), relationshipCursor.targetNodeReference() );
            }
        }

        assertEquals( createdRels, actualRels );
    }

    @Test
    void shouldFindSingleRelationship()
    {
        createSomeNodesAndRelationships();
        Node node = node();
        Node otherNode = node();
        int type1 = 1;
        int type2 = 2;
        for ( int i = 0; i < 3; i++ )
        {
            otherNode.relationship( type1, node );
            node.relationship( type1, otherNode );
        }
        long relId = node.relationshipAndReturnItsId( type2, otherNode );
        node.store();
        otherNode.store();

        try ( FrekiRelationshipScanCursor relationshipCursor = cursorFactory.allocateRelationshipScanCursor( NULL ) )
        {
            relationshipCursor.single( relId );
            assertTrue( relationshipCursor.next() );
            assertEquals( node.id(), relationshipCursor.sourceNodeReference() );
            assertEquals( otherNode.id(), relationshipCursor.targetNodeReference() );
            assertEquals( type2, relationshipCursor.type() );
        }
    }

    @Test
    void shouldNotFindSingleRelationship()
    {
        createSomeNodesAndRelationships();
        Node node = node();
        Node otherNode = node();
        int type = 1;
        long relId = node.relationshipAndReturnItsId( type, otherNode );
        // dont store the nodes (so relationship doesnt exist)

        try ( FrekiRelationshipScanCursor relationshipCursor = cursorFactory.allocateRelationshipScanCursor( NULL ) )
        {
            relationshipCursor.single( relId );
            assertFalse( relationshipCursor.next() );
        }
    }

    @Test
    void shouldSelectRelationships()
    {
        // given
        Set<Triple<Long,Integer,Long>> relationships = new HashSet<>();
        int numRelationshipTypes = 4;
        FrekiNodeCursor node = createNodeWithRandomRelationships( numRelationshipTypes, 1, 5, relationships );

        // when
        try ( FrekiRelationshipTraversalCursor relationshipCursor = cursorFactory.allocateRelationshipTraversalCursor( NULL ) )
        {
            for ( int type = 0; type < numRelationshipTypes; type++ )
            {
                assertRelationshipSelection( node, relationshipCursor, relationships, selection( type, Direction.BOTH ) );
                assertRelationshipSelection( node, relationshipCursor, relationships, selection( type, Direction.OUTGOING ) );
                assertRelationshipSelection( node, relationshipCursor, relationships, selection( type, Direction.INCOMING ) );
                assertRelationshipSelection( node, relationshipCursor, relationships, selection( typesUpTo( type ), Direction.BOTH ) );
                assertRelationshipSelection( node, relationshipCursor, relationships, selection( typesUpTo( type ), Direction.OUTGOING ) );
                assertRelationshipSelection( node, relationshipCursor, relationships, selection( typesUpTo( type ), Direction.INCOMING ) );
                assertRelationshipSelection( node, relationshipCursor, relationships, selection( Direction.BOTH ) );
                assertRelationshipSelection( node, relationshipCursor, relationships, selection( Direction.OUTGOING ) );
                assertRelationshipSelection( node, relationshipCursor, relationships, selection( Direction.INCOMING ) );
            }
        }
    }

    private static int[] typesUpTo( int type )
    {
        int[] types = new int[type + 1];
        for ( int i = 0; i <= type; i++ )
        {
            types[i] = i;
        }
        return types;
    }

    private void assertRelationshipSelection( FrekiNodeCursor node, FrekiRelationshipTraversalCursor relationshipCursor,
            Set<Triple<Long,Integer,Long>> relationships, RelationshipSelection selection )
    {
        Set<Triple<Long,Integer,Long>> expectedRelationships = new HashSet<>();
        relationships.stream().filter( r -> selection.test( r.getMiddle(), directionOf( node.entityReference(), r.getLeft(), r.getRight() ) ) )
                .forEach( expectedRelationships::add );
        node.relationships( relationshipCursor, selection );
        while ( relationshipCursor.next() )
        {
            Triple<Long,Integer,Long> foundRelationship =
                    Triple.of( relationshipCursor.sourceNodeReference(), relationshipCursor.type(), relationshipCursor.targetNodeReference() );
            assertThat( expectedRelationships.remove( foundRelationship ) ).isTrue();
        }
        assertThat( expectedRelationships.isEmpty() ).isTrue();
    }

    private static RelationshipDirection directionOf( long nodeId, long startNodeId, long endNodeId )
    {
        return nodeId == startNodeId ? nodeId == endNodeId ? LOOP : OUTGOING : INCOMING;
    }

    private FrekiNodeCursor createNodeWithRandomRelationships( int numRelationshipTypes, int minNumRelationshipsPerType, int maxNumRelationshipsPerType,
            Set<Triple<Long,Integer,Long>> relationships )
    {
        Node node = node();
        for ( int type = 0; type < numRelationshipTypes; type++ )
        {
            int numRelationshipsOfThisType = random.nextInt( minNumRelationshipsPerType, maxNumRelationshipsPerType );
            for ( int i = 0; i < numRelationshipsOfThisType; i++ )
            {
                Node otherNode = node();
                boolean outgoing = random.nextBoolean();
                Node startNode = outgoing ? node : otherNode;
                Node endNode = outgoing ? otherNode : node;
                startNode.relationship( type, endNode );
                otherNode.store();
                relationships.add( Triple.of( startNode.id(), type, endNode.id() ) );
            }
        }
        return node.storeAndPlaceNodeCursorAt();
    }

    // by types - sparse/dense
    // by type and direction - sparse/dense
    // by direction - sparse/dense
    // by types and direction - sparse/dense

    void createSomeNodesAndRelationships()
    {
        Node[] nodes = new Node[random.nextInt( 20 ) + 20];
        for ( int i = 0; i < nodes.length; i++ )
        {
            nodes[i] = node();
        }

        for ( Node node : nodes )
        {
            int numRels = random.nextInt( 3 );
            for ( int i = 0; i < numRels; i++ )
            {
                int type = random.nextInt( 3 ) + 1;
                node.relationship( type, nodes[random.nextInt( nodes.length )] );
            }
        }

        for ( Node node : nodes )
        {
            node.store();
        }
    }

    @TestFactory
    Collection<DynamicTest> shouldSeeAllLabelPropertiesRelationshipPermutations()
    {
        List<DynamicTest> tests = new ArrayList<>();
        for ( int numLabels = 0; numLabels < 3; numLabels++ )
        {
            for ( int numProperties = 0; numProperties < 3; numProperties++ )
            {
                for ( int numRelationsType1 = 0; numRelationsType1 < 3; numRelationsType1++ )
                {
                    for ( int numRelsType1WithProps = 0; numRelsType1WithProps <= numRelationsType1; numRelsType1WithProps++ )
                    {
                        for ( int numRelsType2 = 0; numRelsType2 < 3; numRelsType2++ )
                        {
                            for ( int numRelsType2WithProps = 0; numRelsType2WithProps <= numRelsType2; numRelsType2WithProps++ )
                            {
                                Executable test = createPermutationTest(
                                        numLabels,
                                        numProperties,
                                        numRelationsType1,
                                        numRelsType1WithProps,
                                        numRelsType2,
                                        numRelsType2WithProps
                                );
                                tests.add( DynamicTest.dynamicTest( test.toString(), test ) );
                            }
                        }
                    }
                }
            }
        }
        return tests;
    }

    private Executable createPermutationTest(
            int numLabels,
            int numProperties,
            int numRelationsType1,
            int numRelationsType1WithProperties,
            int numRelationsType2,
            int numRelationsType2WithProperties
    )
    {
        return new Executable()
        {
            @Override
            public String toString()
            {
                return format( "Test Node with %d labels, %d properties, %d relationships(type1)(withProps:%s), %d relationships(type2)(withProps:%s)",
                        numLabels, numProperties, numRelationsType1, numRelationsType1WithProperties, numRelationsType2, numRelationsType2WithProperties );
            }

            private static final int REL_TYPE_1 = 2;
            private static final int REL_TYPE_2 = 3;

            private Node node;
            private int[] labels;
            private MutableIntObjectMap<Value> properties;
            private MutableLongSet relationships;
            private MutableIntObjectMap<MutableIntObjectMap<Value>> relationshipProperties;
            private int createdNodeIdCounter;

            private void init()
            {
                createdNodeIdCounter = 0;
                node = new Node( createdNodeIdCounter++ );
                labels = new int[numLabels];
                relationships = LongSets.mutable.empty();
                properties = IntObjectMaps.mutable.empty();
                relationshipProperties = IntObjectMaps.mutable.empty();
            }

            @Override
            public void execute()
            {
                init();

                //Create stuff
                createLabels();
                createNodeProperties();
                createRelationships( REL_TYPE_1, numRelationsType1, numRelationsType1WithProperties, true );
                createRelationships( REL_TYPE_2, numRelationsType2, numRelationsType2WithProperties, false );

                //Store it
                FrekiNodeCursor cursor = node.storeAndPlaceNodeCursorAt();

                //Check it
                checkLabels( cursor );
                checkProperties( cursor );
                checkRelationships( cursor );
            }

            private void createLabels()
            {
                for ( int i = 0; i < numLabels; i++ )
                {
                    labels[i] = i;
                }
                node.labels( labels );
            }

            private void checkLabels( FrekiNodeCursor cursor )
            {
                assertArrayEquals( toLongArray( labels ), cursor.labels() );
            }

            private void createNodeProperties()
            {
                for ( int i = 0; i < numProperties; i++ )
                {
                    properties.put( i + 10, intValue( i + 100 ) );
                }
                node.properties( properties );
            }

            private void checkProperties( FrekiNodeCursor cursor )
            {
                FrekiPropertyCursor propertyCursor = cursorFactory.allocatePropertyCursor( NULL );
                EntityAndPropertyConnector.DIRECT.connect( cursor, propertyCursor );
                MutableIntObjectMap<Value> readProperties = IntObjectMaps.mutable.empty();
                while ( propertyCursor.next() )
                {
                    readProperties.put( propertyCursor.propertyKey(), propertyCursor.propertyValue() );
                }

                assertEquals( numProperties, readProperties.size() );
                assertEquals( properties, readProperties );
            }

            private void createRelationships( int type, int num, int numWithProps, boolean outgoing )
            {
                for ( int i = 0; i < num; i++ )
                {
                    Node otherNode = new Node( createdNodeIdCounter++ );
                    Node sourceNode = outgoing ? node : otherNode;
                    Node targetNode = outgoing ? otherNode : node;

                    MutableIntObjectMap<Value> relProps = IntObjectMaps.mutable.empty();
                    if ( i < numWithProps )
                    {
                        for ( int j = 0; j < numWithProps; j++ )
                        {
                            relProps.put( i + type * 10, intValue( i + type * 10 ) );
                        }
                        relationshipProperties.put( makeupRelId( sourceNode.id(), targetNode.id() ) , relProps );
                    }

                    sourceNode.relationship( type, targetNode, relProps );
                    otherNode.store();

                    relationships.add( otherNode.id() );
                }
            }

            private void checkRelationships( FrekiNodeCursor cursor )
            {
                checkRelationships( cursor, REL_TYPE_1, numRelationsType1, numRelationsType1WithProperties, true );
                checkRelationships( cursor, REL_TYPE_2, numRelationsType2, numRelationsType2WithProperties, false );

                assertEquals( 0, relationships.size() );
                assertEquals( 0, relationshipProperties.size() );
            }

            private void checkRelationships( FrekiNodeCursor cursor, int typeToCheck, int expectedNum, int expectedNumWithProps, boolean expectedOutgoing )
            {
                FrekiRelationshipTraversalCursor relationshipCursor = cursorFactory.allocateRelationshipTraversalCursor( NULL );
                AllRelationshipsConnector.DIRECT_REFERENCE.connect( cursor, relationshipCursor, ALL_RELATIONSHIPS );
                FrekiPropertyCursor propertyCursor = cursorFactory.allocatePropertyCursor( NULL );

                int numRelationsRead = 0;
                int numRelationsWithPropsRead = 0;
                while ( relationshipCursor.next() )
                {

                    int type = relationshipCursor.type();
                    if ( type == typeToCheck )
                    {
                        numRelationsRead++;
                        assertEquals( expectedOutgoing, relationshipCursor.sourceNodeReference() == node.id() );
                        if ( relationshipCursor.hasProperties() )
                        {
                            EntityAndPropertyConnector.DIRECT.connect( relationshipCursor, propertyCursor );
                            numRelationsWithPropsRead++;

                            MutableIntObjectMap<Value> readRelProps = IntObjectMaps.mutable.empty();
                            while ( propertyCursor.next() )
                            {
                                readRelProps.put( propertyCursor.propertyKey(), propertyCursor.propertyValue() );
                            }

                            int makeupId = makeupRelId( relationshipCursor.sourceNodeReference(), relationshipCursor.targetNodeReference() );
                            MutableIntObjectMap<Value> relProps = relationshipProperties.remove( makeupId );
                            assertNotNull( relProps );
                            assertEquals( relProps, readRelProps );
                        }
                        assertTrue( relationships.remove( relationshipCursor.neighbourNodeReference() ) );
                    }
                }

                assertEquals( numRelationsRead, expectedNum );
                assertEquals( numRelationsWithPropsRead, expectedNumWithProps );
            }

            private int makeupRelId( long from, long to )
            {
                return (int) ( from << 16 | to );
            }
        };
    }

    // TODO other randomized testing

    private MutableIntObjectMap<Value> readProperties( FrekiPropertyCursor propertyCursor )
    {
        MutableIntObjectMap<Value> readProperties = IntObjectMaps.mutable.empty();
        while ( propertyCursor.next() )
        {
            readProperties.put( propertyCursor.propertyKey(), propertyCursor.propertyValue() );
        }
        return readProperties;
    }

    static Iterable<Arguments> allRelationshipsAndPropertyConnector()
    {
        Collection<Arguments> permutations = new ArrayList<>();
        for ( AllRelationshipsConnector relationshipsConnector : AllRelationshipsConnector.values() )
        {
            for ( EntityAndPropertyConnector propertyConnector : EntityAndPropertyConnector.values() )
            {
                permutations.add( Arguments.arguments( relationshipsConnector, propertyConnector ) );
            }
        }
        return permutations;
    }
}
