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

import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import org.neo4j.graphdb.Direction;
import org.neo4j.storageengine.util.EagerDegrees;
import org.neo4j.storageengine.util.SingleDegree;
import org.neo4j.test.rule.RandomRule;

import static java.util.stream.Collectors.toSet;
import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer.NULL;
import static org.neo4j.storageengine.api.RelationshipSelection.ALL_RELATIONSHIPS;
import static org.neo4j.storageengine.api.RelationshipSelection.selection;
import static org.neo4j.values.storable.Values.stringValue;

class FrekiNodeCursorDenseTest extends FrekiCursorsTest
{
    @RandomRule.Seed( 1589789400946L )
    @Test
    void shouldGetDegreesFromDenseNodeWithHighIds()
    {
        // given
        store.setHighId( random.nextLong( Integer.MAX_VALUE ) );
        largeStore.setHighId( random.nextLong( Integer.MAX_VALUE ) );
        var node = node().labels( 1, 2, 3 ).property( 0, stringValue( "Dude" ) );
        var nodeId = node.id();
        node.store();
        var numTypes = 25;
        var expectedDegrees = new EagerDegrees();
        Set<RelationshipSpec> expectedRelationships = new HashSet<>();

        // when
        for ( var i = 0; i < 100; i++ )
        {
            node = existingNode( nodeId );
            int count = random.nextInt( 1, 100 );
            for ( int j = 0; j < count; j++ )
            {
                int type = random.nextInt( numTypes );
                var otherNode = node();
                Node startNode;
                Node endNode;
                float dice = random.nextFloat();
                if ( dice < 0.01 )
                {
                    startNode = endNode = node;
                    expectedDegrees.add( type, 0, 0, 1 );
                }
                else if ( dice < 0.5 )
                {
                    startNode = node;
                    endNode = otherNode;
                    expectedDegrees.add( type, 1, 0, 0 );
                }
                else
                {
                    startNode = otherNode;
                    endNode = node;
                    expectedDegrees.add( type, 0, 1, 0 );
                }
                long relationshipId = startNode.relationshipAndReturnItsId( type, endNode );
                otherNode.store();
                RelationshipSpec relationship = new RelationshipSpec( startNode.id(), type, endNode.id(), Collections.emptySet(), relationshipId );
                expectedRelationships.add( relationship );
            }
            var nodeCursor = node.storeAndPlaceNodeCursorAt();

            // then
            assertThat( nodeCursor.labels() ).isEqualTo( new long[]{1, 2, 3} );
            var degrees = new EagerDegrees();
            nodeCursor.degrees( ALL_RELATIONSHIPS, degrees, true );
            assertThat( degrees ).isEqualTo( expectedDegrees );
            var totalDegree = new SingleDegree();
            nodeCursor.degrees( ALL_RELATIONSHIPS, totalDegree, true );
            assertThat( totalDegree.getTotal() ).isEqualTo( expectedDegrees.totalDegree() );

            var relationshipCursor = cursorFactory.allocateRelationshipTraversalCursor( NULL );
            for ( int t = 0; t < numTypes; t++ )
            {
                int type = t;
                Set<RelationshipSpec> expectedRelationshipsOfType = expectedRelationships.stream().filter( r -> r.type == type ).collect( toSet() );
                nodeCursor.relationships( relationshipCursor, selection( t, Direction.BOTH ) );
                while ( relationshipCursor.next() )
                {
                    var readRelationship =
                            new RelationshipSpec( relationshipCursor.sourceNodeReference(), relationshipCursor.type(), relationshipCursor.targetNodeReference(),
                                    Collections.emptySet(), relationshipCursor.entityReference() );
                    assertThat( expectedRelationshipsOfType.remove( readRelationship ) ).isTrue();
                }
                assertThat( expectedRelationshipsOfType.isEmpty() ).isTrue();
            }
        }
    }
}
