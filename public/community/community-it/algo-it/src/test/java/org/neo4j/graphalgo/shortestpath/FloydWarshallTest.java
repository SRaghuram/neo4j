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
package org.neo4j.graphalgo.shortestpath;

import common.Neo4jAlgoTestCase;
import org.junit.jupiter.api.Test;

import java.util.List;

import org.neo4j.graphalgo.CommonEvaluators;
import org.neo4j.graphalgo.impl.shortestpath.FloydWarshall;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Node;

import static org.junit.jupiter.api.Assertions.assertEquals;

class FloydWarshallTest extends Neo4jAlgoTestCase
{
    /**
     * Test case for paths of length 0 and 1, and an impossible path
     */
    @Test
    void testMinimal()
    {
        graph.makeEdge( "a", "b", "cost", (double) 1 );
        graph.makeEdge( "a", "c", "cost", (float) 1 );
        graph.makeEdge( "a", "d", "cost", (long) 1 );
        graph.makeEdge( "a", "e", "cost", 1 );
        graph.makeEdge( "b", "c", "cost", (double) 1 );
        graph.makeEdge( "c", "d", "cost", (byte) 1 );
        graph.makeEdge( "d", "e", "cost", (short) 1 );
        graph.makeEdge( "e", "b", "cost", (byte) 1 );
        FloydWarshall<Double> floydWarshall = new FloydWarshall<>( 0.0, Double.MAX_VALUE, Direction.OUTGOING,
                CommonEvaluators.doubleCostEvaluator( "cost" ), new org.neo4j.graphalgo.impl.util.DoubleAdder(),
                Double::compareTo, graph.getAllNodes(), graph.getAllEdges() );
        assertEquals( 0.0, floydWarshall.getCost( graph.getNode( "a" ), graph.getNode( "a" ) ), 0.0 );
        assertEquals( 1.0, floydWarshall.getCost( graph.getNode( "a" ), graph.getNode( "b" ) ), 0.0 );
        assertEquals( floydWarshall.getCost( graph.getNode( "b" ), graph.getNode( "a" ) ), Double.MAX_VALUE, 0.0 );
    }

    /**
     * Test case for extracting paths
     */
    @Test
    void testPath()
    {
        graph.makeEdge( "a", "b", "cost", (double) 1 );
        graph.makeEdge( "b", "c", "cost", (float) 1 );
        graph.makeEdge( "c", "d", "cost", 1 );
        graph.makeEdge( "d", "e", "cost", (long) 1 );
        graph.makeEdge( "e", "f", "cost", (byte) 1 );
        FloydWarshall<Double> floydWarshall = new FloydWarshall<>( 0.0, Double.MAX_VALUE, Direction.OUTGOING,
                CommonEvaluators.doubleCostEvaluator( "cost" ), new org.neo4j.graphalgo.impl.util.DoubleAdder(),
                Double::compareTo, graph.getAllNodes(), graph.getAllEdges() );
        List<Node> path = floydWarshall.getPath( graph.getNode( "a" ), graph
            .getNode( "f" ) );
        assertEquals( 6, path.size() );
        assertEquals( path.get( 0 ), graph.getNode( "a" ) );
        assertEquals( path.get( 1 ), graph.getNode( "b" ) );
        assertEquals( path.get( 2 ), graph.getNode( "c" ) );
        assertEquals( path.get( 3 ), graph.getNode( "d" ) );
        assertEquals( path.get( 4 ), graph.getNode( "e" ) );
        assertEquals( path.get( 5 ), graph.getNode( "f" ) );
    }

    @Test
    void testDirection()
    {
        graph.makeEdge( "a", "b" );
        graph.makeEdge( "b", "c" );
        graph.makeEdge( "c", "d" );
        graph.makeEdge( "d", "a" );
        graph.makeEdge( "s", "a" );
        graph.makeEdge( "b", "s" );
        graph.makeEdge( "e", "c" );
        graph.makeEdge( "d", "e" );
        new FloydWarshall<>( 0.0, Double.MAX_VALUE, Direction.OUTGOING, ( relationship, direction ) -> {
            assertEquals( Direction.OUTGOING, direction );
            return 1.0;
        }, new org.neo4j.graphalgo.impl.util.DoubleAdder(), Double::compareTo,
                graph.getAllNodes(), graph.getAllEdges() ).calculate();
        new FloydWarshall<>( 0.0, Double.MAX_VALUE, Direction.INCOMING, ( relationship, direction ) -> {
            assertEquals( Direction.INCOMING, direction );
            return 1.0;
        }, new org.neo4j.graphalgo.impl.util.DoubleAdder(), Double::compareTo,
                graph.getAllNodes(), graph.getAllEdges() ).calculate();
    }
}
