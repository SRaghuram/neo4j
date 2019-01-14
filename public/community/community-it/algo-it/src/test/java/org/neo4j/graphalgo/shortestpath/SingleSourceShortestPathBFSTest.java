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

import org.neo4j.graphalgo.impl.shortestpath.SingleSourceShortestPath;
import org.neo4j.graphalgo.impl.shortestpath.SingleSourceShortestPathBFS;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.RelationshipType;

public class SingleSourceShortestPathBFSTest extends
    SingleSourceShortestPathTest
{
    @Override
    protected SingleSourceShortestPath<Integer> getSingleSourceAlgorithm(
        Node startNode )
    {
        SingleSourceShortestPathBFS sourceBFS = new SingleSourceShortestPathBFS(
            startNode, Direction.BOTH, MyRelTypes.R1 );
        return sourceBFS;
    }

    @Override
    protected SingleSourceShortestPath<Integer> getSingleSourceAlgorithm(
        Node startNode, Direction direction, RelationshipType... relTypes )
    {
        return new SingleSourceShortestPathBFS( startNode, direction, relTypes );
    }
}
