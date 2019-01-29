/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
 *
 */

package com.neo4j.bench.ldbc.interactive.embedded_core;

import com.ldbc.driver.DbException;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery14;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery14Result;
import com.neo4j.bench.ldbc.Domain.Nodes;
import com.neo4j.bench.ldbc.Domain.Person;
import com.neo4j.bench.ldbc.Domain.Rels;
import com.neo4j.bench.ldbc.connection.Neo4jConnectionState;
import com.neo4j.bench.ldbc.interactive.Neo4jQuery14;
import com.neo4j.bench.ldbc.operators.Operators;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.neo4j.graphalgo.GraphAlgoFactory;
import org.neo4j.graphalgo.PathFinder;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.PathExpanders;
import org.neo4j.graphdb.RelationshipType;

public class LongQuery14EmbeddedCore_0 extends Neo4jQuery14<Neo4jConnectionState>
{
    private static final DescendingPathWeight DESCENDING_PATH_WEIGHT = new DescendingPathWeight();
    private static final RelationshipType[] POST_HAS_CREATOR_RELATIONSHIP_TYPES =
            new RelationshipType[]{Rels.POST_HAS_CREATOR};
    private static final RelationshipType[] COMMENT_HAS_CREATOR_RELATIONSHIP_TYPES =
            new RelationshipType[]{Rels.COMMENT_HAS_CREATOR};

    @Override
    public List<LdbcQuery14Result> execute( Neo4jConnectionState connection, LdbcQuery14 operation )
            throws DbException
    {
        Node person1 = Operators.findNode( connection.db(), Nodes.Person, Person.ID, operation.person1Id() );
        Node person2 = Operators.findNode( connection.db(), Nodes.Person, Person.ID, operation.person2Id() );
        PathFinder<Path> finder = GraphAlgoFactory
                .shortestPath( PathExpanders.forTypeAndDirection( Rels.KNOWS, Direction.BOTH ),
                        Integer.MAX_VALUE );
        List<LdbcQuery14Result> results = new ArrayList<>();
        NodePairWeightCache nodePairWeightCache = new NodePairWeightCache();
        for ( Path path : finder.findAllPaths( person1, person2 ) )
        {
            List<Long> personIdsInPath = new ArrayList<>();
            double pathWeight = calculatePathWeight( path, personIdsInPath, nodePairWeightCache );
            results.add(
                    new LdbcQuery14Result( personIdsInPath, pathWeight )
            );
        }
        Collections.sort( results, DESCENDING_PATH_WEIGHT );
        return results;
    }

    private double calculatePathWeight( Path path, List<Long> personIdsInPath, NodePairWeightCache nodePairWeightCache )
    {
        double weight = 0;
        Map<Node,Set<Node>> personComments = new HashMap<>();
        Map<Node,Set<Node>> personPosts = new HashMap<>();
        Iterator<Node> pathNodes = path.nodes().iterator();
        Node prevPerson;
        Node currPerson = pathNodes.next();
        personIdsInPath.add( (long) currPerson.getProperty( Person.ID ) );
        while ( pathNodes.hasNext() )
        {
            prevPerson = currPerson;
            currPerson = pathNodes.next();
            personIdsInPath.add( (long) currPerson.getProperty( Person.ID ) );
            Double nodePairWeight = nodePairWeightCache.getWeightOrNull( prevPerson, currPerson );
            if ( null == nodePairWeight )
            {
                nodePairWeight = Neo4jQuery14.conversationWeightBetweenPersons(
                        currPerson,
                        prevPerson,
                        personComments,
                        personPosts,
                        POST_HAS_CREATOR_RELATIONSHIP_TYPES,
                        COMMENT_HAS_CREATOR_RELATIONSHIP_TYPES );
                nodePairWeightCache.putWeight( prevPerson, currPerson, nodePairWeight );
                weight += nodePairWeight;
            }
            else
            {
                weight += nodePairWeight;
            }
        }
        return weight;
    }

    private static class DescendingPathWeight implements Comparator<LdbcQuery14Result>
    {
        @Override
        public int compare( LdbcQuery14Result result1, LdbcQuery14Result result2 )
        {
            if ( result1.pathWeight() > result2.pathWeight() )
            {
                return -1;
            }
            else if ( result1.pathWeight() < result2.pathWeight() )
            {
                return 1;
            }
            else
            {
                return 0;
            }
        }
    }

    private static class NodePairWeightCache
    {
        private final Map<NodePair,Double> cachedScores;

        NodePairWeightCache()
        {
            this.cachedScores = new HashMap<>();
        }

        Double getWeightOrNull( Node first, Node second )
        {
            NodePair nodePair = new NodePair( first, second );
            return cachedScores.get( nodePair );
        }

        void putWeight( Node first, Node second, double weight )
        {
            cachedScores.put( new NodePair( first, second ), weight );
            cachedScores.put( new NodePair( second, first ), weight );
        }
    }

    private static class NodePair
    {
        private final Node first;
        private final Node second;

        private NodePair( Node first, Node second )
        {
            this.first = first;
            this.second = second;
        }

        @Override
        public boolean equals( Object o )
        {
            if ( this == o )
            {
                return true;
            }
            NodePair nodePair = (NodePair) o;
            return first.equals( nodePair.first ) && second.equals( nodePair.second );
        }

        @Override
        public int hashCode()
        {
            int result = first != null ? first.hashCode() : 0;
            result = 31 * result + (second != null ? second.hashCode() : 0);
            return result;
        }
    }
}
