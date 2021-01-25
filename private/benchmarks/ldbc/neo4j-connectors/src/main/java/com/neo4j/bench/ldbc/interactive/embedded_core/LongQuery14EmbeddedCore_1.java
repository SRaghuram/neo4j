/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.ldbc.interactive.embedded_core;

import com.ldbc.driver.DbException;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery14;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery14Result;
import com.neo4j.bench.ldbc.Domain.Knows;
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

import org.neo4j.graphalgo.BasicEvaluationContext;
import org.neo4j.graphalgo.GraphAlgoFactory;
import org.neo4j.graphalgo.PathFinder;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Entity;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.PathExpanders;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;

public class LongQuery14EmbeddedCore_1 extends Neo4jQuery14<Neo4jConnectionState>
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
        Node person1 = Operators.findNode( connection.getTx(), Nodes.Person, Person.ID, operation.person1Id() );
        Node person2 = Operators.findNode( connection.getTx(), Nodes.Person, Person.ID, operation.person2Id() );
        var context = new BasicEvaluationContext( connection.getTx(), connection.getDb() );
        PathFinder<Path> finder = GraphAlgoFactory.shortestPath( context,
                PathExpanders.forTypeAndDirection( Rels.KNOWS, Direction.BOTH ),
                Integer.MAX_VALUE );
        List<LdbcQuery14Result> results = new ArrayList<>();
        for ( Path path : finder.findAllPaths( person1, person2 ) )
        {
            List<Long> personIdsInPath = new ArrayList<>();
            double pathWeight = calculatePathWeight(
                    path,
                    personIdsInPath,
                    POST_HAS_CREATOR_RELATIONSHIP_TYPES,
                    COMMENT_HAS_CREATOR_RELATIONSHIP_TYPES );
            results.add(
                    new LdbcQuery14Result( personIdsInPath, pathWeight )
            );
        }
        Collections.sort( results, DESCENDING_PATH_WEIGHT );
        return results;
    }

    private double calculatePathWeight(
            Iterable<Entity> path,
            List<Long> personIdsInPath,
            RelationshipType[] postHasCreatorRelationshipTypes,
            RelationshipType[] commentHasCreatorRelationshipTypes )
    {
        double weight = 0;
        Map<Node,Set<Node>> personComments = new HashMap<>();
        Map<Node,Set<Node>> personPosts = new HashMap<>();
        Iterator<Entity> pathIterator = path.iterator();
        Node prevPerson;
        Node currPerson = (Node) pathIterator.next();
        personIdsInPath.add( (long) currPerson.getProperty( Person.ID ) );
        while ( pathIterator.hasNext() )
        {
            Relationship knows = (Relationship) pathIterator.next();
            prevPerson = currPerson;
            currPerson = (Node) pathIterator.next();
            personIdsInPath.add( (long) currPerson.getProperty( Person.ID ) );
            Double knowsWeight = (Double) knows.getProperty( Knows.WEIGHT, null );
            if ( null == knowsWeight )
            {
                knowsWeight = Neo4jQuery14.conversationWeightBetweenPersons(
                        currPerson,
                        prevPerson,
                        personComments,
                        personPosts,
                        postHasCreatorRelationshipTypes,
                        commentHasCreatorRelationshipTypes );
                knows.setProperty( Knows.WEIGHT, knowsWeight );
            }
            weight += knowsWeight;
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
}
