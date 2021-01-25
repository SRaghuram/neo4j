/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.ldbc.interactive.embedded_core;

import com.google.common.collect.MinMaxPriorityQueue;
import com.ldbc.driver.DbException;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery10;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery10Result;
import com.neo4j.bench.ldbc.Domain.Nodes;
import com.neo4j.bench.ldbc.Domain.Person;
import com.neo4j.bench.ldbc.Domain.Place;
import com.neo4j.bench.ldbc.Domain.Rels;
import com.neo4j.bench.ldbc.connection.Neo4jConnectionState;
import com.neo4j.bench.ldbc.connection.QueryDateUtil;
import com.neo4j.bench.ldbc.interactive.Neo4jQuery10;
import com.neo4j.bench.ldbc.operators.Operators;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.ResourceIterator;

public class LongQuery10EmbeddedCore_0_1 extends Neo4jQuery10<Neo4jConnectionState>
{
    private static final DescendingSimilarityScoreAscendingPersonId DESCENDING_SIMILARITY_SCORE_ASCENDING_PERSON_ID =
            new DescendingSimilarityScoreAscendingPersonId();

    @Override
    public List<LdbcQuery10Result> execute( Neo4jConnectionState connection, final LdbcQuery10 operation )
            throws DbException
    {
        QueryDateUtil dateUtil = connection.dateUtil();
        Node person = Operators.findNode( connection.getTx(), Nodes.Person, Person.ID, operation.personId() );

        Set<Node> immediateFriends = new HashSet<>();

        for ( Relationship knows : person.getRelationships( Rels.KNOWS ) )
        {
            Node friend = knows.getOtherNode( person );
            immediateFriends.add( friend );
        }

        Set<Node> friendsOfFriends = new HashSet<>();
        int thisMonth = operation.month();
        int nextMonth = (operation.month() % 12) + 1;

        for ( Node immediateFriend : immediateFriends )
        {
            for ( Relationship knows : immediateFriend.getRelationships( Rels.KNOWS ) )
            {
                Node friendOfFriend = knows.getOtherNode( immediateFriend );
                if ( friendOfFriend.equals( person ) )
                {
                    continue;
                }
                if ( immediateFriends.contains( friendOfFriend ) )
                {
                    continue;
                }
                long birthday = (long) friendOfFriend.getProperty( Person.BIRTHDAY );
                int birthdayMonth = dateUtil.formatToMonth( birthday );
                if ( thisMonth == birthdayMonth )
                {
                    int birthdayDayOfMonth = dateUtil.formatToDay( birthday );
                    if ( birthdayDayOfMonth >= 21 )
                    {
                        friendsOfFriends.add( friendOfFriend );
                    }
                }
                else if ( nextMonth == birthdayMonth )
                {
                    int birthdayDayOfMonth = dateUtil.formatToDay( birthday );
                    if ( birthdayDayOfMonth < 22 )
                    {
                        friendsOfFriends.add( friendOfFriend );
                    }
                }
            }
        }

        final Set<Node> tagsPersonIsInterestedIn = new HashSet<>();
        for ( Relationship relationship : person.getRelationships( Direction.OUTGOING, Rels.HAS_INTEREST ) )
        {
            Node tag = relationship.getEndNode();
            tagsPersonIsInterestedIn.add( tag );
        }

        MinMaxPriorityQueue<LdbcQuery10PreResult> preResults = MinMaxPriorityQueue
                .orderedBy( DESCENDING_SIMILARITY_SCORE_ASCENDING_PERSON_ID )
                .maximumSize( operation.limit() )
                .create();

        for ( Node friend : friendsOfFriends )
        {
            int commonInterestScore = 0;
            for ( Relationship hasCreator : friend.getRelationships( Direction.INCOMING, Rels.POST_HAS_CREATOR ) )
            {
                Node post = hasCreator.getStartNode();
                if ( postIsTaggedWithAnyOfGivenTags( post, tagsPersonIsInterestedIn ) )
                {
                    commonInterestScore++;
                }
                else
                {
                    commonInterestScore--;
                }
            }
            preResults.add(
                    new LdbcQuery10PreResult( friend, commonInterestScore )
            );
        }

        List<LdbcQuery10Result> results = new ArrayList<>();
        LdbcQuery10PreResult preResult;
        while ( null != (preResult = preResults.poll()) )
        {
            Map<String,Object> personProperties = preResult.personProperties();
            results.add(
                    new LdbcQuery10Result(
                            preResult.id(),
                            (String) personProperties.get( Person.FIRST_NAME ),
                            (String) personProperties.get( Person.LAST_NAME ),
                            preResult.commonInterestScore(),
                            (String) personProperties.get( Person.GENDER ),
                            preResult.cityName()
                    )
            );
        }
        return results;
    }

    private boolean postIsTaggedWithAnyOfGivenTags( Node post, Set<Node> tags )
    {
        try ( ResourceIterator<Relationship> relationships =
                      (ResourceIterator<Relationship>) post.getRelationships( Direction.OUTGOING, Rels.POST_HAS_TAG ).iterator() )
        {
            while ( relationships.hasNext() )
            {
                Relationship relationship = relationships.next();
                Node postTag = relationship.getEndNode();
                if ( tags.contains( postTag ) )
                {
                    return true;
                }
            }
        }
        return false;
    }

    private static class DescendingSimilarityScoreAscendingPersonId implements Comparator<LdbcQuery10PreResult>
    {
        @Override
        public int compare( LdbcQuery10PreResult result1, LdbcQuery10PreResult result2 )
        {
            if ( result1.commonInterestScore() > result2.commonInterestScore() )
            {
                return -1;
            }
            else if ( result1.commonInterestScore() < result2.commonInterestScore() )
            {
                return 1;
            }
            else
            {
                if ( result1.id() < result2.id() )
                {
                    return -1;
                }
                else if ( result1.id() > result2.id() )
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

    private static class LdbcQuery10PreResult
    {
        private final Node person;
        private final int commonInterestScore;
        private long id = -1;
        private String cityName;

        private LdbcQuery10PreResult( Node person, int commonInterestScore )
        {
            this.person = person;
            this.commonInterestScore = commonInterestScore;
        }

        private int commonInterestScore()
        {
            return commonInterestScore;
        }

        private long id()
        {
            if ( -1 == id )
            {
                id = (long) person.getProperty( Person.ID );
            }
            return id;
        }

        private Map<String,Object> personProperties()
        {
            return person.getProperties(
                    Person.FIRST_NAME,
                    Person.LAST_NAME,
                    Person.GENDER );
        }

        private String cityName()
        {
            if ( null == cityName )
            {
                cityName = (String) person.getSingleRelationship( Rels.PERSON_IS_LOCATED_IN, Direction.OUTGOING )
                        .getEndNode().getProperty( Place.NAME );
            }
            return cityName;
        }
    }
}
