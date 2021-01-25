/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.ldbc.interactive.embedded_core;

import com.google.common.collect.MinMaxPriorityQueue;
import com.ldbc.driver.DbException;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery3;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery3Result;
import com.neo4j.bench.ldbc.Domain.Message;
import com.neo4j.bench.ldbc.Domain.Nodes;
import com.neo4j.bench.ldbc.Domain.Person;
import com.neo4j.bench.ldbc.Domain.Place;
import com.neo4j.bench.ldbc.Domain.Rels;
import com.neo4j.bench.ldbc.connection.Neo4jConnectionState;
import com.neo4j.bench.ldbc.connection.QueryDateUtil;
import com.neo4j.bench.ldbc.interactive.Neo4jQuery3;
import com.neo4j.bench.ldbc.operators.Operators;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;

public class LongQuery3EmbeddedCore_0 extends Neo4jQuery3<Neo4jConnectionState>
{
    private static final DescendingCountAscendingPersonIdComparator DESCENDING_COUNT_ASCENDING_PERSON_ID_COMPARATOR =
            new DescendingCountAscendingPersonIdComparator();

    @Override
    public List<LdbcQuery3Result> execute( final Neo4jConnectionState connection, final LdbcQuery3 operation )
            throws DbException
    {
        QueryDateUtil dateUtil = connection.dateUtil();
        Node person = Operators.findNode( connection.getTx(), Nodes.Person, Person.ID, operation.personId() );
        Node countryX = Operators.findNode( connection.getTx(), Place.Type.Country, Place.NAME, operation.countryXName() );
        Node countryY = Operators.findNode( connection.getTx(), Place.Type.Country, Place.NAME, operation.countryYName() );

        CityCountryCache cityCountryCache = new CityCountryCache( countryX, countryY );

        Set<Node> friends = new HashSet<>();
        for ( Relationship knows : person.getRelationships( Rels.KNOWS ) )
        {
            Node friend = knows.getOtherNode( person );
            Node friendsCity =
                    friend.getSingleRelationship( Rels.PERSON_IS_LOCATED_IN, Direction.OUTGOING ).getEndNode();
            if ( !cityCountryCache.isCityInCountries( friendsCity ) )
            {
                friends.add( friend );
            }
            for ( Relationship knowsKnows : friend.getRelationships( Rels.KNOWS ) )
            {
                Node friendsFriend = knowsKnows.getOtherNode( friend );
                if ( friendsFriend.equals( person ) )
                {
                    continue;
                }
                if ( friends.contains( friendsFriend ) )
                {
                    continue;
                }
                Node friendsFriendsCity =
                        friendsFriend.getSingleRelationship( Rels.PERSON_IS_LOCATED_IN, Direction.OUTGOING )
                                .getEndNode();
                if ( !cityCountryCache.isCityInCountries( friendsFriendsCity ) )
                {
                    friends.add( friendsFriend );
                }
            }
        }

        MinMaxPriorityQueue<LdbcQuery3PreResult> preResults = MinMaxPriorityQueue
                .orderedBy( DESCENDING_COUNT_ASCENDING_PERSON_ID_COMPARATOR )
                .maximumSize( operation.limit() )
                .create();

        long minDate = dateUtil.utcToFormat(
                operation.startDate().getTime() );
        long maxDate = dateUtil.utcToFormat(
                operation.startDate().getTime() + TimeUnit.DAYS.toMillis( operation.durationDays() ) );
        for ( Node friend : friends )
        {
            int[] counts = messageCountInCountryBetweenDates( friend, countryX, countryY, minDate, maxDate );
            int countryXCount = counts[0];
            int countryYCount = counts[1];
            if ( 0 == countryXCount || 0 == countryYCount )
            {
                continue;
            }
            preResults.add(
                    new LdbcQuery3PreResult( countryXCount, countryYCount, friend )
            );
        }

        List<LdbcQuery3Result> results = new ArrayList<>();
        LdbcQuery3PreResult preResult;
        while ( null != (preResult = preResults.poll()) )
        {
            results.add(
                    new LdbcQuery3Result(
                            preResult.id(),
                            preResult.firstName(),
                            preResult.lastName(),
                            preResult.xCount(),
                            preResult.yCount(),
                            preResult.count()
                    )
            );
        }

        return results;
    }

    private int[] messageCountInCountryBetweenDates(
            Node person,
            Node countryX,
            Node countryY,
            long minDate,
            long maxDate )
    {
        int countX = 0;
        int countY = 0;
        for ( Relationship hasCreator : person.getRelationships(
                Direction.INCOMING,
                Rels.COMMENT_HAS_CREATOR,
                Rels.POST_HAS_CREATOR ) )
        {
            Node message = hasCreator.getStartNode();
            long creationDate = (long) message.getProperty( Message.CREATION_DATE );
            if ( creationDate >= minDate && maxDate > creationDate )
            {
                Node messageCountry = (hasCreator.isType( Rels.POST_HAS_CREATOR ))
                                      ? message.getSingleRelationship( Rels.POST_IS_LOCATED_IN, Direction.OUTGOING )
                                              .getEndNode()
                                      : message.getSingleRelationship( Rels.COMMENT_IS_LOCATED_IN, Direction.OUTGOING )
                                              .getEndNode();
                if ( messageCountry.equals( countryX ) )
                {
                    countX++;
                }
                else if ( messageCountry.equals( countryY ) )
                {
                    countY++;
                }
            }
        }
        return new int[]{countX, countY};
    }

    static class DescendingCountAscendingPersonIdComparator implements Comparator<LdbcQuery3PreResult>
    {
        @Override
        public int compare( LdbcQuery3PreResult result1, LdbcQuery3PreResult result2 )
        {
            if ( result1.count() > result2.count() )
            {
                return -1;
            }
            else if ( result1.count() < result2.count() )
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

    private static class LdbcQuery3PreResult
    {
        private final int xCount;
        private final int yCount;
        private final int count;
        private final Node person;
        private long id = -1;
        private String firstName;
        private String lastName;

        private LdbcQuery3PreResult( int xCount, int yCount, Node person )
        {
            this.xCount = xCount;
            this.yCount = yCount;
            this.count = xCount + yCount;
            this.person = person;
        }

        int xCount()
        {
            return xCount;
        }

        int yCount()
        {
            return yCount;
        }

        int count()
        {
            return count;
        }

        long id()
        {
            if ( -1 == id )
            {
                id = (long) person.getProperty( Person.ID );
            }
            return id;
        }

        String firstName()
        {
            if ( null == firstName )
            {
                firstName = (String) person.getProperty( Person.FIRST_NAME );
            }
            return firstName;
        }

        String lastName()
        {
            if ( null == lastName )
            {
                lastName = (String) person.getProperty( Person.LAST_NAME );
            }
            return lastName;
        }
    }

    private static class CityCountryCache
    {
        private final Map<Node,Node> cityToCountryMapping;
        private final Node countryX;
        private final Node countryY;

        private CityCountryCache( Node countryX, Node countryY )
        {
            this.cityToCountryMapping = new HashMap<>();
            this.countryX = countryX;
            this.countryY = countryY;
        }

        boolean isCityInCountries( Node city )
        {
            Node countryOfCity = cityToCountryMapping.get( city );
            if ( null == cityToCountryMapping.get( city ) )
            {
                countryOfCity = city.getSingleRelationship( Rels.IS_PART_OF, Direction.OUTGOING ).getEndNode();
                cityToCountryMapping.put( city, countryOfCity );
            }
            return countryOfCity.equals( countryX ) || countryOfCity.equals( countryY );
        }
    }
}
