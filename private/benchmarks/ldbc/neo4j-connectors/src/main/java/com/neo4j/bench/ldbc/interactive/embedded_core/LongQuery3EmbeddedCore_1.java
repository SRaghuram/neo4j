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
import com.neo4j.bench.ldbc.connection.TimeStampedRelationshipTypesCache;
import com.neo4j.bench.ldbc.interactive.Neo4jQuery3;
import com.neo4j.bench.ldbc.operators.ManyToOneIsConnectedCache;
import com.neo4j.bench.ldbc.operators.Operators;
import it.unimi.dsi.fastutil.longs.Long2IntMap;
import it.unimi.dsi.fastutil.longs.Long2IntOpenHashMap;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import it.unimi.dsi.fastutil.longs.LongSet;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;

public class LongQuery3EmbeddedCore_1 extends Neo4jQuery3<Neo4jConnectionState>
{
    private static final DescendingCountAscendingPersonIdComparator DESCENDING_COUNT_ASCENDING_PERSON_ID_COMPARATOR =
            new DescendingCountAscendingPersonIdComparator();

    @Override
    public List<LdbcQuery3Result> execute( Neo4jConnectionState connection, final LdbcQuery3 operation )
            throws DbException
    {
        QueryDateUtil dateUtil = connection.dateUtil();
        Node person = Operators.findNode( connection.getTx(), Nodes.Person, Person.ID, operation.personId() );
        Node countryX = Operators.findNode( connection.getTx(), Place.Type.Country, Place.NAME, operation.countryXName() );
        Node countryY = Operators.findNode( connection.getTx(), Place.Type.Country, Place.NAME, operation.countryYName() );

        long minDate = dateUtil.utcToFormat(
                operation.startDate().getTime() );
        long maxDate = dateUtil.utcToFormat(
                operation.startDate().getTime() + TimeUnit.DAYS.toMillis( operation.durationDays() ) );
        Calendar calendar = connection.calendar();

        CityCountryCache cityCountryCache = new CityCountryCache( countryX, countryY );

        LongSet friendsInWrongCountries = new LongOpenHashSet();
        friendsInWrongCountries.add( person.getId() );
        LongSet friends = new LongOpenHashSet();
        for ( Relationship knows : person.getRelationships( Rels.KNOWS ) )
        {
            Node friend = knows.getOtherNode( person );
            if ( friends.contains( friend.getId() ) ||
                 friendsInWrongCountries.contains( friend.getId() ) )
            {
                // do nothing, person has already been seen
            }
            else if ( cityCountryCache.isPersonInCountries( friend ) )
            {
                friendsInWrongCountries.add( friend.getId() );
            }
            else
            {
                friends.add( friend.getId() );
            }
            for ( Relationship knowsKnows : friend.getRelationships( Rels.KNOWS ) )
            {
                Node friendsFriend = knowsKnows.getOtherNode( friend );
                if ( friends.contains( friendsFriend.getId() ) ||
                     friendsInWrongCountries.contains( friendsFriend.getId() ) )
                {
                    // do nothing, person has already been seen
                }
                else if ( cityCountryCache.isPersonInCountries( friendsFriend ) )
                {
                    friendsInWrongCountries.add( friendsFriend.getId() );
                }
                else
                {
                    friends.add( friendsFriend.getId() );
                }
            }
        }

        RelationshipType[] commentIsLocatedInAtTime =
                connection.timeStampedRelationshipTypesCache().commentIsLocatedInForDateRange(
                        calendar,
                        dateUtil.formatToEncodedDateAtResolution( minDate ),
                        dateUtil.formatToEncodedDateAtResolution( maxDate ),
                        dateUtil
                );
        RelationshipType[] postIsLocatedInAtTime =
                connection.timeStampedRelationshipTypesCache().postIsLocatedInForDateRange(
                        calendar,
                        dateUtil.formatToEncodedDateAtResolution( minDate ),
                        dateUtil.formatToEncodedDateAtResolution( maxDate ),
                        dateUtil
                );
        RelationshipType[] messageIsLocatedInAtTime = TimeStampedRelationshipTypesCache.joinArrays(
                commentIsLocatedInAtTime,
                postIsLocatedInAtTime );

        Long2IntMap personCountryXCounts = new Long2IntOpenHashMap();
        // TODO choose country based on degree
        for ( Relationship isLocatedIn : countryX.getRelationships( Direction.INCOMING, messageIsLocatedInAtTime ) )
        {
            Node message = isLocatedIn.getStartNode();
            Node creator = (message.hasLabel( Nodes.Comment ))
                           ? message.getSingleRelationship( Rels.COMMENT_HAS_CREATOR, Direction.OUTGOING ).getEndNode()
                           : message.getSingleRelationship( Rels.POST_HAS_CREATOR, Direction.OUTGOING ).getEndNode();
            long creatorId = creator.getId();
            if ( friends.contains( creatorId ) )
            {
                long creationDate = (long) message.getProperty( Message.CREATION_DATE );
                if ( creationDate >= minDate && maxDate > creationDate )
                {
                    if ( personCountryXCounts.containsKey( creatorId ) )
                    {
                        personCountryXCounts.put( creatorId, personCountryXCounts.get( creatorId ) + 1 );
                    }
                    else
                    {
                        personCountryXCounts.put( creatorId, 1 );
                    }
                }
            }
        }

        Map<Node,Integer> personCountryYCounts = new HashMap<>();
        for ( Relationship isLocatedIn : countryY.getRelationships( Direction.INCOMING, messageIsLocatedInAtTime ) )
        {
            Node message = isLocatedIn.getStartNode();
            Node creator = (message.hasLabel( Nodes.Comment ))
                           ? message.getSingleRelationship( Rels.COMMENT_HAS_CREATOR, Direction.OUTGOING ).getEndNode()
                           : message.getSingleRelationship( Rels.POST_HAS_CREATOR, Direction.OUTGOING ).getEndNode();
            if ( personCountryXCounts.containsKey( creator.getId() ) )
            {
                long creationDate = (long) message.getProperty( Message.CREATION_DATE );
                if ( creationDate >= minDate && maxDate > creationDate )
                {
                    if ( personCountryYCounts.containsKey( creator ) )
                    {
                        personCountryYCounts.put( creator, personCountryYCounts.get( creator ) + 1 );
                    }
                    else
                    {
                        personCountryYCounts.put( creator, 1 );
                    }
                }
            }
        }

        MinMaxPriorityQueue<LdbcQuery3PreResult> preResults = MinMaxPriorityQueue
                .orderedBy( DESCENDING_COUNT_ASCENDING_PERSON_ID_COMPARATOR )
                .maximumSize( operation.limit() )
                .create();

        for ( Node friend : personCountryYCounts.keySet() )
        {
            preResults.add(
                    new LdbcQuery3PreResult(
                            personCountryXCounts.get( friend.getId() ),
                            personCountryYCounts.get( friend ),
                            friend
                    )
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
        private final ManyToOneIsConnectedCache cityInCountryCache;

        private CityCountryCache( Node countryX, Node countryY )
        {
            LongSet countryIds = new LongOpenHashSet( 2 );
            countryIds.add( countryX.getId() );
            countryIds.add( countryY.getId() );
            this.cityInCountryCache = Operators.manyToOneIsConnected().lazyPull(
                    countryIds,
                    Rels.IS_PART_OF,
                    Direction.OUTGOING );
        }

        boolean isPersonInCountries( Node person ) throws DbException
        {
            Node city = person.getSingleRelationship( Rels.PERSON_IS_LOCATED_IN, Direction.OUTGOING ).getEndNode();
            return cityInCountryCache.isConnected( city );
        }
    }
}
