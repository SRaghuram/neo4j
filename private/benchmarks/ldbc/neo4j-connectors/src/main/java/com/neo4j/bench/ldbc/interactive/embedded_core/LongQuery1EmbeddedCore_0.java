/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.ldbc.interactive.embedded_core;

import com.google.common.collect.Lists;
import com.google.common.collect.MinMaxPriorityQueue;
import com.google.common.collect.Sets;
import com.ldbc.driver.DbException;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery1;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery1Result;
import com.neo4j.bench.ldbc.Domain.Nodes;
import com.neo4j.bench.ldbc.Domain.Organisation;
import com.neo4j.bench.ldbc.Domain.Person;
import com.neo4j.bench.ldbc.Domain.Place;
import com.neo4j.bench.ldbc.Domain.Rels;
import com.neo4j.bench.ldbc.Domain.StudiesAt;
import com.neo4j.bench.ldbc.Domain.WorksAt;
import com.neo4j.bench.ldbc.connection.Neo4jConnectionState;
import com.neo4j.bench.ldbc.connection.QueryDateUtil;
import com.neo4j.bench.ldbc.interactive.Neo4jQuery1;
import com.neo4j.bench.ldbc.operators.Operators;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.ResourceIterator;

public class LongQuery1EmbeddedCore_0 extends Neo4jQuery1<Neo4jConnectionState>
{
    private static final PersonLastNameAndIdComparator PERSON_LAST_NAME_AND_ID_COMPARATOR =
            new PersonLastNameAndIdComparator();
    private static final Query1ResultProjectionFunction QUERY_1_RESULT_PROJECTION_FUNCTION =
            new Query1ResultProjectionFunction();

    @Override
    public List<LdbcQuery1Result> execute( Neo4jConnectionState connection, LdbcQuery1 operation )
            throws DbException
    {
        QueryDateUtil dateUtil = connection.dateUtil();
        Node startPerson = Operators.findNode( connection.getTx(), Nodes.Person, Person.ID, operation.personId() );

        Set<Node> personsWithFirstName = Sets.newHashSet(
                connection.getTx().findNodes( Nodes.Person, Person.FIRST_NAME, operation.firstName() )
        );

        // friends
        Set<Node> personsAtDistance1 = Sets.newHashSet();
        List<PersonAndDistance> friends = Lists.newArrayList();
        for ( Relationship rel : startPerson.getRelationships( Rels.KNOWS ) )
        {
            Node friend = rel.getOtherNode( startPerson );
            if ( personsWithFirstName.contains( friend ) )
            {
                friends.add( new PersonAndDistance( friend, 1 ) );
                personsWithFirstName.remove( friend );
            }
            personsAtDistance1.add( friend );
        }
        Collections.sort( friends, PERSON_LAST_NAME_AND_ID_COMPARATOR );

        // friends of friends
        if ( operation.limit() > friends.size() && personsWithFirstName.size() > 0 )
        {
            MinMaxPriorityQueue<PersonAndDistance> friendsOfFriends = MinMaxPriorityQueue
                    .orderedBy( PERSON_LAST_NAME_AND_ID_COMPARATOR )
                    .maximumSize( operation.limit() - friends.size() )
                    .create();
            Set<Node> personsAtDistance2 = Sets.newHashSet();
            for ( Node personAtDistance1 : personsAtDistance1 )
            {
                for ( Relationship rel : personAtDistance1.getRelationships( Rels.KNOWS ) )
                {
                    Node friendOfFriend = rel.getOtherNode( personAtDistance1 );
                    if ( !personsAtDistance1.contains( friendOfFriend ) &&
                         !startPerson.equals( friendOfFriend ) )
                    {
                        if ( !personsAtDistance2.contains( friendOfFriend )
                             && personsWithFirstName.contains( friendOfFriend ) )
                        {
                            friendsOfFriends.add( new PersonAndDistance( friendOfFriend, 2 ) );
                            personsWithFirstName.remove( friendOfFriend );
                        }
                        personsAtDistance2.add( friendOfFriend );
                    }
                }
            }

            PersonAndDistance friendOfFriend;
            while ( null != (friendOfFriend = friendsOfFriends.poll()) )
            {
                friends.add( friendOfFriend );
            }

            // friends of friends of friends
            if ( operation.limit() > friends.size() && personsWithFirstName.size() > 0 )
            {
                MinMaxPriorityQueue<PersonAndDistance> friendsOfFriendsOfFriends = MinMaxPriorityQueue
                        .orderedBy( PERSON_LAST_NAME_AND_ID_COMPARATOR )
                        .maximumSize( operation.limit() - friends.size() )
                        .create();

                if ( personsWithFirstName.size() < personsAtDistance2.size() )
                {
                    // Start traversal from side of persons given first name
                    for ( Node personWithFirstName : personsWithFirstName )
                    {
                        try ( ResourceIterator<Relationship> rels =
                                      (ResourceIterator<Relationship>) personWithFirstName.getRelationships( Rels.KNOWS ).iterator() )
                        {
                            while ( rels.hasNext() )
                            {
                                Relationship rel = rels.next();
                                Node potentialFriendOfFriend = rel.getOtherNode( personWithFirstName );
                                if ( personsAtDistance2.contains( potentialFriendOfFriend ) )
                                {
                                    friendsOfFriendsOfFriends.add( new PersonAndDistance( personWithFirstName, 3 ) );
                                    break;
                                }
                            }
                        }
                    }
                }
                else
                {
                    // Start traversal from side of friends of friends
                    for ( Node personAtDistance2 : personsAtDistance2 )
                    {
                        for ( Relationship rel : personAtDistance2.getRelationships( Rels.KNOWS ) )
                        {
                            Node friendOfFriendOfFriend = rel.getOtherNode( personAtDistance2 );
                            // person is friend already
                            if ( personsAtDistance1.contains( friendOfFriendOfFriend ) )
                            {
                                continue;
                            }
                            // person is friend of friend already
                            if ( personsAtDistance2.contains( friendOfFriendOfFriend ) )
                            {
                                continue;
                            }
                            // person is friend of friend of friend already
                            if ( personsWithFirstName.contains( friendOfFriendOfFriend ) )
                            {
                                friendsOfFriendsOfFriends.add( new PersonAndDistance( friendOfFriendOfFriend, 3 ) );
                                personsWithFirstName.remove( friendOfFriendOfFriend );
                            }
                        }
                    }
                }

                PersonAndDistance friendsOfFriendsOfFriend;
                while ( null != (friendsOfFriendsOfFriend = friendsOfFriendsOfFriends.poll()) )
                {
                    friends.add( friendsOfFriendsOfFriend );
                }
            }
        }

        List<LdbcQuery1Result> results = new ArrayList<>();

        CompanyCache companyCache = new CompanyCache();
        UniversityCache universityCache = new UniversityCache();
        Iterator<PersonAndDistance> friendsIterator = friends.iterator();
        for ( int i = 0; i < operation.limit() && friendsIterator.hasNext(); i++ )
        {
            results.add(
                    QUERY_1_RESULT_PROJECTION_FUNCTION.apply(
                            friendsIterator.next(),
                            companyCache,
                            universityCache,
                            dateUtil
                    )
            );
        }

        return results;
    }

    private class PersonAndDistance
    {
        private final Node person;
        private final int distance;
        private long id = -1;
        private String lastName;

        private PersonAndDistance( Node person, int distance )
        {
            this.person = person;
            this.distance = distance;
        }

        public Node node()
        {
            return person;
        }

        public long id()
        {
            if ( -1 == id )
            {
                id = (long) person.getProperty( Person.ID );
            }
            return id;
        }

        public String lastName()
        {
            if ( null == lastName )
            {
                lastName = (String) person.getProperty( Person.LAST_NAME );
            }
            return lastName;
        }

        public int distance()
        {
            return distance;
        }
    }

    private static class PersonLastNameAndIdComparator implements Comparator<PersonAndDistance>
    {
        @Override
        public int compare( PersonAndDistance personAndDistance1, PersonAndDistance personAndDistance2 )
        {
            // first sort by last name
            int lastNameCompare = personAndDistance1.lastName().compareTo( personAndDistance2.lastName() );
            if ( 0 != lastNameCompare )
            {
                return lastNameCompare;
            }
            // then sort by id
            if ( personAndDistance1.id() > personAndDistance2.id() )
            {
                return 1;
            }
            if ( personAndDistance1.id() < personAndDistance2.id() )
            {
                return -1;
            }
            return 0;
        }
    }

    private static class Query1ResultProjectionFunction
    {
        public LdbcQuery1Result apply(
                PersonAndDistance personAndDistance,
                CompanyCache companyCache,
                UniversityCache universityCache,
                QueryDateUtil dateUtil )
        {
            Node person = personAndDistance.node();
            int pathLength = personAndDistance.distance();
            long id = personAndDistance.id();
            String lastName = personAndDistance.lastName();
            long birthday = dateUtil.formatToUtc( (long) person.getProperty( Person.BIRTHDAY ) );
            long creationDate = dateUtil.formatToUtc( (long) person.getProperty( Person.CREATION_DATE ) );
            String gender = (String) person.getProperty( Person.GENDER );
            List<String> languages = Lists.newArrayList( (String[]) person.getProperty( Person.LANGUAGES ) );
            String browser = (String) person.getProperty( Person.BROWSER_USED );
            String ip = (String) person.getProperty( Person.LOCATION_IP );
            List<String> emails = Lists.newArrayList( (String[]) person.getProperty( Person.EMAIL_ADDRESSES ) );
            String personCity = (String) person
                    .getSingleRelationship( Rels.PERSON_IS_LOCATED_IN, Direction.OUTGOING )
                    .getEndNode()
                    .getProperty( Place.NAME );

            List<List<Object>> unis = new ArrayList<>();
            for ( Relationship studyAt : person.getRelationships( Direction.OUTGOING, Rels.STUDY_AT ) )
            {
                Node university = studyAt.getEndNode();
                unis.add(
                        universityCache.universityInfoFor( university, studyAt )
                );
            }

            List<List<Object>> companies = new ArrayList<>();
            for ( Relationship worksAt : person.getRelationships( Direction.OUTGOING, Rels.WORKS_AT ) )
            {
                Node company = worksAt.getEndNode();
                companies.add(
                        companyCache.companyInfoFor( company, worksAt )
                );
            }

            return new LdbcQuery1Result(
                    id,
                    lastName,
                    pathLength,
                    birthday,
                    creationDate,
                    gender,
                    browser,
                    ip,
                    emails,
                    languages,
                    personCity,
                    unis,
                    companies );
        }
    }

    private static class UniversityCache
    {
        private final Map<Node,String> universityNames;
        private final Map<Node,String> universityCityNames;
        private final PlaceNameCache placeNameCache;

        private UniversityCache()
        {
            this.placeNameCache = new PlaceNameCache();
            this.universityNames = new HashMap<>();
            this.universityCityNames = new HashMap<>();
        }

        private List<Object> universityInfoFor( Node university, Relationship studyAt )
        {
            String universityName = universityNames.get( university );
            String universityCityName;
            if ( null == universityName )
            {
                universityName = (String) university.getProperty( Organisation.NAME );
                universityNames.put( university, universityName );
                Node city = university.getSingleRelationship( Rels.ORGANISATION_IS_LOCATED_IN, Direction.OUTGOING )
                        .getEndNode();
                universityCityName = placeNameCache.nameForPlace( city );
                universityCityNames.put( university, universityCityName );
            }
            else
            {
                universityCityName = universityCityNames.get( university );
            }
            List<Object> universityInfo = new ArrayList<>();
            universityInfo.add( universityName );
            universityInfo.add( studyAt.getProperty( StudiesAt.CLASS_YEAR ) );
            universityInfo.add( universityCityName );
            return universityInfo;
        }
    }

    private static class CompanyCache
    {
        private final Map<Node,String> companyNames;
        private final Map<Node,String> companyCountryNames;
        private final PlaceNameCache placeNameCache;

        private CompanyCache()
        {
            this.placeNameCache = new PlaceNameCache();
            this.companyNames = new HashMap<>();
            this.companyCountryNames = new HashMap<>();
        }

        private List<Object> companyInfoFor( Node company, Relationship worksAt )
        {
            String companyName = companyNames.get( company );
            String companyCountryName;
            if ( null == companyName )
            {
                companyName = (String) company.getProperty( Organisation.NAME );
                companyNames.put( company, companyName );
                Node country = company.getSingleRelationship( Rels.ORGANISATION_IS_LOCATED_IN, Direction.OUTGOING )
                        .getEndNode();
                companyCountryName = placeNameCache.nameForPlace( country );
                companyCountryNames.put( company, companyCountryName );
            }
            else
            {
                companyCountryName = companyCountryNames.get( company );
            }
            List<Object> companyInfo = new ArrayList<>();
            companyInfo.add( companyName );
            companyInfo.add( worksAt.getProperty( WorksAt.WORK_FROM ) );
            companyInfo.add( companyCountryName );
            return companyInfo;
        }
    }

    private static class PlaceNameCache
    {
        private final Map<Node,String> placeNames;

        private PlaceNameCache()
        {
            this.placeNames = new HashMap<>();
        }

        private String nameForPlace( Node country )
        {
            String name = placeNames.get( country );
            if ( null == name )
            {
                name = (String) country.getProperty( Place.NAME );
                placeNames.put( country, name );
            }
            return name;
        }
    }
}
