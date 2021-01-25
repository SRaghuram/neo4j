/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.ldbc.interactive.embedded_core;

import com.google.common.collect.MinMaxPriorityQueue;
import com.ldbc.driver.DbException;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery11;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery11Result;
import com.neo4j.bench.ldbc.Domain.Nodes;
import com.neo4j.bench.ldbc.Domain.Organisation;
import com.neo4j.bench.ldbc.Domain.Person;
import com.neo4j.bench.ldbc.Domain.Place;
import com.neo4j.bench.ldbc.Domain.Rels;
import com.neo4j.bench.ldbc.Domain.WorksAt;
import com.neo4j.bench.ldbc.connection.Neo4jConnectionState;
import com.neo4j.bench.ldbc.interactive.Neo4jQuery11;
import com.neo4j.bench.ldbc.operators.Operators;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;

public class LongQuery11EmbeddedCore_1_2 extends Neo4jQuery11<Neo4jConnectionState>
{
    @Override
    public List<LdbcQuery11Result> execute( Neo4jConnectionState connection, LdbcQuery11 operation )
            throws DbException
    {
        Node person = Operators.findNode( connection.getTx(), Nodes.Person, Person.ID, operation.personId() );
        Node country = Operators.findNode( connection.getTx(), Place.Type.Country, Place.NAME, operation.countryName() );

        Set<Node> companies = new HashSet<>();
        for ( Relationship isLocatedIn : country.getRelationships( Direction.INCOMING, Rels.ORGANISATION_IS_LOCATED_IN ) )
        {
            Node company = isLocatedIn.getStartNode();
            if ( company.hasLabel( Organisation.Type.University ) )
            {
                continue;
            }
            companies.add( company );
        }

        Set<Node> friends = new HashSet<>();
        for ( Relationship knows : person.getRelationships( Rels.KNOWS ) )
        {
            Node friend = knows.getOtherNode( person );
            friends.add( friend );
            for ( Relationship knowsKnows : friend.getRelationships( Rels.KNOWS ) )
            {
                Node friendsFriend = knowsKnows.getOtherNode( friend );
                if ( friendsFriend.equals( person ) )
                {
                    continue;
                }
                friends.add( friendsFriend );
            }
        }

        MinMaxPriorityQueue<LdbcQuery11PreResult> preResults = MinMaxPriorityQueue
                .maximumSize( operation.limit() )
                .create();
        int mostRecentToleratedWorkFromYear = operation.workFromYear() - 1;
        boolean atLimit = false;

        Iterator<Node> companiesIterator = companies.iterator();
        RelationshipType[] worksAtRelationshipTypes =
                connection.timeStampedRelationshipTypesCache().worksAtForAllYears();
        boolean mostRecentToleratedWorkFromYearWasModified;

        while ( companiesIterator.hasNext() )
        {
            Node company = companiesIterator.next();
            for ( Relationship worksAt : company.getRelationships( Direction.INCOMING, worksAtRelationshipTypes ) )
            {
                Node potentialFriend = worksAt.getStartNode();
                if ( friends.contains( potentialFriend ) )
                {
                    int workFromYear = (int) worksAt.getProperty( WorksAt.WORK_FROM );
                    if ( workFromYear <= mostRecentToleratedWorkFromYear )
                    {
                        preResults.add( new LdbcQuery11PreResult( potentialFriend, workFromYear, company ) );
                        if ( atLimit )
                        {
                            mostRecentToleratedWorkFromYear = preResults.peekLast().workFromYear();
                        }
                        else if ( preResults.size() == operation.limit() )
                        {
                            mostRecentToleratedWorkFromYear = preResults.peekLast().workFromYear();
                            atLimit = true;
                        }
                    }
                }
            }
            if ( atLimit )
            {
                worksAtRelationshipTypes = connection.timeStampedRelationshipTypesCache().worksAtForYearsBefore(
                        mostRecentToleratedWorkFromYear
                );
                break;
            }
        }

        while ( companiesIterator.hasNext() )
        {
            Node company = companiesIterator.next();
            mostRecentToleratedWorkFromYearWasModified = false;
            for ( Relationship worksAt : company.getRelationships( Direction.INCOMING, worksAtRelationshipTypes ) )
            {
                Node potentialFriend = worksAt.getStartNode();
                if ( friends.contains( potentialFriend ) )
                {
                    int workFromYear = (int) worksAt.getProperty( WorksAt.WORK_FROM );
                    preResults.add( new LdbcQuery11PreResult( potentialFriend, workFromYear, company ) );
                    int newMostRecentToleratedWorkFromYear = preResults.peekLast().workFromYear();
                    if ( newMostRecentToleratedWorkFromYear < mostRecentToleratedWorkFromYear )
                    {
                        mostRecentToleratedWorkFromYear = newMostRecentToleratedWorkFromYear;
                        mostRecentToleratedWorkFromYearWasModified = true;
                    }
                }
            }
            if ( mostRecentToleratedWorkFromYearWasModified )
            {
                worksAtRelationshipTypes = connection.timeStampedRelationshipTypesCache().worksAtForYearsBefore(
                        mostRecentToleratedWorkFromYear
                );
            }
        }

        List<LdbcQuery11Result> results = new ArrayList<>();
        LdbcQuery11PreResult preResult;
        while ( null != (preResult = preResults.poll()) )
        {
            long friendId = preResult.friendId();
            String friendFirstName = (String) preResult.friend().getProperty( Person.FIRST_NAME );
            String friendLastName = (String) preResult.friend().getProperty( Person.LAST_NAME );
            results.add(
                    new LdbcQuery11Result(
                            friendId,
                            friendFirstName,
                            friendLastName,
                            preResult.companyName(),
                            preResult.workFromYear()
                    )
            );
        }

        return results;
    }

    private class LdbcQuery11PreResult implements Comparable<LdbcQuery11PreResult>
    {
        private final Node friend;
        private final int workFromYear;
        private final Node company;
        private long friendId = -1;
        private String companyName;

        private LdbcQuery11PreResult( Node friend, int workFromYear, Node company )
        {
            this.friend = friend;
            this.workFromYear = workFromYear;
            this.company = company;
        }

        private int workFromYear()
        {
            return workFromYear;
        }

        private long friendId()
        {
            if ( -1 == friendId )
            {
                friendId = (long) friend.getProperty( Person.ID );
            }
            return friendId;
        }

        private String companyName()
        {
            if ( null == companyName )
            {
                companyName = (String) company.getProperty( Organisation.NAME );
            }
            return companyName;
        }

        private Node friend()
        {
            return friend;
        }

        @Override
        public int compareTo( LdbcQuery11PreResult other )
        {
            if ( this.workFromYear() < other.workFromYear() )
            {
                return -1;
            }
            else if ( this.workFromYear() > other.workFromYear() )
            {
                return 1;
            }
            else
            {
                if ( this.friendId() < other.friendId() )
                {
                    return -1;
                }
                else if ( this.friendId() > other.friendId() )
                {
                    return 1;
                }
                else
                {
                    return other.companyName().compareTo( this.companyName() );
                }
            }
        }
    }
}
