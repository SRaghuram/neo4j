/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.ldbc.interactive.embedded_core;

import com.ldbc.driver.DbException;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcShortQuery3PersonFriends;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcShortQuery3PersonFriendsResult;
import com.neo4j.bench.ldbc.Domain.Knows;
import com.neo4j.bench.ldbc.Domain.Nodes;
import com.neo4j.bench.ldbc.Domain.Person;
import com.neo4j.bench.ldbc.Domain.Rels;
import com.neo4j.bench.ldbc.connection.Neo4jConnectionState;
import com.neo4j.bench.ldbc.connection.QueryDateUtil;
import com.neo4j.bench.ldbc.interactive.Neo4jShortQuery3;
import com.neo4j.bench.ldbc.operators.Operators;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;

public class ShortQuery3EmbeddedCore_0_1_2 extends Neo4jShortQuery3<Neo4jConnectionState>
{
    private static final DescendingCreationDateAscendingIdComparator DESCENDING_CREATION_DATE_ASCENDING_ID_COMPARATOR =
            new DescendingCreationDateAscendingIdComparator();
    private static final String[] PERSON_PROPERTIES = new String[]{
            Person.ID,
            Person.FIRST_NAME,
            Person.LAST_NAME};

    @Override
    public List<LdbcShortQuery3PersonFriendsResult> execute( Neo4jConnectionState connection,
            LdbcShortQuery3PersonFriends operation ) throws DbException
    {
        QueryDateUtil dateUtil = connection.dateUtil();
        Node person = Operators.findNode( connection.getTx(), Nodes.Person, Person.ID, operation.personId() );

        List<LdbcShortQuery3PersonFriendsResult> results = new ArrayList<>();
        for ( Relationship knows : person.getRelationships( Direction.BOTH, Rels.KNOWS ) )
        {
            Node friend = knows.getOtherNode( person );
            Map<String,Object> friendProperties = friend.getProperties( PERSON_PROPERTIES );
            results.add(
                    new LdbcShortQuery3PersonFriendsResult(
                            (long) friendProperties.get( Person.ID ),
                            (String) friendProperties.get( Person.FIRST_NAME ),
                            (String) friendProperties.get( Person.LAST_NAME ),
                            dateUtil.formatToUtc( (long) knows.getProperty( Knows.CREATION_DATE ) )
                    )
            );
        }

        Collections.sort( results, DESCENDING_CREATION_DATE_ASCENDING_ID_COMPARATOR );

        return results;
    }

    private static class DescendingCreationDateAscendingIdComparator
            implements Comparator<LdbcShortQuery3PersonFriendsResult>
    {
        @Override
        public int compare( LdbcShortQuery3PersonFriendsResult result1, LdbcShortQuery3PersonFriendsResult result2 )
        {
            if ( result1.friendshipCreationDate() > result2.friendshipCreationDate() )
            {
                return -1;
            }
            else if ( result1.friendshipCreationDate() < result2.friendshipCreationDate() )
            {
                return 1;
            }
            else
            {
                if ( result1.personId() < result2.personId() )
                {
                    return -1;
                }
                else if ( result1.personId() > result2.personId() )
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
}
