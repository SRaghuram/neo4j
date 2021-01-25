/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.ldbc.interactive.embedded_core;

import com.google.common.collect.MinMaxPriorityQueue;
import com.ldbc.driver.DbException;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery5;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery5Result;
import com.neo4j.bench.ldbc.Domain.Forum;
import com.neo4j.bench.ldbc.Domain.HasMember;
import com.neo4j.bench.ldbc.Domain.Nodes;
import com.neo4j.bench.ldbc.Domain.Person;
import com.neo4j.bench.ldbc.Domain.Rels;
import com.neo4j.bench.ldbc.connection.Neo4jConnectionState;
import com.neo4j.bench.ldbc.connection.QueryDateUtil;
import com.neo4j.bench.ldbc.interactive.Neo4jQuery5;
import com.neo4j.bench.ldbc.operators.Operators;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;

public class LongQuery5EmbeddedCore_0 extends Neo4jQuery5<Neo4jConnectionState>
{
    private static final DescendingPostCountAscendingForumIdComparator
            DESCENDING_POST_COUNT_ASCENDING_FORUM_ID_COMPARATOR = new DescendingPostCountAscendingForumIdComparator();

    @Override
    public List<LdbcQuery5Result> execute( Neo4jConnectionState connection, final LdbcQuery5 operation )
            throws DbException
    {
        QueryDateUtil dateUtil = connection.dateUtil();
        long minDate = dateUtil.utcToFormat( operation.minDate().getTime() );

        Node person = Operators.findNode( connection.getTx(), Nodes.Person, Person.ID, operation.personId() );

        Set<Node> friends = new HashSet<>();
        for ( Relationship knows : person.getRelationships( Rels.KNOWS ) )
        {
            Node friend = knows.getOtherNode( person );
            friends.add( friend );
            for ( Relationship knowsKnows : friend.getRelationships( Rels.KNOWS ) )
            {
                Node friendsFriend = knowsKnows.getOtherNode( friend );
                if ( !friendsFriend.equals( person ) )
                {
                    friends.add( friendsFriend );
                }
            }
        }

        Map<Node,Set<Node>> friendsInForums = new HashMap<>();
        for ( Node friend : friends )
        {
            for ( Relationship hasMember : friend.getRelationships( Direction.INCOMING, Rels.HAS_MEMBER ) )
            {
                long joinDate = (long) hasMember.getProperty( HasMember.JOIN_DATE );
                if ( joinDate > minDate )
                {
                    Node forum = hasMember.getStartNode();
                    Set<Node> friendsInForum = friendsInForums.get( forum );
                    if ( null == friendsInForum )
                    {
                        friendsInForum = new HashSet<>();
                        friendsInForums.put( forum, friendsInForum );
                    }
                    friendsInForum.add( friend );
                }
            }
        }

        MinMaxPriorityQueue<LdbcQuery5PreResult> preResults = MinMaxPriorityQueue
                .orderedBy( DESCENDING_POST_COUNT_ASCENDING_FORUM_ID_COMPARATOR )
                .maximumSize( operation.limit() )
                .create();

        for ( Node forum : friendsInForums.keySet() )
        {
            int forumPostCount = 0;
            for ( Relationship containerOf : forum.getRelationships( Direction.OUTGOING, Rels.CONTAINER_OF ) )
            {
                Node post = containerOf.getEndNode();
                Node creator = post.getSingleRelationship( Rels.POST_HAS_CREATOR, Direction.OUTGOING ).getEndNode();
                Set<Node> friendsInForum = friendsInForums.get( forum );
                if ( friendsInForum.contains( creator ) )
                {
                    forumPostCount++;
                }
            }
            preResults.add( new LdbcQuery5PreResult( forum, forumPostCount ) );
        }

        List<LdbcQuery5Result> results = new ArrayList<>();
        LdbcQuery5PreResult preResult;
        while ( null != (preResult = preResults.poll()) )
        {
            results.add(
                    new LdbcQuery5Result( preResult.forumTitle(), preResult.postCount() )
            );
        }
        return results;
    }

    private static class LdbcQuery5PreResult
    {
        private final Node forum;
        private final int postCount;
        private long forumId = -1;
        private String forumTitle;

        private LdbcQuery5PreResult( Node forum, int postCount )
        {
            this.forum = forum;
            this.postCount = postCount;
        }

        private long forumId()
        {
            if ( -1 == forumId )
            {
                forumId = (long) forum.getProperty( Forum.ID );
            }
            return forumId;
        }

        private String forumTitle()
        {
            if ( null == forumTitle )
            {
                forumTitle = (String) forum.getProperty( Forum.TITLE );
            }
            return forumTitle;
        }

        private int postCount()
        {
            return postCount;
        }
    }

    public static class DescendingPostCountAscendingForumIdComparator implements Comparator<LdbcQuery5PreResult>
    {
        @Override
        public int compare( LdbcQuery5PreResult preResult1, LdbcQuery5PreResult preResult2 )
        {
            // descending post count
            if ( preResult1.postCount() < preResult2.postCount() )
            {
                return 1;
            }
            else if ( preResult1.postCount() > preResult2.postCount() )
            {
                return -1;
            }
            else
            {
                // ascending forum id
                if ( preResult1.forumId() > preResult2.forumId() )
                {
                    return 1;
                }
                else if ( preResult1.forumId() < preResult2.forumId() )
                {
                    return -1;
                }
                else
                {
                    return 0;
                }
            }
        }
    }
}
