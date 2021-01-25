/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.ldbc.interactive.embedded_core;

import com.google.common.collect.MinMaxPriorityQueue;
import com.ldbc.driver.DbException;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery9;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery9Result;
import com.neo4j.bench.ldbc.Domain.Message;
import com.neo4j.bench.ldbc.Domain.Nodes;
import com.neo4j.bench.ldbc.Domain.Person;
import com.neo4j.bench.ldbc.Domain.Post;
import com.neo4j.bench.ldbc.Domain.Rels;
import com.neo4j.bench.ldbc.connection.Neo4jConnectionState;
import com.neo4j.bench.ldbc.connection.QueryDateUtil;
import com.neo4j.bench.ldbc.interactive.Neo4jQuery9;
import com.neo4j.bench.ldbc.operators.Operators;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;

public class LongQuery9EmbeddedCore_0 extends Neo4jQuery9<Neo4jConnectionState>
{
    private static final DescendingMessageCreationDateAscendingMessageId
            DESCENDING_MESSAGE_CREATION_DATE_ASCENDING_MESSAGE_ID =
            new DescendingMessageCreationDateAscendingMessageId();

    @Override
    public List<LdbcQuery9Result> execute( Neo4jConnectionState connection, LdbcQuery9 operation )
            throws DbException
    {
        QueryDateUtil dateUtil = connection.dateUtil();
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

        long creationDate = dateUtil.utcToFormat( operation.maxDate().getTime() );
        MinMaxPriorityQueue<LdbcQuery9PreResult> preResults = MinMaxPriorityQueue
                .orderedBy( DESCENDING_MESSAGE_CREATION_DATE_ASCENDING_MESSAGE_ID )
                .maximumSize( operation.limit() )
                .create();

        long leastRecentPostCreationDate = Long.MIN_VALUE;
        int preResultsSize = 0;

        for ( Node friend : friends )
        {
            for ( Relationship hasCreator : friend
                    .getRelationships( Direction.INCOMING, Rels.POST_HAS_CREATOR ) )
            {
                Node message = hasCreator.getStartNode();
                long messageCreationDate = (long) message.getProperty( Message.CREATION_DATE );
                if ( messageCreationDate < creationDate && messageCreationDate >= leastRecentPostCreationDate )
                {
                    preResults.add(
                            new LdbcQuery9PreResult(
                                    friend,
                                    message,
                                    messageCreationDate,
                                    true
                            )
                    );
                    if ( preResultsSize == operation.limit() )
                    {
                        leastRecentPostCreationDate = preResults.peekLast().messageCreationDate();
                    }
                    else
                    {
                        preResultsSize++;
                    }
                }
            }
        }

        for ( Node friend : friends )
        {
            for ( Relationship hasCreator : friend
                    .getRelationships( Direction.INCOMING, Rels.COMMENT_HAS_CREATOR ) )
            {
                Node message = hasCreator.getStartNode();
                long messageCreationDate = (long) message.getProperty( Message.CREATION_DATE );
                if ( messageCreationDate < creationDate && messageCreationDate >= leastRecentPostCreationDate )
                {
                    preResults.add(
                            new LdbcQuery9PreResult(
                                    friend,
                                    message,
                                    messageCreationDate,
                                    false
                            )
                    );
                    if ( preResultsSize == operation.limit() )
                    {
                        leastRecentPostCreationDate = preResults.peekLast().messageCreationDate();
                    }
                    else
                    {
                        preResultsSize++;
                    }
                }
            }
        }

        List<LdbcQuery9Result> results = new ArrayList<>();
        LdbcQuery9PreResult preResult;
        while ( null != (preResult = preResults.poll()) )
        {
            results.add(
                    new LdbcQuery9Result(
                            preResult.personId(),
                            preResult.personFirstName(),
                            preResult.personLastName(),
                            preResult.messageId(),
                            preResult.messageContent(),
                            dateUtil.formatToUtc( preResult.messageCreationDate() )
                    )
            );
        }

        return results;
    }

    public static class DescendingMessageCreationDateAscendingMessageId implements Comparator<LdbcQuery9PreResult>
    {
        @Override
        public int compare( LdbcQuery9PreResult result1, LdbcQuery9PreResult result2 )
        {
            if ( result1.messageCreationDate() > result2.messageCreationDate() )
            {
                return -1;
            }
            else if ( result1.messageCreationDate() < result2.messageCreationDate() )
            {
                return 1;
            }
            else
            {
                if ( result1.messageId() < result2.messageId() )
                {
                    return -1;
                }
                else if ( result1.messageId() > result2.messageId() )
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

    private class LdbcQuery9PreResult
    {
        private final Node person;
        private final Node message;
        private final long messageCreationDate;
        private final boolean messageIsPost;

        private long messageId = -1;
        private String messageContent;
        private long personId = -1;
        private String personFirstName;
        private String personLastName;

        private LdbcQuery9PreResult( Node person, Node message, long messageCreationDate, boolean messageIsPost )
        {
            this.person = person;
            this.message = message;
            this.messageCreationDate = messageCreationDate;
            this.messageIsPost = messageIsPost;
        }

        private long messageId()
        {
            if ( -1 == messageId )
            {
                messageId = (long) message.getProperty( Message.ID );
            }
            return messageId;
        }

        private long messageCreationDate()
        {
            return messageCreationDate;
        }

        private String messageContent()
        {
            if ( null == messageContent )
            {
                if ( messageIsPost )
                {
                    messageContent = (message.hasProperty( Message.CONTENT ))
                                     ? (String) message.getProperty( Message.CONTENT )
                                     : (String) message.getProperty( Post.IMAGE_FILE );
                }
                else
                {
                    messageContent = (String) message.getProperty( Message.CONTENT );
                }
            }
            return messageContent;
        }

        private long personId()
        {
            if ( -1 == personId )
            {
                personId = (long) person.getProperty( Person.ID );
            }
            return personId;
        }

        private String personFirstName()
        {
            if ( null == personFirstName )
            {
                personFirstName = (String) person.getProperty( Person.FIRST_NAME );
            }
            return personFirstName;
        }

        private String personLastName()
        {
            if ( null == personLastName )
            {
                personLastName = (String) person.getProperty( Person.LAST_NAME );
            }
            return personLastName;
        }
    }
}
