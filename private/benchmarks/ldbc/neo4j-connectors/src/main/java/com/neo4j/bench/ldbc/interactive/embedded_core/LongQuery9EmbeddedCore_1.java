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
import com.neo4j.bench.ldbc.connection.TimeStampedRelationshipTypesCache;
import com.neo4j.bench.ldbc.interactive.Neo4jQuery9;
import com.neo4j.bench.ldbc.operators.Operators;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;

public class LongQuery9EmbeddedCore_1 extends Neo4jQuery9<Neo4jConnectionState>
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

        MinMaxPriorityQueue<LdbcQuery9PreResult> preResults = MinMaxPriorityQueue
                .orderedBy( DESCENDING_MESSAGE_CREATION_DATE_ASCENDING_MESSAGE_ID )
                .maximumSize( operation.limit() )
                .create();

        long maxCreationDate = dateUtil.utcToFormat( operation.maxDate().getTime() );
        long leastRecentMessageCreationDate = Long.MIN_VALUE;
        boolean preResultSizeIsAtLimit = false;
        boolean leastRecentMessageCreationDateWasModified;

        final Calendar calendar = connection.calendar();
        RelationshipType[] hasCreatorRelationshipTypes = null;

        Iterator<Node> friendsIterator = friends.iterator();

        while ( friendsIterator.hasNext() )
        {
            Node friend = friendsIterator.next();
            for ( Relationship hasCreator : friend
                    .getRelationships( Direction.INCOMING, Rels.POST_HAS_CREATOR, Rels.COMMENT_HAS_CREATOR ) )
            {
                Node message = hasCreator.getStartNode();
                long messageCreationDate = (long) message.getProperty( Message.CREATION_DATE );
                if ( messageCreationDate < maxCreationDate && messageCreationDate >= leastRecentMessageCreationDate )
                {
                    preResults.add(
                            new LdbcQuery9PreResult(
                                    friend,
                                    message,
                                    messageCreationDate
                            )
                    );
                    if ( preResultSizeIsAtLimit )
                    {
                        leastRecentMessageCreationDate = preResults.peekLast().messageCreationDate();
                    }
                    else if ( preResults.size() == operation.limit() )
                    {
                        leastRecentMessageCreationDate = preResults.peekLast().messageCreationDate();
                        preResultSizeIsAtLimit = true;
                    }
                }
            }
            if ( preResultSizeIsAtLimit )
            {
                RelationshipType[] commentHasCreatorAtTime =
                        connection.timeStampedRelationshipTypesCache().commentHasCreatorForDateRange(
                                calendar,
                                dateUtil.formatToEncodedDateAtResolution( leastRecentMessageCreationDate ),
                                dateUtil.formatToEncodedDateAtResolution( maxCreationDate ),
                                dateUtil
                        );
                RelationshipType[] postHasCreatorAtTime =
                        connection.timeStampedRelationshipTypesCache().postHasCreatorForDateRange(
                                calendar,
                                dateUtil.formatToEncodedDateAtResolution( leastRecentMessageCreationDate ),
                                dateUtil.formatToEncodedDateAtResolution( maxCreationDate ),
                                dateUtil
                        );
                hasCreatorRelationshipTypes = TimeStampedRelationshipTypesCache.joinArrays(
                        commentHasCreatorAtTime,
                        postHasCreatorAtTime );

                break;
            }
        }

        while ( friendsIterator.hasNext() )
        {
            Node friend = friendsIterator.next();
            leastRecentMessageCreationDateWasModified = false;
            for ( Relationship hasCreator : friend.getRelationships( Direction.INCOMING, hasCreatorRelationshipTypes ) )
            {
                Node message = hasCreator.getStartNode();
                long messageCreationDate = (long) message.getProperty( Message.CREATION_DATE );
                if ( messageCreationDate < maxCreationDate && messageCreationDate >= leastRecentMessageCreationDate )
                {
                    preResults.add(
                            new LdbcQuery9PreResult(
                                    friend,
                                    message,
                                    messageCreationDate
                            )
                    );
                    long newLeastRecentMessageCreationDate = preResults.peekLast().messageCreationDate();
                    if ( newLeastRecentMessageCreationDate > leastRecentMessageCreationDate )
                    {
                        leastRecentMessageCreationDate = preResults.peekLast().messageCreationDate();
                        leastRecentMessageCreationDateWasModified = true;
                    }
                }
            }
            if ( leastRecentMessageCreationDateWasModified )
            {
                RelationshipType[] commentHasCreatorAtTime =
                        connection.timeStampedRelationshipTypesCache().commentHasCreatorForDateRange(
                                calendar,
                                dateUtil.formatToEncodedDateAtResolution( leastRecentMessageCreationDate ),
                                dateUtil.formatToEncodedDateAtResolution( maxCreationDate ),
                                dateUtil
                        );
                RelationshipType[] postHasCreatorAtTime =
                        connection.timeStampedRelationshipTypesCache().postHasCreatorForDateRange(
                                calendar,
                                dateUtil.formatToEncodedDateAtResolution( leastRecentMessageCreationDate ),
                                dateUtil.formatToEncodedDateAtResolution( maxCreationDate ),
                                dateUtil
                        );
                hasCreatorRelationshipTypes = TimeStampedRelationshipTypesCache.joinArrays(
                        commentHasCreatorAtTime,
                        postHasCreatorAtTime );
            }
        }

        List<LdbcQuery9Result> results = new ArrayList<>();
        LdbcQuery9PreResult preResult;
        while ( null != (preResult = preResults.poll()) )
        {
            Map<String,Object> personProperties = preResult.personProperties();
            results.add(
                    new LdbcQuery9Result(
                            (long) personProperties.get( Person.ID ),
                            (String) personProperties.get( Person.FIRST_NAME ),
                            (String) personProperties.get( Person.LAST_NAME ),
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

        private long messageId = -1;
        private String messageContent;

        private LdbcQuery9PreResult( Node person, Node message, long messageCreationDate )
        {
            this.person = person;
            this.message = message;
            this.messageCreationDate = messageCreationDate;
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
                if ( message.hasLabel( Nodes.Post ) )
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

        private Map<String,Object> personProperties()
        {
            return person.getProperties(
                    Person.ID,
                    Person.FIRST_NAME,
                    Person.LAST_NAME );
        }
    }
}
