/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.ldbc.interactive.embedded_core;

import com.google.common.collect.MinMaxPriorityQueue;
import com.ldbc.driver.DbException;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery2;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery2Result;
import com.neo4j.bench.ldbc.Domain.Message;
import com.neo4j.bench.ldbc.Domain.Nodes;
import com.neo4j.bench.ldbc.Domain.Person;
import com.neo4j.bench.ldbc.Domain.Post;
import com.neo4j.bench.ldbc.Domain.Rels;
import com.neo4j.bench.ldbc.connection.Neo4jConnectionState;
import com.neo4j.bench.ldbc.connection.QueryDateUtil;
import com.neo4j.bench.ldbc.connection.TimeStampedRelationshipTypesCache;
import com.neo4j.bench.ldbc.interactive.Neo4jQuery2;
import com.neo4j.bench.ldbc.operators.Operators;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;

public class LongQuery2EmbeddedCore_1 extends Neo4jQuery2<Neo4jConnectionState>
{
    private static final DescendingCreationDateAscendingMessageIdComparator
            DESCENDING_CREATION_DATE_ASCENDING_MESSAGE_ID_COMPARATOR =
            new DescendingCreationDateAscendingMessageIdComparator();

    @Override
    public List<LdbcQuery2Result> execute( Neo4jConnectionState connection, LdbcQuery2 operation )
            throws DbException
    {
        QueryDateUtil dateUtil = connection.dateUtil();
        Node person = Operators.findNode( connection.getTx(), Nodes.Person, Person.ID, operation.personId() );

        MinMaxPriorityQueue<LdbcQuery2PreResult> preResults = MinMaxPriorityQueue
                .orderedBy( DESCENDING_CREATION_DATE_ASCENDING_MESSAGE_ID_COMPARATOR )
                .maximumSize( operation.limit() )
                .create();

        long maxCreationDate = dateUtil.utcToFormat( operation.maxDate().getTime() );
        long leastRecentMessageCreationDate = Long.MIN_VALUE;
        boolean preResultSizeIsAtLimit = false;
        boolean leastRecentMessageCreationDateWasModified;

        Calendar calendar = connection.calendar();
        RelationshipType[] hasCreatorRelationshipTypes = null;

        Iterator<Relationship> knowsIterator = person.getRelationships( Rels.KNOWS ).iterator();

        while ( knowsIterator.hasNext() )
        {
            Node friend = knowsIterator.next().getOtherNode( person );
            for ( Relationship hasCreator : friend
                    .getRelationships( Direction.INCOMING, Rels.POST_HAS_CREATOR, Rels.COMMENT_HAS_CREATOR ) )
            {
                Node message = hasCreator.getStartNode();
                long messageCreationDate = (long) message.getProperty( Message.CREATION_DATE );
                if ( messageCreationDate <= maxCreationDate && messageCreationDate >= leastRecentMessageCreationDate )
                {
                    preResults.add(
                            new LdbcQuery2PreResult( friend, message, messageCreationDate )
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

        while ( knowsIterator.hasNext() )
        {
            Node friend = knowsIterator.next().getOtherNode( person );
            leastRecentMessageCreationDateWasModified = false;
            for ( Relationship hasCreator : friend.getRelationships( Direction.INCOMING, hasCreatorRelationshipTypes ) )
            {
                Node message = hasCreator.getStartNode();
                long messageCreationDate = (long) message.getProperty( Message.CREATION_DATE );
                if ( messageCreationDate <= maxCreationDate && messageCreationDate >= leastRecentMessageCreationDate )
                {
                    preResults.add(
                            new LdbcQuery2PreResult( friend, message, messageCreationDate )
                    );
                    leastRecentMessageCreationDate = preResults.peekLast().messageCreationDate();
                    leastRecentMessageCreationDateWasModified = true;
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

        List<LdbcQuery2Result> results = new ArrayList<>();
        LdbcQuery2PreResult preResult;
        while ( null != (preResult = preResults.poll()) )
        {
            results.add(
                    new LdbcQuery2Result(
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

    private static class DescendingCreationDateAscendingMessageIdComparator implements Comparator<LdbcQuery2PreResult>
    {
        @Override
        public int compare( LdbcQuery2PreResult result1, LdbcQuery2PreResult result2 )
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

    private static class LdbcQuery2PreResult
    {
        private final Node person;
        private final Node message;
        private final long messageCreationDate;
        private long personId = -1;
        private String personFirstName;
        private String personLastName;
        private long messageId = -1;
        private String messageContent;

        private LdbcQuery2PreResult( Node person, Node message, long messageCreationDate )
        {
            this.person = person;
            this.message = message;
            this.messageCreationDate = messageCreationDate;
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
                messageContent = (message.hasProperty( Message.CONTENT ))
                                 ? (String) message.getProperty( Message.CONTENT )
                                 : (String) message.getProperty( Post.IMAGE_FILE );
            }
            return messageContent;
        }
    }
}
