/**
 * Copyright (c) 2002-2019 "Neo4j,"
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
import com.neo4j.bench.ldbc.interactive.Neo4jQuery2;
import com.neo4j.bench.ldbc.operators.Operators;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;

public class LongQuery2EmbeddedCore_0 extends Neo4jQuery2<Neo4jConnectionState>
{
    private static final DescendingCreationDateAscendingMessageIdComparator
            DESCENDING_CREATION_DATE_ASCENDING_MESSAGE_ID_COMPARATOR =
            new DescendingCreationDateAscendingMessageIdComparator();

    @Override
    public List<LdbcQuery2Result> execute( Neo4jConnectionState connection, LdbcQuery2 operation )
            throws DbException
    {
        QueryDateUtil dateUtil = connection.dateUtil();
        Node person = Operators.findNode( connection.db(), Nodes.Person, Person.ID, operation.personId() );

        MinMaxPriorityQueue<LdbcQuery2PreResult> preResults = MinMaxPriorityQueue
                .orderedBy( DESCENDING_CREATION_DATE_ASCENDING_MESSAGE_ID_COMPARATOR )
                .maximumSize( operation.limit() )
                .create();

        long leastRecentPostCreationDate = Long.MIN_VALUE;

        long maxCreationDate = dateUtil.utcToFormat( operation.maxDate().getTime() );
        for ( Relationship knows : person.getRelationships( Rels.KNOWS ) )
        {
            Node friend = knows.getOtherNode( person );
            for ( Relationship hasCreator : friend
                    .getRelationships( Direction.INCOMING, Rels.POST_HAS_CREATOR, Rels.COMMENT_HAS_CREATOR ) )
            {
                Node message = hasCreator.getStartNode();
                long messageCreationDate = (long) message.getProperty( Message.CREATION_DATE );
                if ( messageCreationDate <= maxCreationDate && messageCreationDate >= leastRecentPostCreationDate )
                {
                    preResults.add(
                            new LdbcQuery2PreResult( friend, message, messageCreationDate )
                    );
                    if ( preResults.size() == operation.limit() )
                    {
                        leastRecentPostCreationDate = preResults.peekLast().messageCreationDate();
                    }
                }
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
