/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.ldbc.interactive.embedded_core;

import com.google.common.collect.MinMaxPriorityQueue;
import com.ldbc.driver.DbException;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery8;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery8Result;
import com.neo4j.bench.ldbc.Domain.Message;
import com.neo4j.bench.ldbc.Domain.Nodes;
import com.neo4j.bench.ldbc.Domain.Person;
import com.neo4j.bench.ldbc.Domain.Rels;
import com.neo4j.bench.ldbc.connection.Neo4jConnectionState;
import com.neo4j.bench.ldbc.connection.QueryDateUtil;
import com.neo4j.bench.ldbc.interactive.Neo4jQuery8;
import com.neo4j.bench.ldbc.operators.Operators;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;

public class LongQuery8EmbeddedCore_0_1 extends Neo4jQuery8<Neo4jConnectionState>
{
    @Override
    public List<LdbcQuery8Result> execute( Neo4jConnectionState connection, LdbcQuery8 operation )
            throws DbException
    {
        QueryDateUtil dateUtil = connection.dateUtil();
        Node person = Operators.findNode( connection.getTx(), Nodes.Person, Person.ID, operation.personId() );

        MinMaxPriorityQueue<Reply> replies = MinMaxPriorityQueue.maximumSize( operation.limit() ).create();

        for ( Relationship hasCreator : person
                .getRelationships( Direction.INCOMING, Rels.POST_HAS_CREATOR, Rels.COMMENT_HAS_CREATOR ) )
        {
            Node message = hasCreator.getStartNode();
            for ( Relationship replyOf : message
                    .getRelationships( Direction.INCOMING, Rels.REPLY_OF_POST, Rels.REPLY_OF_COMMENT ) )
            {
                Node reply = replyOf.getStartNode();
                replies.add( new Reply( reply ) );
            }
        }

        List<LdbcQuery8Result> results = new ArrayList<>();
        Reply reply;
        while ( null != (reply = replies.poll()) )
        {
            Node creator =
                    reply.node().getSingleRelationship( Rels.COMMENT_HAS_CREATOR, Direction.OUTGOING ).getEndNode();
            Map<String,Object> creatorProperties = creator.getProperties(
                    Person.ID,
                    Person.FIRST_NAME,
                    Person.LAST_NAME );
            long creatorId = (long) creatorProperties.get( Person.ID );
            String creatorFirstName = (String) creatorProperties.get( Person.FIRST_NAME );
            String creatorLastName = (String) creatorProperties.get( Person.LAST_NAME );
            LdbcQuery8Result result = new LdbcQuery8Result(
                    creatorId,
                    creatorFirstName,
                    creatorLastName,
                    dateUtil.formatToUtc( reply.creationDate() ),
                    reply.id(),
                    reply.content()
            );
            results.add( result );
        }

        return results;
    }

    private class Reply implements Comparable<Reply>
    {
        private final Node reply;
        private long id = -1;
        private long creationDate = -1;
        private String content;

        private Reply( Node reply )
        {
            this.reply = reply;
        }

        private Node node()
        {
            return reply;
        }

        private long id()
        {
            if ( -1 == id )
            {
                id = (long) reply.getProperty( Message.ID );
            }
            return id;
        }

        private long creationDate()
        {
            if ( -1 == creationDate )
            {
                creationDate = (long) reply.getProperty( Message.CREATION_DATE );
            }
            return creationDate;
        }

        private String content()
        {
            if ( null == content )
            {
                content = (String) reply.getProperty( Message.CONTENT );
            }
            return content;
        }

        @Override
        public int compareTo( Reply other )
        {
            if ( this.creationDate() > other.creationDate() )
            {
                return -1;
            }
            else if ( this.creationDate() < other.creationDate() )
            {
                return 1;
            }
            else
            {
                if ( this.id() < other.id() )
                {
                    return -1;
                }
                else if ( this.id() > other.id() )
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
