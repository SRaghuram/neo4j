/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.ldbc.interactive.embedded_core;

import com.ldbc.driver.DbException;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcShortQuery6MessageForum;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcShortQuery6MessageForumResult;
import com.neo4j.bench.ldbc.Domain.Forum;
import com.neo4j.bench.ldbc.Domain.Message;
import com.neo4j.bench.ldbc.Domain.Nodes;
import com.neo4j.bench.ldbc.Domain.Person;
import com.neo4j.bench.ldbc.Domain.Rels;
import com.neo4j.bench.ldbc.connection.Neo4jConnectionState;
import com.neo4j.bench.ldbc.interactive.Neo4jShortQuery6;
import com.neo4j.bench.ldbc.operators.Operators;

import java.util.Map;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.ResourceIterator;

public class ShortQuery6EmbeddedCore_0_1_2 extends Neo4jShortQuery6<Neo4jConnectionState>
{
    private static final String[] PERSON_PROPERTIES = new String[]{
            Person.ID,
            Person.FIRST_NAME,
            Person.LAST_NAME};

    @Override
    public LdbcShortQuery6MessageForumResult execute( Neo4jConnectionState connection,
            LdbcShortQuery6MessageForum operation ) throws DbException
    {
        Node message = Operators.findNode( connection.getTx(), Nodes.Message, Message.ID, operation.messageId() );
        Node post;
        if ( message.hasLabel( Nodes.Comment ) )
        {
            post = getParentPostOfComment( message );
        }
        else
        {
            post = message;
        }
        Node forum = post.getSingleRelationship( Rels.CONTAINER_OF, Direction.INCOMING ).getStartNode();
        Node moderator = forum.getSingleRelationship( Rels.HAS_MODERATOR, Direction.OUTGOING ).getEndNode();
        Map<String,Object> moderatorProperties = moderator.getProperties( PERSON_PROPERTIES );
        return new LdbcShortQuery6MessageForumResult(
                (long) forum.getProperty( Forum.ID ),
                (String) forum.getProperty( Forum.TITLE ),
                (long) moderatorProperties.get( Person.ID ),
                (String) moderatorProperties.get( Person.FIRST_NAME ),
                (String) moderatorProperties.get( Person.LAST_NAME )
        );
    }

    Node getParentPostOfComment( Node message )
    {
        try ( ResourceIterator<Relationship> replyOfRels = (ResourceIterator<Relationship>) message.getRelationships(
                Direction.OUTGOING,
                Rels.REPLY_OF_COMMENT,
                Rels.REPLY_OF_POST ).iterator() )
        {
            Relationship replyOf = replyOfRels.next();
            while ( replyOf.isType( Rels.REPLY_OF_COMMENT ) )
            {
                try ( ResourceIterator<Relationship> nextReplyOfRels = (ResourceIterator<Relationship>) replyOf.getEndNode().getRelationships(
                        Direction.OUTGOING,
                        Rels.REPLY_OF_COMMENT,
                        Rels.REPLY_OF_POST ).iterator() )
                {
                    replyOf = nextReplyOfRels.next();
                }
            }
            return replyOf.getEndNode();
        }
    }
}
