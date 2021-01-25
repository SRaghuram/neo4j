/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.ldbc.interactive.embedded_core;

import com.ldbc.driver.DbException;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcNoResult;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcUpdate7AddComment;
import com.neo4j.bench.ldbc.Domain.Message;
import com.neo4j.bench.ldbc.Domain.Nodes;
import com.neo4j.bench.ldbc.Domain.Person;
import com.neo4j.bench.ldbc.Domain.Place;
import com.neo4j.bench.ldbc.Domain.Rels;
import com.neo4j.bench.ldbc.Domain.Tag;
import com.neo4j.bench.ldbc.connection.Neo4jConnectionState;
import com.neo4j.bench.ldbc.connection.QueryDateUtil;
import com.neo4j.bench.ldbc.interactive.Neo4jUpdate7;
import com.neo4j.bench.ldbc.operators.Operators;

import org.neo4j.graphdb.Node;

public class Update7EmbeddedCore_0 extends Neo4jUpdate7<Neo4jConnectionState>
{
    @Override
    public LdbcNoResult execute( Neo4jConnectionState connection, LdbcUpdate7AddComment operation )
            throws DbException
    {
        QueryDateUtil dateUtil = connection.dateUtil();
        Node comment = connection.getTx().createNode( Nodes.Comment, Nodes.Message );
        comment.setProperty( Message.ID, operation.commentId() );
        comment.setProperty( Message.CREATION_DATE, dateUtil.utcToFormat( operation.creationDate().getTime() ) );
        comment.setProperty( Message.LOCATION_IP, operation.locationIp() );
        comment.setProperty( Message.BROWSER_USED, operation.browserUsed() );
        comment.setProperty( Message.CONTENT, operation.content() );
        comment.setProperty( Message.LENGTH, operation.length() );

        Node person = Operators.findNode( connection.getTx(), Nodes.Person, Person.ID, operation.authorPersonId() );
        comment.createRelationshipTo( person, Rels.COMMENT_HAS_CREATOR );

        if ( -1 == operation.replyToPostId() )
        {
            Node replyToComment = Operators.findNode( connection.getTx(), Nodes.Message,
                    Message.ID,
                    operation.replyToCommentId() );
            comment.createRelationshipTo( replyToComment, Rels.REPLY_OF_COMMENT );
        }
        else
        {
            Node replyToPost = Operators.findNode( connection.getTx(), Nodes.Message,
                    Message.ID,
                    operation.replyToPostId() );
            comment.createRelationshipTo( replyToPost, Rels.REPLY_OF_POST );
        }

        Node country = Operators.findNode( connection.getTx(), Place.Type.Country, Place.ID, operation.countryId() );
        comment.createRelationshipTo( country, Rels.COMMENT_IS_LOCATED_IN );

        for ( Long tagId : operation.tagIds() )
        {
            Node tag = Operators.findNode( connection.getTx(), Nodes.Tag, Tag.ID, tagId );
            comment.createRelationshipTo( tag, Rels.COMMENT_HAS_TAG );
        }

        return LdbcNoResult.INSTANCE;
    }
}
