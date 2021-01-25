/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.ldbc.interactive.embedded_core;

import com.ldbc.driver.DbException;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcNoResult;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcUpdate6AddPost;
import com.neo4j.bench.ldbc.Domain.Forum;
import com.neo4j.bench.ldbc.Domain.Message;
import com.neo4j.bench.ldbc.Domain.Nodes;
import com.neo4j.bench.ldbc.Domain.Person;
import com.neo4j.bench.ldbc.Domain.Place;
import com.neo4j.bench.ldbc.Domain.Post;
import com.neo4j.bench.ldbc.Domain.Rels;
import com.neo4j.bench.ldbc.Domain.Tag;
import com.neo4j.bench.ldbc.connection.Neo4jConnectionState;
import com.neo4j.bench.ldbc.connection.QueryDateUtil;
import com.neo4j.bench.ldbc.interactive.Neo4jUpdate6;
import com.neo4j.bench.ldbc.operators.Operators;

import org.neo4j.graphdb.Node;

public class Update6EmbeddedCore_0 extends Neo4jUpdate6<Neo4jConnectionState>
{
    @Override
    public LdbcNoResult execute( Neo4jConnectionState connection, LdbcUpdate6AddPost operation ) throws DbException
    {
        QueryDateUtil dateUtil = connection.dateUtil();
        Node post = connection.getTx().createNode( Nodes.Post, Nodes.Message );
        post.setProperty( Message.ID, operation.postId() );
        if ( !operation.imageFile().isEmpty() )
        {
            post.setProperty( Post.IMAGE_FILE, operation.imageFile() );
        }
        post.setProperty( Message.CREATION_DATE, dateUtil.utcToFormat( operation.creationDate().getTime() ) );
        post.setProperty( Message.LOCATION_IP, operation.locationIp() );
        post.setProperty( Message.BROWSER_USED, operation.browserUsed() );
        post.setProperty( Post.LANGUAGE, operation.language() );
        if ( !operation.content().isEmpty() )
        {
            post.setProperty( Message.CONTENT, operation.content() );
        }
        post.setProperty( Message.LENGTH, operation.length() );

        Node person = Operators.findNode( connection.getTx(), Nodes.Person, Person.ID, operation.authorPersonId() );
        post.createRelationshipTo( person, Rels.POST_HAS_CREATOR );

        Node forum = Operators.findNode( connection.getTx(), Nodes.Forum, Forum.ID, operation.forumId() );
        forum.createRelationshipTo( post, Rels.CONTAINER_OF );

        Node country = Operators.findNode( connection.getTx(), Place.Type.Country, Place.ID, operation.countryId() );
        post.createRelationshipTo( country, Rels.POST_IS_LOCATED_IN );

        for ( Long tagId : operation.tagIds() )
        {
            Node tag = Operators.findNode( connection.getTx(), Nodes.Tag, Tag.ID, tagId );
            post.createRelationshipTo( tag, Rels.POST_HAS_TAG );
        }

        return LdbcNoResult.INSTANCE;
    }
}
