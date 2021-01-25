/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.ldbc.interactive.embedded_core;

import com.ldbc.driver.DbException;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcNoResult;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcUpdate4AddForum;
import com.neo4j.bench.ldbc.Domain.Forum;
import com.neo4j.bench.ldbc.Domain.Nodes;
import com.neo4j.bench.ldbc.Domain.Person;
import com.neo4j.bench.ldbc.Domain.Rels;
import com.neo4j.bench.ldbc.Domain.Tag;
import com.neo4j.bench.ldbc.connection.Neo4jConnectionState;
import com.neo4j.bench.ldbc.connection.QueryDateUtil;
import com.neo4j.bench.ldbc.interactive.Neo4jUpdate4;
import com.neo4j.bench.ldbc.operators.Operators;

import org.neo4j.graphdb.Node;

public class Update4EmbeddedCore_0_1_2 extends Neo4jUpdate4<Neo4jConnectionState>
{
    @Override
    public LdbcNoResult execute( Neo4jConnectionState connection, LdbcUpdate4AddForum operation )
            throws DbException
    {
        QueryDateUtil dateUtil = connection.dateUtil();
        Node forum = connection.getTx().createNode( Nodes.Forum );
        forum.setProperty( Forum.ID, operation.forumId() );
        forum.setProperty( Forum.TITLE, operation.forumTitle() );
        forum.setProperty( Forum.CREATION_DATE, dateUtil.utcToFormat( operation.creationDate().getTime() ) );
        Node person = Operators.findNode( connection.getTx(), Nodes.Person, Person.ID, operation.moderatorPersonId() );
        forum.createRelationshipTo( person, Rels.HAS_MODERATOR );
        for ( Long tagId : operation.tagIds() )
        {
            Node tag = Operators.findNode( connection.getTx(), Nodes.Tag, Tag.ID, tagId );
            forum.createRelationshipTo( tag, Rels.FORUM_HAS_TAG );
        }
        return LdbcNoResult.INSTANCE;
    }
}
