/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.ldbc.interactive.embedded_core;

import com.ldbc.driver.DbException;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcNoResult;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcUpdate3AddCommentLike;
import com.neo4j.bench.ldbc.Domain.Likes;
import com.neo4j.bench.ldbc.Domain.Message;
import com.neo4j.bench.ldbc.Domain.Nodes;
import com.neo4j.bench.ldbc.Domain.Person;
import com.neo4j.bench.ldbc.Domain.Rels;
import com.neo4j.bench.ldbc.connection.Neo4jConnectionState;
import com.neo4j.bench.ldbc.connection.QueryDateUtil;
import com.neo4j.bench.ldbc.interactive.Neo4jUpdate3;
import com.neo4j.bench.ldbc.operators.Operators;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;

public class Update3EmbeddedCore_0_1_2 extends Neo4jUpdate3<Neo4jConnectionState>
{
    @Override
    public LdbcNoResult execute( Neo4jConnectionState connection, LdbcUpdate3AddCommentLike operation )
            throws DbException
    {
        QueryDateUtil dateUtil = connection.dateUtil();
        Node person = Operators.findNode( connection.getTx(), Nodes.Person, Person.ID, operation.personId() );
        Node comment = Operators.findNode( connection.getTx(), Nodes.Message, Message.ID, operation.commentId() );
        Relationship like = person.createRelationshipTo( comment, Rels.LIKES_COMMENT );
        like.setProperty( Likes.CREATION_DATE, dateUtil.utcToFormat( operation.creationDate().getTime() ) );
        return LdbcNoResult.INSTANCE;
    }
}
