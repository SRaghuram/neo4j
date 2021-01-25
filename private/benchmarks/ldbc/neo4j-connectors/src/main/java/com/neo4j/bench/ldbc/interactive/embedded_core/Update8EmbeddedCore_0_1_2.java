/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.ldbc.interactive.embedded_core;

import com.ldbc.driver.DbException;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcNoResult;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcUpdate8AddFriendship;
import com.neo4j.bench.ldbc.Domain.Knows;
import com.neo4j.bench.ldbc.Domain.Nodes;
import com.neo4j.bench.ldbc.Domain.Person;
import com.neo4j.bench.ldbc.Domain.Rels;
import com.neo4j.bench.ldbc.connection.Neo4jConnectionState;
import com.neo4j.bench.ldbc.connection.QueryDateUtil;
import com.neo4j.bench.ldbc.interactive.Neo4jUpdate8;
import com.neo4j.bench.ldbc.operators.Operators;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;

public class Update8EmbeddedCore_0_1_2 extends Neo4jUpdate8<Neo4jConnectionState>
{
    @Override
    public LdbcNoResult execute( Neo4jConnectionState connection, LdbcUpdate8AddFriendship operation )
            throws DbException
    {
        QueryDateUtil dateUtil = connection.dateUtil();
        Node person1 = Operators.findNode( connection.getTx(), Nodes.Person, Person.ID, operation.person1Id() );
        Node person2 = Operators.findNode( connection.getTx(), Nodes.Person, Person.ID, operation.person2Id() );
        Relationship knows = person1.createRelationshipTo( person2, Rels.KNOWS );
        knows.setProperty( Knows.CREATION_DATE, dateUtil.utcToFormat( operation.creationDate().getTime() ) );
        return LdbcNoResult.INSTANCE;
    }
}
