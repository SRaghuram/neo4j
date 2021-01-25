/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.ldbc.interactive.embedded_core;

import com.ldbc.driver.DbException;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery13;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery13Result;
import com.neo4j.bench.ldbc.Domain.Nodes;
import com.neo4j.bench.ldbc.Domain.Person;
import com.neo4j.bench.ldbc.Domain.Rels;
import com.neo4j.bench.ldbc.connection.Neo4jConnectionState;
import com.neo4j.bench.ldbc.interactive.Neo4jQuery13;
import com.neo4j.bench.ldbc.operators.Operators;

import org.neo4j.graphalgo.BasicEvaluationContext;
import org.neo4j.graphalgo.GraphAlgoFactory;
import org.neo4j.graphalgo.PathFinder;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.PathExpanders;

public class LongQuery13EmbeddedCore_0_1 extends Neo4jQuery13<Neo4jConnectionState>
{
    private static final LdbcQuery13Result NO_PATH = new LdbcQuery13Result( -1 );

    @Override
    public LdbcQuery13Result execute( Neo4jConnectionState connection, LdbcQuery13 operation ) throws DbException
    {
        Node person1 = Operators.findNode( connection.getTx(), Nodes.Person, Person.ID, operation.person1Id() );
        Node person2 = Operators.findNode( connection.getTx(), Nodes.Person, Person.ID, operation.person2Id() );
        var context = new BasicEvaluationContext( connection.getTx(), connection.getDb() );
        PathFinder<Path> finder = GraphAlgoFactory
                .shortestPath( context, PathExpanders.forTypeAndDirection( Rels.KNOWS, Direction.BOTH ), Integer.MAX_VALUE );
        Path shortestPath = finder.findSinglePath( person1, person2 );
        return (null == shortestPath) ? NO_PATH : new LdbcQuery13Result( shortestPath.length() );
    }
}
