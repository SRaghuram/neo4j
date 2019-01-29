/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
 *
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
        Node person1 = Operators.findNode( connection.db(), Nodes.Person, Person.ID, operation.person1Id() );
        Node person2 = Operators.findNode( connection.db(), Nodes.Person, Person.ID, operation.person2Id() );
        PathFinder<Path> finder = GraphAlgoFactory
                .shortestPath( PathExpanders.forTypeAndDirection( Rels.KNOWS, Direction.BOTH ), Integer.MAX_VALUE );
        Path shortestPath = finder.findSinglePath( person1, person2 );
        return (null == shortestPath) ? NO_PATH : new LdbcQuery13Result( shortestPath.length() );
    }
}
