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

package com.neo4j.bench.ldbc.interactive.embedded_cypher_regular;

import com.ldbc.driver.DbException;
import com.ldbc.driver.OperationHandler;
import com.ldbc.driver.ResultReporter;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcShortQuery2PersonPosts;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcShortQuery2PersonPostsResult;
import com.neo4j.bench.ldbc.Retries;
import com.neo4j.bench.ldbc.connection.Neo4jConnectionState;

import java.util.List;

import static com.neo4j.bench.ldbc.Retries.runInTransaction;

public class LdbcShortQuery2HandlerEmbeddedCypher
        implements OperationHandler<LdbcShortQuery2PersonPosts,Neo4jConnectionState>
{
    private static final Neo4jShortQuery2EmbeddedCypher QUERY = new Neo4jShortQuery2EmbeddedCypher();

    @Override
    public void executeOperation( LdbcShortQuery2PersonPosts operation, Neo4jConnectionState dbConnectionState,
            ResultReporter resultReporter ) throws DbException
    {
        Retries<LdbcShortQuery2PersonPosts,List<LdbcShortQuery2PersonPostsResult>> retries = runInTransaction(
                QUERY,
                operation,
                dbConnectionState );
        resultReporter.report( retries.encodedResultsCode(), retries.result(), operation );
    }
}
