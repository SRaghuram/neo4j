/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.ldbc.interactive.remote_cypher_regular;

import com.ldbc.driver.DbException;
import com.ldbc.driver.OperationHandler;
import com.ldbc.driver.ResultReporter;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcShortQuery2PersonPosts;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcShortQuery2PersonPostsResult;
import com.neo4j.bench.ldbc.Retries;
import com.neo4j.bench.ldbc.connection.Neo4jConnectionState;

import java.util.List;

import static com.neo4j.bench.ldbc.Retries.run;

public class LdbcShortQuery2HandlerRemoteCypher
        implements OperationHandler<LdbcShortQuery2PersonPosts,Neo4jConnectionState>
{
    private static final Neo4jShortQuery2RemoteCypher QUERY = new Neo4jShortQuery2RemoteCypher();

    @Override
    public void executeOperation(
            LdbcShortQuery2PersonPosts operation,
            Neo4jConnectionState dbConnectionState,
            ResultReporter resultReporter ) throws DbException
    {
        Retries<LdbcShortQuery2PersonPosts,List<LdbcShortQuery2PersonPostsResult>> retries = run(
                QUERY,
                operation,
                dbConnectionState
        );
        resultReporter.report( retries.encodedResultsCode(), retries.result(), operation );
    }
}
