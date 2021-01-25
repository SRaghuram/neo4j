/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.ldbc.interactive.embedded_core_dense1;

import com.ldbc.driver.DbException;
import com.ldbc.driver.OperationHandler;
import com.ldbc.driver.ResultReporter;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcShortQuery2PersonPosts;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcShortQuery2PersonPostsResult;
import com.neo4j.bench.ldbc.Retries;
import com.neo4j.bench.ldbc.connection.Neo4jConnectionState;
import com.neo4j.bench.ldbc.interactive.Neo4jShortQuery2;
import com.neo4j.bench.ldbc.interactive.embedded_core.ShortQuery2EmbeddedCore_0_1;

import java.util.List;

public class ShortQuery2HandlerEmbeddedCoreDense1
        implements OperationHandler<LdbcShortQuery2PersonPosts,Neo4jConnectionState>
{
    private static final Neo4jShortQuery2<Neo4jConnectionState> QUERY = new ShortQuery2EmbeddedCore_0_1();

    @Override
    public void executeOperation( LdbcShortQuery2PersonPosts operation, Neo4jConnectionState dbConnectionState,
            ResultReporter resultReporter ) throws DbException
    {
        Retries<LdbcShortQuery2PersonPosts,List<LdbcShortQuery2PersonPostsResult>> retries = Retries.runInTransaction(
                QUERY,
                operation,
                dbConnectionState );
        resultReporter.report( retries.encodedResultsCode(), retries.result(), operation );
    }
}
