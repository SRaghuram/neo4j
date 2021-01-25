/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.ldbc.interactive.remote_cypher_regular;

import com.ldbc.driver.DbException;
import com.ldbc.driver.OperationHandler;
import com.ldbc.driver.ResultReporter;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcShortQuery7MessageReplies;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcShortQuery7MessageRepliesResult;
import com.neo4j.bench.ldbc.Retries;
import com.neo4j.bench.ldbc.connection.Neo4jConnectionState;

import java.util.List;

import static com.neo4j.bench.ldbc.Retries.run;

public class LdbcShortQuery7HandlerRemoteCypher
        implements OperationHandler<LdbcShortQuery7MessageReplies,Neo4jConnectionState>
{
    private static final Neo4jShortQuery7RemoteCypher QUERY = new Neo4jShortQuery7RemoteCypher();

    @Override
    public void executeOperation(
            LdbcShortQuery7MessageReplies operation,
            Neo4jConnectionState dbConnectionState,
            ResultReporter resultReporter ) throws DbException
    {
        Retries<LdbcShortQuery7MessageReplies,List<LdbcShortQuery7MessageRepliesResult>> retries = run(
                QUERY,
                operation,
                dbConnectionState
        );
        resultReporter.report( retries.encodedResultsCode(), retries.result(), operation );
    }
}
