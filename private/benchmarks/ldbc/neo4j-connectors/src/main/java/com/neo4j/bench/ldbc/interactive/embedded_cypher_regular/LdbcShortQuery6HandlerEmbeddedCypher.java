/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.ldbc.interactive.embedded_cypher_regular;

import com.ldbc.driver.DbException;
import com.ldbc.driver.OperationHandler;
import com.ldbc.driver.ResultReporter;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcShortQuery6MessageForum;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcShortQuery6MessageForumResult;
import com.neo4j.bench.ldbc.Retries;
import com.neo4j.bench.ldbc.connection.Neo4jConnectionState;

public class LdbcShortQuery6HandlerEmbeddedCypher
        implements OperationHandler<LdbcShortQuery6MessageForum,Neo4jConnectionState>
{
    private static final Neo4jShortQuery6EmbeddedCypher QUERY = new Neo4jShortQuery6EmbeddedCypher();

    @Override
    public void executeOperation( LdbcShortQuery6MessageForum operation, Neo4jConnectionState dbConnectionState,
            ResultReporter resultReporter ) throws DbException
    {
        Retries<LdbcShortQuery6MessageForum,LdbcShortQuery6MessageForumResult> retries = Retries.runInTransaction(
                QUERY,
                operation,
                dbConnectionState );
        resultReporter.report( retries.encodedResultsCode(), retries.result(), operation );
    }
}
