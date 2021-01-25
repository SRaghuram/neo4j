/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.ldbc.interactive.remote_cypher_regular;

import com.ldbc.driver.DbException;
import com.ldbc.driver.OperationHandler;
import com.ldbc.driver.ResultReporter;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcNoResult;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcUpdate3AddCommentLike;
import com.neo4j.bench.ldbc.Retries;
import com.neo4j.bench.ldbc.connection.Neo4jConnectionState;

import static com.neo4j.bench.ldbc.Retries.run;

public class LdbcUpdate3HandlerRemoteCypher implements OperationHandler<LdbcUpdate3AddCommentLike,Neo4jConnectionState>
{
    private static final Neo4jUpdate3RemoteCypher QUERY = new Neo4jUpdate3RemoteCypher();

    @Override
    public void executeOperation( LdbcUpdate3AddCommentLike operation, Neo4jConnectionState dbConnectionState,
            ResultReporter resultReporter ) throws DbException
    {
        Retries<LdbcUpdate3AddCommentLike,LdbcNoResult> retries = run( QUERY, operation, dbConnectionState );
        resultReporter.report( retries.encodedResultsCode(), retries.result(), operation );
    }
}
