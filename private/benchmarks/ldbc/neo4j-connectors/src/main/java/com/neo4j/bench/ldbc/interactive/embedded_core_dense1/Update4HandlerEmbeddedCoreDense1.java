/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.ldbc.interactive.embedded_core_dense1;

import com.ldbc.driver.DbException;
import com.ldbc.driver.OperationHandler;
import com.ldbc.driver.ResultReporter;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcNoResult;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcUpdate4AddForum;
import com.neo4j.bench.ldbc.Retries;
import com.neo4j.bench.ldbc.connection.Neo4jConnectionState;
import com.neo4j.bench.ldbc.interactive.Neo4jUpdate4;
import com.neo4j.bench.ldbc.interactive.embedded_core.Update4EmbeddedCore_0_1_2;

import static com.neo4j.bench.ldbc.Retries.runInTransaction;

public class Update4HandlerEmbeddedCoreDense1
        implements OperationHandler<LdbcUpdate4AddForum,Neo4jConnectionState>
{
    private static final Neo4jUpdate4<Neo4jConnectionState> QUERY = new Update4EmbeddedCore_0_1_2();

    @Override
    public void executeOperation( LdbcUpdate4AddForum operation, Neo4jConnectionState dbConnectionState,
            ResultReporter resultReporter ) throws DbException
    {
        Retries<LdbcUpdate4AddForum,LdbcNoResult> retries = runInTransaction( QUERY, operation, dbConnectionState );
        resultReporter.report( retries.encodedResultsCode(), retries.result(), operation );
    }
}
