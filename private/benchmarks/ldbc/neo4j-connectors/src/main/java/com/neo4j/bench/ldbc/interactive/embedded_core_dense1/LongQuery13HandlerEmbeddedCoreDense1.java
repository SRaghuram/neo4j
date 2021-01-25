/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.ldbc.interactive.embedded_core_dense1;

import com.ldbc.driver.DbException;
import com.ldbc.driver.OperationHandler;
import com.ldbc.driver.ResultReporter;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery13;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery13Result;
import com.neo4j.bench.ldbc.Retries;
import com.neo4j.bench.ldbc.connection.Neo4jConnectionState;
import com.neo4j.bench.ldbc.interactive.Neo4jQuery13;
import com.neo4j.bench.ldbc.interactive.embedded_core.LongQuery13EmbeddedCore_0_1;

public class LongQuery13HandlerEmbeddedCoreDense1
        implements OperationHandler<LdbcQuery13,Neo4jConnectionState>
{
    private static final Neo4jQuery13<Neo4jConnectionState> QUERY = new LongQuery13EmbeddedCore_0_1();

    @Override
    public void executeOperation( LdbcQuery13 operation, Neo4jConnectionState dbConnectionState,
            ResultReporter resultReporter ) throws DbException
    {
        Retries<LdbcQuery13,LdbcQuery13Result> retries =
                Retries.runInTransaction( QUERY, operation, dbConnectionState );
        resultReporter.report( retries.encodedResultsCode(), retries.result(), operation );
    }
}
