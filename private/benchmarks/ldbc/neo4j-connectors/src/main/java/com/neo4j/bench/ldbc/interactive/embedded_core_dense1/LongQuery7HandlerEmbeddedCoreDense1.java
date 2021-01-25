/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.ldbc.interactive.embedded_core_dense1;

import com.ldbc.driver.DbException;
import com.ldbc.driver.OperationHandler;
import com.ldbc.driver.ResultReporter;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery7;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery7Result;
import com.neo4j.bench.ldbc.Retries;
import com.neo4j.bench.ldbc.connection.Neo4jConnectionState;
import com.neo4j.bench.ldbc.interactive.Neo4jQuery7;
import com.neo4j.bench.ldbc.interactive.embedded_core.LongQuery7EmbeddedCore_0_1;

import java.util.List;

public class LongQuery7HandlerEmbeddedCoreDense1 implements OperationHandler<LdbcQuery7,Neo4jConnectionState>
{
    private static final Neo4jQuery7<Neo4jConnectionState> QUERY = new LongQuery7EmbeddedCore_0_1();

    @Override
    public void executeOperation( LdbcQuery7 operation, Neo4jConnectionState dbConnectionState,
            ResultReporter resultReporter ) throws DbException
    {
        Retries<LdbcQuery7,List<LdbcQuery7Result>>
                retries = Retries.runInTransaction( QUERY, operation, dbConnectionState );
        resultReporter.report( retries.encodedResultsCode(), retries.result(), operation );
    }
}
