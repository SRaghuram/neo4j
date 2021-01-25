/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.ldbc.interactive.embedded_core_regular;

import com.ldbc.driver.DbException;
import com.ldbc.driver.OperationHandler;
import com.ldbc.driver.ResultReporter;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery6;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery6Result;
import com.neo4j.bench.ldbc.Retries;
import com.neo4j.bench.ldbc.connection.Neo4jConnectionState;
import com.neo4j.bench.ldbc.interactive.Neo4jQuery6;
import com.neo4j.bench.ldbc.interactive.embedded_core.LongQuery6EmbeddedCore_0_1;

import java.util.List;

import static com.neo4j.bench.ldbc.Retries.runInTransaction;

public class LongQuery6HandlerEmbeddedCoreRegular implements OperationHandler<LdbcQuery6,Neo4jConnectionState>
{
    private static final Neo4jQuery6<Neo4jConnectionState> QUERY = new LongQuery6EmbeddedCore_0_1();

    @Override
    public void executeOperation( LdbcQuery6 operation, Neo4jConnectionState dbConnectionState,
            ResultReporter resultReporter ) throws DbException
    {
        Retries<LdbcQuery6,List<LdbcQuery6Result>> retries = runInTransaction( QUERY, operation, dbConnectionState );
        resultReporter.report( retries.encodedResultsCode(), retries.result(), operation );
    }
}
