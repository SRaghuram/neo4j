/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.ldbc.interactive.embedded_core_dense1;

import com.ldbc.driver.DbException;
import com.ldbc.driver.OperationHandler;
import com.ldbc.driver.ResultReporter;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery4;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery4Result;
import com.neo4j.bench.ldbc.Retries;
import com.neo4j.bench.ldbc.connection.Neo4jConnectionState;
import com.neo4j.bench.ldbc.interactive.Neo4jQuery4;
import com.neo4j.bench.ldbc.interactive.embedded_core.LongQuery4EmbeddedCore_0_1;

import java.util.List;

public class LongQuery4HandlerEmbeddedCoreDense1 implements OperationHandler<LdbcQuery4,Neo4jConnectionState>
{
    private static final Neo4jQuery4<Neo4jConnectionState> QUERY = new LongQuery4EmbeddedCore_0_1();

    @Override
    public void executeOperation( LdbcQuery4 operation, Neo4jConnectionState dbConnectionState,
            ResultReporter resultReporter ) throws DbException
    {
        Retries<LdbcQuery4,List<LdbcQuery4Result>>
                retries = Retries.runInTransaction( QUERY, operation, dbConnectionState );
        resultReporter.report( retries.encodedResultsCode(), retries.result(), operation );
    }
}
