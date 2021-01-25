/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.ldbc.interactive.embedded_core_regular;

import com.ldbc.driver.DbException;
import com.ldbc.driver.OperationHandler;
import com.ldbc.driver.ResultReporter;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery10;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery10Result;
import com.neo4j.bench.ldbc.Retries;
import com.neo4j.bench.ldbc.connection.Neo4jConnectionState;
import com.neo4j.bench.ldbc.interactive.Neo4jQuery10;
import com.neo4j.bench.ldbc.interactive.embedded_core.LongQuery10EmbeddedCore_0_1;

import java.util.List;

public class LongQuery10HandlerEmbeddedCoreRegular implements OperationHandler<LdbcQuery10,Neo4jConnectionState>
{
    private static final Neo4jQuery10<Neo4jConnectionState> QUERY = new LongQuery10EmbeddedCore_0_1();

    @Override
    public void executeOperation( LdbcQuery10 operation, Neo4jConnectionState dbConnectionState,
            ResultReporter resultReporter ) throws DbException
    {
        Retries<LdbcQuery10,List<LdbcQuery10Result>>
                retries = Retries.runInTransaction( QUERY, operation, dbConnectionState );
        resultReporter.report( retries.encodedResultsCode(), retries.result(), operation );
    }
}
