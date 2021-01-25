/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.ldbc.interactive.embedded_core_regular;

import com.ldbc.driver.DbException;
import com.ldbc.driver.OperationHandler;
import com.ldbc.driver.ResultReporter;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery3;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery3Result;
import com.neo4j.bench.ldbc.Retries;
import com.neo4j.bench.ldbc.connection.Neo4jConnectionState;
import com.neo4j.bench.ldbc.interactive.Neo4jQuery3;
import com.neo4j.bench.ldbc.interactive.embedded_core.LongQuery3EmbeddedCore_0;

import java.util.List;

import static com.neo4j.bench.ldbc.Retries.runInTransaction;

public class LongQuery3HandlerEmbeddedCoreRegular implements OperationHandler<LdbcQuery3,Neo4jConnectionState>
{
    private static final Neo4jQuery3<Neo4jConnectionState> QUERY = new LongQuery3EmbeddedCore_0();

    @Override
    public void executeOperation( LdbcQuery3 operation, Neo4jConnectionState dbConnectionState,
            ResultReporter resultReporter ) throws DbException
    {
        Retries<LdbcQuery3,List<LdbcQuery3Result>> retries = runInTransaction( QUERY, operation, dbConnectionState );
        resultReporter.report( retries.encodedResultsCode(), retries.result(), operation );
    }
}
