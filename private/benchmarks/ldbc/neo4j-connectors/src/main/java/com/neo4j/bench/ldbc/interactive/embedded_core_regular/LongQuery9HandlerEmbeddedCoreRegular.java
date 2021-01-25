/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.ldbc.interactive.embedded_core_regular;

import com.ldbc.driver.DbException;
import com.ldbc.driver.OperationHandler;
import com.ldbc.driver.ResultReporter;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery9;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery9Result;
import com.neo4j.bench.ldbc.Retries;
import com.neo4j.bench.ldbc.connection.Neo4jConnectionState;
import com.neo4j.bench.ldbc.interactive.Neo4jQuery9;
import com.neo4j.bench.ldbc.interactive.embedded_core.LongQuery9EmbeddedCore_0;

import java.util.List;

public class LongQuery9HandlerEmbeddedCoreRegular implements OperationHandler<LdbcQuery9,Neo4jConnectionState>
{
    private static final Neo4jQuery9<Neo4jConnectionState> QUERY = new LongQuery9EmbeddedCore_0();

    @Override
    public void executeOperation( LdbcQuery9 operation, Neo4jConnectionState dbConnectionState,
            ResultReporter resultReporter ) throws DbException
    {
        Retries<LdbcQuery9,List<LdbcQuery9Result>>
                retries = Retries.runInTransaction( QUERY, operation, dbConnectionState );
        resultReporter.report( retries.encodedResultsCode(), retries.result(), operation );
    }
}
