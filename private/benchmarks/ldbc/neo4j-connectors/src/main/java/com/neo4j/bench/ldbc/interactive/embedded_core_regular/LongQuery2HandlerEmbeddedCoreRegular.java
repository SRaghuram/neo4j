/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.ldbc.interactive.embedded_core_regular;

import com.ldbc.driver.DbException;
import com.ldbc.driver.OperationHandler;
import com.ldbc.driver.ResultReporter;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery2;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery2Result;
import com.neo4j.bench.ldbc.Retries;
import com.neo4j.bench.ldbc.connection.Neo4jConnectionState;
import com.neo4j.bench.ldbc.interactive.Neo4jQuery2;
import com.neo4j.bench.ldbc.interactive.embedded_core.LongQuery2EmbeddedCore_0;

import java.util.List;

import static com.neo4j.bench.ldbc.Retries.runInTransaction;

public class LongQuery2HandlerEmbeddedCoreRegular implements OperationHandler<LdbcQuery2,Neo4jConnectionState>
{
    private static final Neo4jQuery2<Neo4jConnectionState> QUERY = new LongQuery2EmbeddedCore_0();

    @Override
    public void executeOperation( LdbcQuery2 operation, Neo4jConnectionState dbConnectionState,
            ResultReporter resultReporter ) throws DbException
    {
        Retries<LdbcQuery2,List<LdbcQuery2Result>> retries = runInTransaction( QUERY, operation, dbConnectionState );
        resultReporter.report( retries.encodedResultsCode(), retries.result(), operation );
    }
}
