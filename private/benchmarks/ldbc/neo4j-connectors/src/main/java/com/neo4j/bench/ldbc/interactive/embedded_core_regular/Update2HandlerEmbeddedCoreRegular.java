/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.ldbc.interactive.embedded_core_regular;

import com.ldbc.driver.DbException;
import com.ldbc.driver.OperationHandler;
import com.ldbc.driver.ResultReporter;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcNoResult;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcUpdate2AddPostLike;
import com.neo4j.bench.ldbc.Retries;
import com.neo4j.bench.ldbc.connection.Neo4jConnectionState;
import com.neo4j.bench.ldbc.interactive.Neo4jUpdate2;
import com.neo4j.bench.ldbc.interactive.embedded_core.Update2EmbeddedCore_0_1_2;

public class Update2HandlerEmbeddedCoreRegular implements OperationHandler<LdbcUpdate2AddPostLike,Neo4jConnectionState>
{
    private static final Neo4jUpdate2<Neo4jConnectionState> QUERY = new Update2EmbeddedCore_0_1_2();

    @Override
    public void executeOperation( LdbcUpdate2AddPostLike operation, Neo4jConnectionState dbConnectionState,
            ResultReporter resultReporter ) throws DbException
    {
        Retries<LdbcUpdate2AddPostLike,LdbcNoResult>
                retries = Retries.runInTransaction( QUERY, operation, dbConnectionState );
        resultReporter.report( retries.encodedResultsCode(), retries.result(), operation );
    }
}
