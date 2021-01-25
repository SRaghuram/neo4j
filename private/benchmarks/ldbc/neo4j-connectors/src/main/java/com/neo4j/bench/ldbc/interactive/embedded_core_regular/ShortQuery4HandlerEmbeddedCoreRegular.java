/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.ldbc.interactive.embedded_core_regular;

import com.ldbc.driver.DbException;
import com.ldbc.driver.OperationHandler;
import com.ldbc.driver.ResultReporter;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcShortQuery4MessageContent;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcShortQuery4MessageContentResult;
import com.neo4j.bench.ldbc.Retries;
import com.neo4j.bench.ldbc.connection.Neo4jConnectionState;
import com.neo4j.bench.ldbc.interactive.Neo4jShortQuery4;
import com.neo4j.bench.ldbc.interactive.embedded_core.ShortQuery4EmbeddedCore_0_1_2;

public class ShortQuery4HandlerEmbeddedCoreRegular
        implements OperationHandler<LdbcShortQuery4MessageContent,Neo4jConnectionState>
{
    private static final Neo4jShortQuery4<Neo4jConnectionState> QUERY = new ShortQuery4EmbeddedCore_0_1_2();

    @Override
    public void executeOperation( LdbcShortQuery4MessageContent operation, Neo4jConnectionState dbConnectionState,
            ResultReporter resultReporter ) throws DbException
    {
        Retries<LdbcShortQuery4MessageContent,LdbcShortQuery4MessageContentResult> retries = Retries.runInTransaction(
                QUERY, operation,
                dbConnectionState
        );
        resultReporter.report( retries.encodedResultsCode(), retries.result(), operation );
    }
}
