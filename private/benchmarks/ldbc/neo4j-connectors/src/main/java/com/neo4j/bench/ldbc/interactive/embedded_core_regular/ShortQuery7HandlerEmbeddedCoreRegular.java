/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.ldbc.interactive.embedded_core_regular;

import com.ldbc.driver.DbException;
import com.ldbc.driver.OperationHandler;
import com.ldbc.driver.ResultReporter;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcShortQuery7MessageReplies;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcShortQuery7MessageRepliesResult;
import com.neo4j.bench.ldbc.Retries;
import com.neo4j.bench.ldbc.connection.Neo4jConnectionState;
import com.neo4j.bench.ldbc.interactive.Neo4jShortQuery7;
import com.neo4j.bench.ldbc.interactive.embedded_core.ShortQuery7EmbeddedCore_0_1;

import java.util.List;

public class ShortQuery7HandlerEmbeddedCoreRegular
        implements OperationHandler<LdbcShortQuery7MessageReplies,Neo4jConnectionState>
{
    private static final Neo4jShortQuery7<Neo4jConnectionState> QUERY = new ShortQuery7EmbeddedCore_0_1();

    @Override
    public void executeOperation( LdbcShortQuery7MessageReplies operation, Neo4jConnectionState dbConnectionState,
            ResultReporter resultReporter ) throws DbException
    {
        Retries<LdbcShortQuery7MessageReplies,List<LdbcShortQuery7MessageRepliesResult>>
                retries = Retries.runInTransaction(
                QUERY,
                operation,
                dbConnectionState
        );
        resultReporter.report( retries.encodedResultsCode(), retries.result(), operation );
    }
}
