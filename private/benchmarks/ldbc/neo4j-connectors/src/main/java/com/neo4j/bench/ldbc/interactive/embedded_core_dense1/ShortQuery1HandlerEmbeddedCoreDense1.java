/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.ldbc.interactive.embedded_core_dense1;

import com.ldbc.driver.DbException;
import com.ldbc.driver.OperationHandler;
import com.ldbc.driver.ResultReporter;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcShortQuery1PersonProfile;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcShortQuery1PersonProfileResult;
import com.neo4j.bench.ldbc.Retries;
import com.neo4j.bench.ldbc.connection.Neo4jConnectionState;
import com.neo4j.bench.ldbc.interactive.Neo4jShortQuery1;
import com.neo4j.bench.ldbc.interactive.embedded_core.ShortQuery1EmbeddedCore_0_1_2;

public class ShortQuery1HandlerEmbeddedCoreDense1
        implements OperationHandler<LdbcShortQuery1PersonProfile,Neo4jConnectionState>
{
    private static final Neo4jShortQuery1<Neo4jConnectionState> QUERY = new ShortQuery1EmbeddedCore_0_1_2();

    @Override
    public void executeOperation( LdbcShortQuery1PersonProfile operation, Neo4jConnectionState dbConnectionState,
            ResultReporter resultReporter ) throws DbException
    {
        Retries<LdbcShortQuery1PersonProfile,LdbcShortQuery1PersonProfileResult> retries = Retries.runInTransaction(
                QUERY,
                operation,
                dbConnectionState );
        resultReporter.report( retries.encodedResultsCode(), retries.result(), operation );
    }
}
