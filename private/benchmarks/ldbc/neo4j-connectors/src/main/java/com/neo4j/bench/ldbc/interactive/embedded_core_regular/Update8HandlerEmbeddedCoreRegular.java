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
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcUpdate8AddFriendship;
import com.neo4j.bench.ldbc.Retries;
import com.neo4j.bench.ldbc.connection.Neo4jConnectionState;
import com.neo4j.bench.ldbc.interactive.Neo4jUpdate8;
import com.neo4j.bench.ldbc.interactive.embedded_core.Update8EmbeddedCore_0_1_2;

public class Update8HandlerEmbeddedCoreRegular implements OperationHandler<LdbcUpdate8AddFriendship,Neo4jConnectionState>
{
    private static final Neo4jUpdate8<Neo4jConnectionState> QUERY = new Update8EmbeddedCore_0_1_2();

    @Override
    public void executeOperation( LdbcUpdate8AddFriendship operation, Neo4jConnectionState dbConnectionState,
            ResultReporter resultReporter ) throws DbException
    {
        Retries<LdbcUpdate8AddFriendship,LdbcNoResult> retries =
                Retries.runInTransaction( QUERY, operation, dbConnectionState );
        resultReporter.report( retries.encodedResultsCode(), retries.result(), operation );
    }
}
