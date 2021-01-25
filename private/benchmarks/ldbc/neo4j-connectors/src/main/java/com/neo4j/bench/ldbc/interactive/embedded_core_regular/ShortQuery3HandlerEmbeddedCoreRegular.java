/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.ldbc.interactive.embedded_core_regular;

import com.ldbc.driver.DbException;
import com.ldbc.driver.OperationHandler;
import com.ldbc.driver.ResultReporter;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcShortQuery3PersonFriends;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcShortQuery3PersonFriendsResult;
import com.neo4j.bench.ldbc.Retries;
import com.neo4j.bench.ldbc.connection.Neo4jConnectionState;
import com.neo4j.bench.ldbc.interactive.Neo4jShortQuery3;
import com.neo4j.bench.ldbc.interactive.embedded_core.ShortQuery3EmbeddedCore_0_1_2;

import java.util.List;

import static com.neo4j.bench.ldbc.Retries.runInTransaction;

public class ShortQuery3HandlerEmbeddedCoreRegular
        implements OperationHandler<LdbcShortQuery3PersonFriends,Neo4jConnectionState>
{
    private static final Neo4jShortQuery3<Neo4jConnectionState> QUERY = new ShortQuery3EmbeddedCore_0_1_2();

    @Override
    public void executeOperation( LdbcShortQuery3PersonFriends operation, Neo4jConnectionState dbConnectionState,
            ResultReporter resultReporter ) throws DbException
    {
        Retries<LdbcShortQuery3PersonFriends,List<LdbcShortQuery3PersonFriendsResult>> retries = runInTransaction(
                QUERY, operation,
                dbConnectionState
        );
        resultReporter.report( retries.encodedResultsCode(), retries.result(), operation );
    }
}
