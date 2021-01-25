/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.ldbc.interactive.embedded_cypher_regular;

import com.ldbc.driver.DbException;
import com.ldbc.driver.OperationHandler;
import com.ldbc.driver.ResultReporter;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcShortQuery5MessageCreator;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcShortQuery5MessageCreatorResult;
import com.neo4j.bench.ldbc.Retries;
import com.neo4j.bench.ldbc.connection.Neo4jConnectionState;

public class LdbcShortQuery5HandlerEmbeddedCypher
        implements OperationHandler<LdbcShortQuery5MessageCreator,Neo4jConnectionState>
{
    private static final Neo4jShortQuery5EmbeddedCypher QUERY = new Neo4jShortQuery5EmbeddedCypher();

    @Override
    public void executeOperation( LdbcShortQuery5MessageCreator operation, Neo4jConnectionState dbConnectionState,
            ResultReporter resultReporter ) throws DbException
    {
        Retries<LdbcShortQuery5MessageCreator,LdbcShortQuery5MessageCreatorResult> retries = Retries.runInTransaction(
                QUERY,
                operation,
                dbConnectionState );
        resultReporter.report( retries.encodedResultsCode(), retries.result(), operation );
    }
}
