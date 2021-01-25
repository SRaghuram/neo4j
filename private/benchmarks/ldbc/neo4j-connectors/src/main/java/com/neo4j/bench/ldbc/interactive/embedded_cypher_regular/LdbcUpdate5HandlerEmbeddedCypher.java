/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.ldbc.interactive.embedded_cypher_regular;

import com.ldbc.driver.DbException;
import com.ldbc.driver.OperationHandler;
import com.ldbc.driver.ResultReporter;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcNoResult;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcUpdate5AddForumMembership;
import com.neo4j.bench.ldbc.Retries;
import com.neo4j.bench.ldbc.connection.Neo4jConnectionState;

public class LdbcUpdate5HandlerEmbeddedCypher
        implements OperationHandler<LdbcUpdate5AddForumMembership,Neo4jConnectionState>
{
    private static final Neo4jUpdate5EmbeddedCypher QUERY = new Neo4jUpdate5EmbeddedCypher();

    @Override
    public void executeOperation( LdbcUpdate5AddForumMembership operation, Neo4jConnectionState dbConnectionState,
            ResultReporter resultReporter ) throws DbException
    {
        Retries<LdbcUpdate5AddForumMembership,LdbcNoResult> retries =
                Retries.runInTransaction( QUERY, operation, dbConnectionState );
        resultReporter.report( retries.encodedResultsCode(), retries.result(), operation );
    }
}
