/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.ldbc.business_intelligence.queries.embedded_cypher_regular;

import com.ldbc.driver.DbException;
import com.ldbc.driver.OperationHandler;
import com.ldbc.driver.ResultReporter;
import com.ldbc.driver.workloads.ldbc.snb.bi.LdbcSnbBiQuery17FriendshipTriangles;
import com.ldbc.driver.workloads.ldbc.snb.bi.LdbcSnbBiQuery17FriendshipTrianglesResult;
import com.neo4j.bench.ldbc.Retries;
import com.neo4j.bench.ldbc.connection.Neo4jConnectionState;

public class LdbcSnbBiQuery17HandlerEmbeddedCypher
        implements OperationHandler<LdbcSnbBiQuery17FriendshipTriangles,Neo4jConnectionState>
{
    private static final Neo4jSnbBiQuery17EmbeddedCypher QUERY = new Neo4jSnbBiQuery17EmbeddedCypher();

    @Override
    public void executeOperation(
            LdbcSnbBiQuery17FriendshipTriangles operation,
            Neo4jConnectionState dbConnectionState,
            ResultReporter resultReporter ) throws DbException
    {
        Retries<LdbcSnbBiQuery17FriendshipTriangles,LdbcSnbBiQuery17FriendshipTrianglesResult> retries =
                Retries.runInTransaction( QUERY, operation, dbConnectionState );
        resultReporter.report( retries.encodedResultsCode(), retries.result(), operation );
    }
}
