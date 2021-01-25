/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.ldbc.interactive.remote_cypher_regular;

import com.ldbc.driver.DbException;
import com.ldbc.driver.OperationHandler;
import com.ldbc.driver.ResultReporter;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcShortQuery4MessageContent;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcShortQuery4MessageContentResult;
import com.neo4j.bench.ldbc.Retries;
import com.neo4j.bench.ldbc.connection.Neo4jConnectionState;

import static com.neo4j.bench.ldbc.Retries.run;

public class LdbcShortQuery4HandlerRemoteCypher
        implements OperationHandler<LdbcShortQuery4MessageContent,Neo4jConnectionState>
{
    private static final Neo4jShortQuery4RemoteCypher QUERY = new Neo4jShortQuery4RemoteCypher();

    @Override
    public void executeOperation(
            LdbcShortQuery4MessageContent operation,
            Neo4jConnectionState dbConnectionState,
            ResultReporter resultReporter ) throws DbException
    {
        Retries<LdbcShortQuery4MessageContent,LdbcShortQuery4MessageContentResult> retries = run(
                QUERY,
                operation,
                dbConnectionState
        );
        resultReporter.report( retries.encodedResultsCode(), retries.result(), operation );
    }
}
