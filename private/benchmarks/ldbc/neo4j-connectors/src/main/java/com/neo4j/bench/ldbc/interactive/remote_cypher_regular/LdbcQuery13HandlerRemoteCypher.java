/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.ldbc.interactive.remote_cypher_regular;

import com.ldbc.driver.DbException;
import com.ldbc.driver.OperationHandler;
import com.ldbc.driver.ResultReporter;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery13;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery13Result;
import com.neo4j.bench.ldbc.Retries;
import com.neo4j.bench.ldbc.connection.Neo4jConnectionState;

import static com.neo4j.bench.ldbc.Retries.run;

public class LdbcQuery13HandlerRemoteCypher implements OperationHandler<LdbcQuery13,Neo4jConnectionState>
{
    private static final Neo4jLongQuery13RemoteCypher QUERY = new Neo4jLongQuery13RemoteCypher();

    @Override
    public void executeOperation(
            LdbcQuery13 operation,
            Neo4jConnectionState dbConnectionState,
            ResultReporter resultReporter ) throws DbException
    {
        Retries<LdbcQuery13,LdbcQuery13Result> retries = run( QUERY, operation, dbConnectionState );
        resultReporter.report( retries.encodedResultsCode(), retries.result(), operation );
    }
}
