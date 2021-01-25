/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.ldbc.interactive.remote_cypher_regular;

import com.ldbc.driver.DbException;
import com.ldbc.driver.OperationHandler;
import com.ldbc.driver.ResultReporter;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery11;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery11Result;
import com.neo4j.bench.ldbc.Retries;
import com.neo4j.bench.ldbc.connection.Neo4jConnectionState;

import java.util.List;

import static com.neo4j.bench.ldbc.Retries.run;

public class LdbcQuery11HandlerRemoteCypher implements OperationHandler<LdbcQuery11,Neo4jConnectionState>
{
    private static final Neo4jLongQuery11RemoteCypher QUERY = new Neo4jLongQuery11RemoteCypher();

    @Override
    public void executeOperation(
            LdbcQuery11 operation,
            Neo4jConnectionState dbConnectionState,
            ResultReporter resultReporter ) throws DbException
    {
        Retries<LdbcQuery11,List<LdbcQuery11Result>> retries = run( QUERY, operation, dbConnectionState );
        resultReporter.report( retries.encodedResultsCode(), retries.result(), operation );
    }
}
