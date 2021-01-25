/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.ldbc.interactive.remote_cypher_regular;

import com.ldbc.driver.DbException;
import com.ldbc.driver.OperationHandler;
import com.ldbc.driver.ResultReporter;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcShortQuery1PersonProfile;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcShortQuery1PersonProfileResult;
import com.neo4j.bench.ldbc.Retries;
import com.neo4j.bench.ldbc.connection.Neo4jConnectionState;

import static com.neo4j.bench.ldbc.Retries.run;

public class LdbcShortQuery1HandlerRemoteCypher
        implements OperationHandler<LdbcShortQuery1PersonProfile,Neo4jConnectionState>
{
    private static final Neo4jShortQuery1RemoteCypher QUERY = new Neo4jShortQuery1RemoteCypher();

    @Override
    public void executeOperation(
            LdbcShortQuery1PersonProfile operation,
            Neo4jConnectionState dbConnectionState,
            ResultReporter resultReporter ) throws DbException
    {
        Retries<LdbcShortQuery1PersonProfile,LdbcShortQuery1PersonProfileResult> retries = run(
                QUERY,
                operation,
                dbConnectionState
        );
        resultReporter.report( retries.encodedResultsCode(), retries.result(), operation );
    }
}
