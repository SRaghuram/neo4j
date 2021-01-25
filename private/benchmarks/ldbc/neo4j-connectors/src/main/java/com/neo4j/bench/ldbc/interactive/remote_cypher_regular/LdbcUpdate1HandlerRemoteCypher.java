/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.ldbc.interactive.remote_cypher_regular;

import com.ldbc.driver.DbException;
import com.ldbc.driver.OperationHandler;
import com.ldbc.driver.ResultReporter;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcNoResult;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcUpdate1AddPerson;
import com.neo4j.bench.ldbc.Retries;
import com.neo4j.bench.ldbc.connection.Neo4jConnectionState;

import static com.neo4j.bench.ldbc.Retries.run;

public class LdbcUpdate1HandlerRemoteCypher implements OperationHandler<LdbcUpdate1AddPerson,Neo4jConnectionState>
{
    private static final Neo4jUpdate1RemoteCypher QUERY = new Neo4jUpdate1RemoteCypher();

    @Override
    public void executeOperation( LdbcUpdate1AddPerson operation, Neo4jConnectionState dbConnectionState,
            ResultReporter resultReporter ) throws DbException
    {
        Retries<LdbcUpdate1AddPerson,LdbcNoResult> retries = run( QUERY, operation, dbConnectionState );
        resultReporter.report( retries.encodedResultsCode(), retries.result(), operation );
    }
}
