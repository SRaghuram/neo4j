/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.ldbc.interactive.embedded_cypher_regular;

import com.ldbc.driver.DbException;
import com.ldbc.driver.OperationHandler;
import com.ldbc.driver.ResultReporter;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery7;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery7Result;
import com.neo4j.bench.ldbc.Retries;
import com.neo4j.bench.ldbc.connection.Neo4jConnectionState;

import java.util.List;

import static com.neo4j.bench.ldbc.Retries.runInTransaction;

public class LdbcQuery7HandlerEmbeddedCypher implements OperationHandler<LdbcQuery7,Neo4jConnectionState>
{
    private static final Neo4jLongQuery7EmbeddedCypher QUERY = new Neo4jLongQuery7EmbeddedCypher();

    @Override
    public void executeOperation( LdbcQuery7 operation, Neo4jConnectionState dbConnectionState,
            ResultReporter resultReporter ) throws DbException
    {
        Retries<LdbcQuery7,List<LdbcQuery7Result>> retries = runInTransaction( QUERY, operation, dbConnectionState );
        resultReporter.report( retries.encodedResultsCode(), retries.result(), operation );
    }
}
