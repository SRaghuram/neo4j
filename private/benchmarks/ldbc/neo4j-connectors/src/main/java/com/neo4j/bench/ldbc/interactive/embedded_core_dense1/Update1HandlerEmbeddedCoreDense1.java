/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.ldbc.interactive.embedded_core_dense1;

import com.ldbc.driver.DbException;
import com.ldbc.driver.OperationHandler;
import com.ldbc.driver.ResultReporter;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcNoResult;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcUpdate1AddPerson;
import com.neo4j.bench.ldbc.Retries;
import com.neo4j.bench.ldbc.connection.Neo4jConnectionState;
import com.neo4j.bench.ldbc.interactive.Neo4jUpdate1;
import com.neo4j.bench.ldbc.interactive.embedded_core.Update1EmbeddedCore_1;

public class Update1HandlerEmbeddedCoreDense1
        implements OperationHandler<LdbcUpdate1AddPerson,Neo4jConnectionState>
{
    private static final Neo4jUpdate1<Neo4jConnectionState> QUERY = new Update1EmbeddedCore_1();

    @Override
    public void executeOperation( LdbcUpdate1AddPerson operation, Neo4jConnectionState dbConnectionState,
            ResultReporter resultReporter ) throws DbException
    {
        Retries<LdbcUpdate1AddPerson,LdbcNoResult>
                retries = Retries.runInTransaction( QUERY, operation, dbConnectionState );
        resultReporter.report( retries.encodedResultsCode(), retries.result(), operation );
    }
}
