/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.ldbc.business_intelligence.queries.embedded_cypher_regular;

import com.ldbc.driver.DbException;
import com.ldbc.driver.OperationHandler;
import com.ldbc.driver.ResultReporter;
import com.ldbc.driver.workloads.ldbc.snb.bi.LdbcSnbBiQuery23HolidayDestinations;
import com.ldbc.driver.workloads.ldbc.snb.bi.LdbcSnbBiQuery23HolidayDestinationsResult;
import com.neo4j.bench.ldbc.Retries;
import com.neo4j.bench.ldbc.connection.Neo4jConnectionState;

import java.util.List;

public class LdbcSnbBiQuery23HandlerEmbeddedCypher
        implements OperationHandler<LdbcSnbBiQuery23HolidayDestinations,Neo4jConnectionState>
{
    private static final Neo4jSnbBiQuery23EmbeddedCypher QUERY = new Neo4jSnbBiQuery23EmbeddedCypher();

    @Override
    public void executeOperation(
            LdbcSnbBiQuery23HolidayDestinations operation,
            Neo4jConnectionState dbConnectionState,
            ResultReporter resultReporter ) throws DbException
    {
        Retries<LdbcSnbBiQuery23HolidayDestinations,List<LdbcSnbBiQuery23HolidayDestinationsResult>> retries =
                Retries.runInTransaction( QUERY, operation, dbConnectionState );
        resultReporter.report( retries.encodedResultsCode(), retries.result(), operation );
    }
}
