/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.ldbc.business_intelligence.queries.embedded_cypher_regular;

import com.ldbc.driver.DbException;
import com.ldbc.driver.OperationHandler;
import com.ldbc.driver.ResultReporter;
import com.ldbc.driver.workloads.ldbc.snb.bi.LdbcSnbBiQuery2TopTags;
import com.ldbc.driver.workloads.ldbc.snb.bi.LdbcSnbBiQuery2TopTagsResult;
import com.neo4j.bench.ldbc.Retries;
import com.neo4j.bench.ldbc.connection.Neo4jConnectionState;

import java.util.List;

public class LdbcSnbBiQuery2HandlerEmbeddedCypher
        implements OperationHandler<LdbcSnbBiQuery2TopTags,Neo4jConnectionState>
{
    private static final Neo4jSnbBiQuery2EmbeddedCypher QUERY = new Neo4jSnbBiQuery2EmbeddedCypher();

    @Override
    public void executeOperation(
            LdbcSnbBiQuery2TopTags operation,
            Neo4jConnectionState dbConnectionState,
            ResultReporter resultReporter ) throws DbException
    {
        Retries<LdbcSnbBiQuery2TopTags,List<LdbcSnbBiQuery2TopTagsResult>> retries =
                Retries.runInTransaction( QUERY, operation, dbConnectionState );
        resultReporter.report( retries.encodedResultsCode(), retries.result(), operation );
    }
}
