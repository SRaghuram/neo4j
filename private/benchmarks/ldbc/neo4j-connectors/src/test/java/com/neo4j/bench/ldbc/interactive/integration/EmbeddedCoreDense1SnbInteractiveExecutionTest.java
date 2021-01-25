/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.ldbc.interactive.integration;

import com.neo4j.bench.common.options.Planner;
import com.neo4j.bench.common.options.Runtime;
import com.neo4j.bench.ldbc.connection.CsvSchema;
import com.neo4j.bench.ldbc.connection.Neo4jApi;
import com.neo4j.bench.ldbc.connection.Neo4jSchema;
import com.neo4j.bench.ldbc.importer.Scenario;

public class EmbeddedCoreDense1SnbInteractiveExecutionTest extends SnbInteractiveExecutionTest
{
    @Override
    Scenario buildValidationData()
    {
        return Scenario.randomInteractiveFor(
                CsvSchema.CSV_MERGE,
                Neo4jSchema.NEO4J_DENSE_1,
                Neo4jApi.EMBEDDED_CORE,
                Planner.DEFAULT,
                Runtime.DEFAULT );
    }
}
