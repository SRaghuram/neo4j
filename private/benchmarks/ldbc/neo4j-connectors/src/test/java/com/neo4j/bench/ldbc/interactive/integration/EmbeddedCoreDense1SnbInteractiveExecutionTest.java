/**
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.ldbc.interactive.integration;

import com.ldbc.driver.DbException;
import com.neo4j.bench.ldbc.connection.CsvSchema;
import com.neo4j.bench.ldbc.connection.Neo4jApi;
import com.neo4j.bench.ldbc.connection.Neo4jSchema;
import com.neo4j.bench.ldbc.importer.Scenario;
import com.neo4j.bench.ldbc.utils.PlannerType;
import com.neo4j.bench.ldbc.utils.RuntimeType;
import org.junit.Ignore;

@Ignore
public class EmbeddedCoreDense1SnbInteractiveExecutionTest extends SnbInteractiveExecutionTest
{
    @Override
    Scenario buildValidationData() throws DbException
    {
        return Scenario.randomInteractiveFor(
                CsvSchema.CSV_MERGE,
                Neo4jSchema.NEO4J_DENSE_1,
                Scenario.randomNeo4jImporterFor( CsvSchema.CSV_MERGE, Neo4jSchema.NEO4J_DENSE_1 ),
                Neo4jApi.EMBEDDED_CORE,
                PlannerType.DEFAULT,
                RuntimeType.DEFAULT );
    }
}
