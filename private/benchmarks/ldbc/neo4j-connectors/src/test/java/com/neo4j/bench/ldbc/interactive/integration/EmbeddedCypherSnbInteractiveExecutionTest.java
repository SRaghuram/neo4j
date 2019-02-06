/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.ldbc.interactive.integration;

import com.ldbc.driver.DbException;
import com.ldbc.driver.control.DriverConfiguration;
import com.ldbc.driver.control.DriverConfigurationException;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcSnbInteractiveWorkloadConfiguration;
import com.neo4j.bench.ldbc.connection.CsvSchema;
import com.neo4j.bench.ldbc.connection.Neo4jApi;
import com.neo4j.bench.ldbc.connection.Neo4jSchema;
import com.neo4j.bench.ldbc.importer.Scenario;
import com.neo4j.bench.ldbc.utils.PlannerType;
import com.neo4j.bench.ldbc.utils.RuntimeType;

public class EmbeddedCypherSnbInteractiveExecutionTest extends SnbInteractiveExecutionTest
{
    @Override
    Scenario buildValidationData() throws DbException
    {
        return Scenario.randomInteractiveFor(
                CsvSchema.CSV_REGULAR,
                Neo4jSchema.NEO4J_REGULAR,
                Scenario.randomNeo4jImporterFor( CsvSchema.CSV_REGULAR, Neo4jSchema.NEO4J_REGULAR ),
                Neo4jApi.EMBEDDED_CYPHER,
                PlannerType.DEFAULT,
                RuntimeType.DEFAULT );
    }

    @Override
    DriverConfiguration modifyConfiguration( DriverConfiguration configuration ) throws DriverConfigurationException
    {
        return configuration
                .applyArg(
                        LdbcSnbInteractiveWorkloadConfiguration.LONG_READ_OPERATION_3_ENABLE_KEY,
                        Boolean.toString( false ) )
                .applyArg(
                        LdbcSnbInteractiveWorkloadConfiguration.LONG_READ_OPERATION_9_ENABLE_KEY,
                        Boolean.toString( false ) )
                .applyArg(
                        LdbcSnbInteractiveWorkloadConfiguration.LONG_READ_OPERATION_14_ENABLE_KEY,
                        Boolean.toString( false ) );
    }
}
