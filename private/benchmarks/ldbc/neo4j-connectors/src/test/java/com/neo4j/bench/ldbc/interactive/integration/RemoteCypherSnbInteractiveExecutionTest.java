/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.ldbc.interactive.integration;

import com.ldbc.driver.control.DriverConfiguration;
import com.ldbc.driver.control.DriverConfigurationException;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcSnbInteractiveWorkloadConfiguration;
import com.neo4j.bench.ldbc.Neo4jDb;
import com.neo4j.bench.ldbc.connection.CsvSchema;
import com.neo4j.bench.ldbc.connection.Neo4jApi;
import com.neo4j.bench.ldbc.connection.Neo4jSchema;
import com.neo4j.bench.ldbc.importer.Scenario;
import com.neo4j.bench.ldbc.utils.PlannerType;
import com.neo4j.bench.ldbc.utils.RuntimeType;

import java.io.File;

import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.test.ports.PortAuthority;

import static com.neo4j.bench.ldbc.DriverConfigUtils.getResource;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;

public class RemoteCypherSnbInteractiveExecutionTest extends SnbInteractiveExecutionTest
{
    @Override
    Scenario buildValidationData()
    {
        return Scenario.randomInteractiveFor(
                CsvSchema.CSV_REGULAR,
                Neo4jSchema.NEO4J_REGULAR,
                Neo4jApi.REMOTE_CYPHER,
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

    @Override
    DatabaseAndUrl createRemoteConnector( File homeDir )
    {
        int port = PortAuthority.allocatePort();
        String boltAddressWithoutPort = "localhost";
        DatabaseManagementService managementService = Neo4jDb.newDbBuilderForBolt(
                homeDir,
                getResource( "/neo4j/neo4j_sf001.conf" ),
                boltAddressWithoutPort,
                port
        ).build();
        GraphDatabaseService db = managementService.database( DEFAULT_DATABASE_NAME );
        String url = "bolt://" + boltAddressWithoutPort + ":" + port;
        return new DatabaseAndUrl( managementService, url );
    }
}

