/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.client.queries.submit;

import com.google.common.collect.ImmutableSet;
import com.neo4j.bench.client.StoreClient;
import com.neo4j.bench.client.queries.schema.CreateSchema;
import com.neo4j.bench.model.model.Benchmark;
import com.neo4j.bench.model.model.BenchmarkConfig;
import com.neo4j.bench.model.model.BenchmarkGroup;
import com.neo4j.bench.model.model.BenchmarkGroupBenchmarkMetrics;
import com.neo4j.bench.model.model.BenchmarkTool;
import com.neo4j.bench.model.model.Environment;
import com.neo4j.bench.model.model.Java;
import com.neo4j.bench.model.model.Job;
import com.neo4j.bench.model.model.Metrics;
import com.neo4j.bench.model.model.Neo4jConfig;
import com.neo4j.bench.model.model.Project;
import com.neo4j.bench.model.model.Repository;
import com.neo4j.bench.model.model.TestRun;
import com.neo4j.bench.model.model.TestRunReport;
import com.neo4j.bench.model.options.Edition;
import com.neo4j.harness.junit.extension.EnterpriseNeo4jExtension;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.net.URI;
import java.time.ZonedDateTime;
import java.util.Collections;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.configuration.connectors.BoltConnector;
import org.neo4j.configuration.connectors.ConnectorPortRegister;
import org.neo4j.driver.Result;
import org.neo4j.driver.Values;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.harness.junit.extension.Neo4jExtension;
import org.neo4j.internal.helpers.HostnamePort;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

import static java.util.Collections.emptyMap;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class CreateJobTest
{

    @RegisterExtension
    static Neo4jExtension neo4jExtension = EnterpriseNeo4jExtension.builder()
                                                                   .withConfig( BoltConnector.enabled, true )
                                                                   .withConfig( BoltConnector.encryption_level, BoltConnector.EncryptionLevel.OPTIONAL )
                                                                   .withConfig( GraphDatabaseSettings.auth_enabled, false )
                                                                   .build();
    private URI boltUri;

    @BeforeEach
    public void setUp( GraphDatabaseService databaseService )
    {
        HostnamePort address = ((GraphDatabaseAPI) databaseService).getDependencyResolver()
                                                                   .resolveDependency( ConnectorPortRegister.class ).getLocalAddress( "bolt" );
        boltUri = URI.create( "bolt://" + address.toString() );
    }

    @Test
    public void createBatchJob()
    {
        // given
        try ( StoreClient storeClient = StoreClient.connect( boltUri, "neo4j", "neo4j" ) )
        {
            storeClient.execute( new CreateSchema() );

            TestRun testRun = new TestRun( UUID.randomUUID().toString(), 0, 0, 0, 0, "triggeredBy" );

            BenchmarkGroupBenchmarkMetrics benchmarkGroupBenchmarkMetrics = new BenchmarkGroupBenchmarkMetrics();
            benchmarkGroupBenchmarkMetrics.add(
                    new BenchmarkGroup( "group" ),
                    Benchmark.benchmarkFor( "description", "simpleName", Benchmark.Mode.LATENCY, emptyMap() ),
                    new Metrics( Metrics.MetricsUnit.latency( TimeUnit.MILLISECONDS ), 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 ),
                    null,
                    Neo4jConfig.empty() );

            TestRunReport testRunReport = new TestRunReport(
                    testRun,
                    new BenchmarkConfig( emptyMap() ),
                    ImmutableSet.of( new Project(
                            Repository.NEO4J,
                            "commit",
                            "3.5.16",
                            Edition.ENTERPRISE,
                            "branch",
                            "owner" )
                    ),
                    Neo4jConfig.empty(),
                    Environment.local(),
                    benchmarkGroupBenchmarkMetrics,
                    new BenchmarkTool( Repository.NEO4J, "commit", "owner", "branch" ),
                    Java.current( "" ),
                    Collections.emptyList(),
                Collections.emptyList()
        );

            SubmitTestRun submitTestRun = new SubmitTestRun( testRunReport );

            storeClient.execute( submitTestRun );

            String awsBatchJobId = UUID.randomUUID().toString();

            Job job = new Job( awsBatchJobId,
                               ZonedDateTime.now().toEpochSecond(),
                               ZonedDateTime.now().toEpochSecond(),
                               ZonedDateTime.now().toEpochSecond(),
                               "",
                               ""
            );
            CreateJob createJob = new CreateJob( job, testRun.id() );

            // when
            boolean created = storeClient.execute( createJob );

            // then
            assertTrue( created );

            // when
            Job actual = storeClient.session().readTransaction( tx ->
                                                                {
                                                                    Result result = tx.run( "MATCH (bj:Job) WHERE bj.id = $id RETURN bj",
                                                                                            Values.parameters( "id", awsBatchJobId ) );
                                                                    if ( result.hasNext() )
                                                                    {
                                                                        return Job.fromMap( result.next().get( "bj" ).asMap() );
                                                                    }
                                                                    return null;
                                                                } );
            assertEquals( job, actual );
        }
    }
}
