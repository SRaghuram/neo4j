/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
import org.junit.Rule;
import org.junit.Test;

import java.time.ZonedDateTime;
import java.util.Collections;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.neo4j.driver.Result;
import org.neo4j.driver.Values;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.harness.junit.EnterpriseNeo4jRule;
import org.neo4j.harness.junit.Neo4jRule;
import org.neo4j.kernel.configuration.Settings;

import static java.util.Collections.emptyMap;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class CreateJobTest
{

    @Rule
    public final Neo4jRule neo4j = new EnterpriseNeo4jRule()
            .withConfig( GraphDatabaseSettings.auth_enabled, Settings.FALSE );

    @Test
    public void createBatchJob()
    {
        // given
        StoreClient storeClient = StoreClient.connect( neo4j.boltURI(), "neo4j", "neo4j" );
        storeClient.execute( new CreateSchema() );

        TestRun testRun = new TestRun( UUID.randomUUID().toString(), 0, 0, 0, 0, "triggeredBy" );

        BenchmarkGroupBenchmarkMetrics benchmarkGroupBenchmarkMetrics = new BenchmarkGroupBenchmarkMetrics();
        benchmarkGroupBenchmarkMetrics.add(
                new BenchmarkGroup( "group" ),
                Benchmark.benchmarkFor( "description", "simpleName", Benchmark.Mode.LATENCY, emptyMap() ),
                new Metrics( TimeUnit.MILLISECONDS, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 ),
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
                Environment.current(),
                benchmarkGroupBenchmarkMetrics,
                new BenchmarkTool( Repository.NEO4J, "commit", "owner", "branch" ),
                Java.current( "" ),
                Collections.emptyList()
        );

        SubmitTestRun submitTestRun = new SubmitTestRun( testRunReport );

        storeClient.execute( submitTestRun );

        ZonedDateTime startDateTime = ZonedDateTime.now();
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
