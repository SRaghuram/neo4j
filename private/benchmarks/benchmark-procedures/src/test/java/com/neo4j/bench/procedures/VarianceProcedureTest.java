/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.procedures;

import com.neo4j.bench.client.QueryRetrier;
import com.neo4j.bench.client.StoreClient;
import com.neo4j.bench.client.queries.CreateSchema;
import com.neo4j.bench.client.queries.VerifyStoreSchema;
import com.neo4j.bench.procedures.detection.VarianceProcedure;
import com.neo4j.harness.junit.extension.CommercialNeo4jExtension;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.net.URI;
import com.neo4j.common.util.SyntheticStoreGenerator;
import com.neo4j.common.util.SyntheticStoreGenerator.SyntheticStoreGeneratorBuilder;

import java.util.List;

import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.configuration.Settings;
import org.neo4j.configuration.connectors.ConnectorPortRegister;
import org.neo4j.driver.v1.Config;
import org.neo4j.driver.v1.GraphDatabase;
import org.neo4j.driver.v1.Record;
import org.neo4j.driver.v1.Session;
import org.neo4j.driver.v1.StatementResult;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.harness.junit.extension.Neo4jExtension;
import org.neo4j.internal.helpers.HostnamePort;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

import static com.neo4j.bench.common.options.Edition.ENTERPRISE;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

public class VarianceProcedureTest
{
    private static final QueryRetrier QUERY_RETRIER = new QueryRetrier();
    private static final String USERNAME = "neo4j";
    private static final String PASSWORD = "neo4j";

    @RegisterExtension
    static Neo4jExtension neo4jExtension = CommercialNeo4jExtension.builder()
            .withProcedure( VarianceProcedure.class )
            .withFunction( VarianceProcedure.class )
            .withConfig( GraphDatabaseSettings.auth_enabled, Settings.FALSE )
            .build();

    private URI boltUri;

    @BeforeEach
    public void generateStore( GraphDatabaseService databaseService ) throws Exception
    {
        SyntheticStoreGenerator generator = new SyntheticStoreGeneratorBuilder()
                .withDays( 10 )
                .withResultsPerDay( 10 )
                .withBenchmarkGroupCount( 2 )
                .withBenchmarkPerGroupCount( 2 )
                .withNeo4jVersions( "3.0.3", "3.0.2", "3.0.1", "3.0.0" )
                .withNeo4jBranchOwners( "neo4j", "other" )
                .withNeo4jEditions( ENTERPRISE )
                .withSettingsInConfig( 1 )
                .withOperatingSystems( "Ubuntu" )
                .withServers( "local" )
                .withJvms( "Oracle" )
                .withJvmVersions( "1.80_66" )
                .withPrintout( true )
                .withAssertions( true )
                .build();

        generateStoreUsing( generator );

        HostnamePort address = ((GraphDatabaseAPI) databaseService).getDependencyResolver()
                .resolveDependency( ConnectorPortRegister.class ).getLocalAddress( "bolt" );
        boltUri = URI.create( "bolt://" + address.toString() );
    }

    @AfterEach
    public void cleanUpDb( GraphDatabaseService databaseService )
    {
        // this is hacky HACK, needs to be fixed in Neo4jExtension
        databaseService.execute( "MATCH (n) DETACH DELETE n" ).close();
    }

    @Disabled
    @Test
    public void shouldCalculateVariancesForBenchmark() throws Throwable
    {
        try ( Session session = GraphDatabase
                .driver( boltUri, Config.build().withoutEncryption().toConfig() )
                .session() )
        {
            String g = "'0'";
            String b = "0_(k_0,v_0)_(k_1,v_1)_(k_2,v_2)_(k_3,v_3)_(k_4,v_4)_(k_5,v_5)_(k_6,v_6)_(k_7,v_7)_(k_8,v_8)_(k_9,v_9)";
            String series = "'3.0'";
            String owner = "'neo4j'";
            String query =
                    "MATCH (g:BenchmarkGroup {name:" + g + "} )-[:HAS_BENCHMARK]->(b:Benchmark)\n" +
                    "WHERE b.name CONTAINS '" + b + "' " +
                    "WITH g, b LIMIT 1 " +
                    "WITH g.name AS g, b.name AS b, bench.varianceForBenchmark(g,b," + series + "," + owner +
                    ") AS v\n" +
                    "RETURN g, b, v, v['50'] AS median";
            StatementResult result = session.run( query );

            List<Record> resultRecords = result.list();

            assertThat( resultRecords.size(), equalTo( 1 ) );
            assertThat( resultRecords.get( 0 ).get( "g" ).asString(), equalTo( "0" ) );
            assertThat( resultRecords.get( 0 ).get( "b" ).asString(), containsString( b ) );
            System.out.println( resultRecords.get( 0 ).asMap() );
        }
    }

    @Disabled
    @Test
    public void shouldCalculateVariancesForBenchmarkGroup() throws Throwable
    {
        try ( Session session = GraphDatabase
                .driver( boltUri, Config.build().withoutEncryption().toConfig() )
                .session() )
        {

            String groupName = "'0'";
            String series = "'3.0'";
            String owner = "'neo4j'";
            String query = "MATCH (g:BenchmarkGroup {name:" + groupName + "})\n" +
                           "CALL bench.variancesForGroup(g," + series + "," + owner + ") \n" +
                           "YIELD group, benchmark, unit, mode, mean, points, diffsHist, diffs\n" +
                           "RETURN group, benchmark, unit, mode, mean, points, diffsHist, diffs\n" +
                           "ORDER BY benchmark ASC";
            StatementResult result = session.run( query );

            List<Record> resultRecords = result.list();

            assertThat( resultRecords.size(), equalTo( 2 ) );
            assertThat( resultRecords.get( 0 ).get( "group" ).asString(), equalTo( "0" ) );
            assertThat( resultRecords.get( 0 ).get( "benchmark" ).asString(),
                        containsString( "0_(k_0,v_0)_(k_1,v_1)_(k_2,v_2)_(k_3,v_3)_(k_4,v_4)_(k_5,v_5)_(k_6,v_6)_(k_7,v_7)_(k_8,v_8)_(k_9,v_9)" ) );
            assertThat( resultRecords.get( 1 ).get( "group" ).asString(), equalTo( "0" ) );
            assertThat( resultRecords.get( 1 ).get( "benchmark" ).asString(),
                        containsString( "1_(k_0,v_0)_(k_1,v_1)_(k_2,v_2)_(k_3,v_3)_(k_4,v_4)_(k_5,v_5)_(k_6,v_6)_(k_7,v_7)_(k_8,v_8)_(k_9,v_9)" ) );
            System.out.println( resultRecords.get( 1 ).asMap() );
        }
    }

    @Disabled
    @Test
    public void shouldCalculateVariancesForSpecificBenchmarksInBenchmarkGroup() throws Throwable
    {
        try ( Session session = GraphDatabase
                .driver( boltUri, Config.build().withoutEncryption().toConfig() )
                .session() )
        {
            String groupName = "'0'";
            String benchmarkName = "'1_'";
            String series = "'3.0'";
            String owner = "'neo4j'";
            String query = "MATCH (g:BenchmarkGroup {name:" + groupName + "})-[:HAS_BENCHMARK]-(b:Benchmark)\n" +
                           "WHERE b.name CONTAINS " + benchmarkName + "\n" +
                           "WITH g, collect(b) AS bs\n" +
                           "CALL bench.variancesForBenchmarks(g,bs," + series + "," + owner + ")\n" +
                           "YIELD group, benchmark, unit, mode, mean, points, diffsHist, diffs\n" +
                           "RETURN group, benchmark, unit, mode, mean, points, diffsHist, diffs\n" +
                           "ORDER BY benchmark ASC";
            StatementResult result = session.run( query );

            List<Record> resultRecords = result.list();

            assertThat( resultRecords.size(), equalTo( 1 ) );
            assertThat( resultRecords.get( 0 ).get( "group" ).asString(), equalTo( "0" ) );
            assertThat( resultRecords.get( 0 ).get( "benchmark" ).asString(),
                        containsString( "1_(k_0,v_0)_(k_1,v_1)_(k_2,v_2)_(k_3,v_3)_(k_4,v_4)_(k_5,v_5)_(k_6,v_6)_(k_7,v_7)_(k_8,v_8)_(k_9,v_9)_" ) );
            System.out.println( resultRecords.get( 0 ).asMap() );
        }
    }

    private void generateStoreUsing( SyntheticStoreGenerator generator ) throws Exception
    {
        try ( StoreClient client = StoreClient.connect( boltUri, USERNAME, PASSWORD ) )
        {
            QUERY_RETRIER.execute( client, new CreateSchema() );
            new QueryRetrier().execute( client, new VerifyStoreSchema() );
            generator.generate( client );
        }
    }
}
