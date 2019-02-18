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
import com.neo4j.bench.client.util.SyntheticStoreGenerator;
import com.neo4j.bench.client.util.SyntheticStoreGenerator.SyntheticStoreGeneratorBuilder;
import com.neo4j.bench.procedures.detection.CompareFunction;
import com.neo4j.bench.procedures.detection.DateTimeFunction;
import com.neo4j.bench.procedures.detection.VarianceProcedure;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;

import java.util.List;

import org.neo4j.driver.v1.Config;
import org.neo4j.driver.v1.GraphDatabase;
import org.neo4j.driver.v1.Record;
import org.neo4j.driver.v1.Session;
import org.neo4j.driver.v1.StatementResult;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import com.neo4j.harness.junit.rule.CommercialNeo4jRule;
import org.neo4j.harness.junit.rule.Neo4jRule;
import org.neo4j.configuration.Settings;

import static com.neo4j.bench.client.model.Edition.ENTERPRISE;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

public class CompareFunctionTest
{
    private static final QueryRetrier QUERY_RETRIER = new QueryRetrier();
    private static final String USERNAME = "neo4j";
    private static final String PASSWORD = "neo4j";

    @Rule
    public Neo4jRule neo4j = new CommercialNeo4jRule()
            .withProcedure( VarianceProcedure.class )
            .withFunction( CompareFunction.class )
            .withFunction( DateTimeFunction.class )
            .withConfig( GraphDatabaseSettings.auth_enabled, Settings.FALSE );

    @Before
    public void generateStore() throws Exception
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
    }

    @Ignore
    @Test
    public void shouldCalculateVariancesForBenchmarkGroup() throws Throwable
    {
        try ( Session session = GraphDatabase
                .driver( neo4j.boltURI(), Config.build().withoutEncryption().toConfig() )
                .session() )
        {

            String groupName = "'0'";
            String neo4jSeries = "'3.0'";
            String owner = "'neo4j'";
            String query = "MATCH (g:BenchmarkGroup {name:" + groupName + "})\n" +
                           "CALL bench.variancesForGroup(g," + neo4jSeries + "," + owner + ") \n" +
                           "YIELD group, benchmark, unit, mode, mean, points, diffsHist, diffs\n" +
                           "RETURN group, benchmark, unit, mode, mean, points, diffsHist, diffs\n" +
                           "ORDER BY benchmark ASC";
            StatementResult result = session.run( query );

            List<Record> resultRecords = result.list();

            assertThat( resultRecords.size(), equalTo( 2 ) );
            assertThat( resultRecords.get( 0 ).get( "group" ).asString(), equalTo( "0" ) );
            assertThat( resultRecords.get( 0 ).get( "benchmark" ).asString(),
                        containsString( "0_(k_0,v_0)_(k_1,v_1)_(k_2,v_2)_(k_3,v_3)_(k_4,v_4)_(k_5,v_5)_(k_6,v_6)_(k_7,v_7)_(k_8,v_8)_(k_9,v_9)_" ) );
            System.out.println( resultRecords.get( 0 ).asMap() );
            assertThat( resultRecords.get( 1 ).get( "group" ).asString(), equalTo( "0" ) );
            assertThat( resultRecords.get( 1 ).get( "benchmark" ).asString(),
                        containsString( "1_(k_0,v_0)_(k_1,v_1)_(k_2,v_2)_(k_3,v_3)_(k_4,v_4)_(k_5,v_5)_(k_6,v_6)_(k_7,v_7)_(k_8,v_8)_(k_9,v_9)_" ) );
            System.out.println( resultRecords.get( 1 ).asMap() );
        }
    }

    @Ignore
    @Test
    public void shouldCalculateVariancesForSpecificBenchmarksInBenchmarkGroup() throws Throwable
    {
        try ( Session session = GraphDatabase
                .driver( neo4j.boltURI(), Config.build().withoutEncryption().toConfig() )
                .session() )
        {
            String groupName = "'0'";
            String benchmarkName = "'1_'";
            String neo4jSeries = "'3.0'";
            String owner = "'neo4j'";
            String query = "MATCH (g:BenchmarkGroup {name:" + groupName + "})-[:HAS_BENCHMARK]-(b:Benchmark)\n" +
                           "WHERE b.name CONTAINS " + benchmarkName + "\n" +
                           "WITH g, collect(b) AS bs\n" +
                           "CALL bench.variancesForBenchmarks(g,bs," + neo4jSeries + "," + owner + ")\n" +
                           "YIELD group, benchmark, unit, mode, mean, points, diffsHist, diffs\n" +
                           "RETURN group, benchmark, unit, mode, mean, points, diffsHist, diffs\n" +
                           "ORDER BY benchmark ASC";
            System.out.println( query );
            StatementResult result = session.run( query );

            List<Record> resultRecords = result.list();

            assertThat( resultRecords.size(), equalTo( 1 ) );
            assertThat( resultRecords.get( 0 ).get( "group" ).asString(), equalTo( "0" ) );
            assertThat( resultRecords.get( 0 ).get( "benchmark" ).asString(),
                        containsString( "1_(k_0,v_0)_(k_1,v_1)_(k_2,v_2)_(k_3,v_3)_(k_4,v_4)_(k_5,v_5)_(k_6,v_6)_(k_7,v_7)_(k_8,v_8)_(k_9,v_9)_" ) );
            System.out.println( resultRecords.get( 0 ).asMap() );
        }
    }

    @Ignore
    @Test
    public void shouldCalculateComparison() throws Throwable
    {
        try ( Session session = GraphDatabase
                .driver( neo4j.boltURI(), Config.build().withoutEncryption().toConfig() )
                .session() )
        {
            String query =
                    "MATCH (g:BenchmarkGroup {name:'0'})-[:HAS_BENCHMARK]-(b:Benchmark)\n" +
                    "WITH g, b\n" +
                    "CALL bench.variancesForBenchmarks(g,[b],'3.0.0','neo4j')\n" +
                    "YIELD diffsHist, unit\n" +
                    "WITH  g, b, bench.convert(diffsHist['75'],unit,'ns',b.mode) AS v\n" +
                    "MATCH (g)-[:HAS_BENCHMARK]-(b)<-[METRICS_FOR]-(m:Metrics)<-[:HAS_METRICS]-(tr:TestRun)" +
                    "-[:WITH_NEO4J]->(:Neo4j {version:'3.0.0'})\n" +
                    "WITH g, b, m, tr, v\n" +
                    "ORDER BY tr.date DESC\n" +
                    "WITH g, b, head(collect(m)) AS m, v\n" +
                    "WITH g, b, m AS baseMetrics, v AS baseVariance\n" +
                    "CALL bench.variancesForBenchmarks(g,[b],'3.0.1','neo4j')\n" +
                    "YIELD diffsHist, unit\n" +
                    "WITH  g, b, baseMetrics, baseVariance, bench.convert(diffsHist['75'],unit,'ns',b.mode) AS " +
                    "compareVariance\n" +
                    "MATCH (g)-[:HAS_BENCHMARK]-(b)<-[METRICS_FOR]-(m:Metrics)<-[:HAS_METRICS]-(tr:TestRun)" +
                    "-[:WITH_NEO4J]->(:Neo4j {version:'3.0.1'})\n" +
                    "WITH g, b, m, tr, baseMetrics, baseVariance, compareVariance\n" +
                    "ORDER BY tr.date DESC\n" +
                    "WITH g,\n" +
                    "     b,\n" +
                    "     head(collect(m)) AS compareMetrics,\n" +
                    "     baseMetrics, \n" +
                    "     baseVariance, \n" +
                    "     compareVariance\n" +
                    "WITH g,\n" +
                    "     b,\n" +
                    "     bench.convert(compareMetrics.mean,compareMetrics.unit,'ns',b.mode) AS compareMean,\n" +
                    "     bench.convert(baseMetrics.mean,baseMetrics.unit,'ns',b.mode) AS baseMean, \n" +
                    "     baseVariance, \n" +
                    "     compareVariance\n" +
                    "RETURN g.name, \n" +
                    "       b.name, \n" +
                    "       baseMean, \n" +
                    "       baseVariance, \n" +
                    "       compareMean,\n" +
                    "       compareVariance,\n" +
                    "       b.mode as mode,\n" +
                    "       bench.compare(baseMean, \n" +
                    "                     baseVariance, \n" +
                    "                     compareMean,\n" +
                    "                     compareVariance,\n" +
                    "                     b.mode) AS compareResult";

            StatementResult result = session.run( query );
            List<Record> resultRecords = result.list();

            resultRecords.forEach( System.out::println );
        }
    }

    private void generateStoreUsing( SyntheticStoreGenerator generator ) throws Exception
    {
        try ( StoreClient client = StoreClient.connect( neo4j.boltURI(), USERNAME, PASSWORD ) )
        {
            QUERY_RETRIER.execute( client, new CreateSchema() );
            new QueryRetrier().execute( client, new VerifyStoreSchema() );
            generator.generate( client );
        }
    }
}
