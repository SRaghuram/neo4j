/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.client;

import com.neo4j.bench.client.queries.CreateSchema;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Stream;

import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.harness.junit.EnterpriseNeo4jRule;
import org.neo4j.harness.junit.Neo4jRule;
import org.neo4j.kernel.configuration.Settings;

import static com.neo4j.bench.common.model.Repository.MICRO_BENCH;
import static com.neo4j.bench.common.model.Repository.NEO4J;
import static com.neo4j.bench.common.options.Edition.ENTERPRISE;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertTrue;

public class CommandsSmokeTestIT
{
    private static final QueryRetrier QUERY_RETRIER = new QueryRetrier( false );
    private static final String[] BENCHMARK_GROUPS = {"group1", "group2"};
    private static final int BENCHMARKS_PER_GROUP = 50;
    private static final long BENCHMARK_COUNT = BENCHMARK_GROUPS.length * BENCHMARKS_PER_GROUP;

    private final Neo4jRule neo4j = new EnterpriseNeo4jRule()
            .withConfig( GraphDatabaseSettings.auth_enabled, Settings.FALSE );

    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    @Rule
    public final RuleChain ruleChain = RuleChain.outerRule( temporaryFolder ).around( neo4j );
    private static final String USERNAME = "neo4j";
    private static final String PASSWORD = "neo4j";

    @Test
    public void shouldRunCompareVersionsCommand() throws Exception
    {
        createSyntheticResultsStore();

        Path outputDir = temporaryFolder.newFolder().toPath();
        List<String> versionComparisonArgs = CompareVersionsCommand.argsFor( USERNAME,
                                                                             PASSWORD,
                                                                             neo4j.boltURI(),
                                                                             "3.0.0",
                                                                             "3.0.1",
                                                                             1.0,
                                                                             outputDir );
        Main.main( versionComparisonArgs.stream().toArray( String[]::new ) );

        Path microComparisonCsv = outputDir.resolve( CompareVersionsCommand.MICRO_COMPARISON_FILENAME );
        Path microCoverageCsv = outputDir.resolve( CompareVersionsCommand.MICRO_COVERAGE_FILENAME );

        assertTrue( Files.exists( microComparisonCsv ) );
        assertTrue( Files.exists( microCoverageCsv ) );

        // Check for Micro comparison CSV
        assertThat( "Micro comparison file should contain correct number of entries",
                    lineCount( microComparisonCsv ),
                    equalTo( BENCHMARK_COUNT + 1 /*header*/ ) );

        // Check Micro coverage CSV
        assertThat( "Micro coverage file should contain correct number of entries",
                    lineCount( microCoverageCsv ),
                    equalTo( BENCHMARK_COUNT + 1 /*header*/ ) );
    }

    private static long lineCount( Path file ) throws IOException
    {
        try ( Stream<String> lines = Files.lines( file ) )
        {
            return lines.count();
        }
    }

    private void createSyntheticResultsStore()
    {
        SyntheticStoreGenerator generator = new SyntheticStoreGenerator.SyntheticStoreGeneratorBuilder()
                .withDays( 5 )
                .withResultsPerDay( 10 )
                .withBenchmarkGroups( BENCHMARK_GROUPS )
                .withBenchmarkPerGroupCount( BENCHMARKS_PER_GROUP )
                .withNeo4jVersions( "3.0.1", "3.0.0" )
                .withNeo4jEditions( ENTERPRISE )
                .withSettingsInConfig( 1 )
                .withNeo4jBranchOwners( "neo4j" )
                .withToolBranchOwners( "neo-technology" )
                .withTools( MICRO_BENCH )
                .withProjects( NEO4J )
                .withPrintout( false )
                .build();

        generateStoreUsing( generator );
    }

    private void generateStoreUsing( SyntheticStoreGenerator generator )
    {
        try ( StoreClient client = StoreClient.connect( neo4j.boltURI(), USERNAME, PASSWORD, 1 ) )
        {
            QUERY_RETRIER.execute( client, new CreateSchema(), 0 );
            generator.generate( client );
        }
    }
}
