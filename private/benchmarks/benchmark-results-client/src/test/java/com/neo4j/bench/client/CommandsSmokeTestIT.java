/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.client;

import com.neo4j.bench.client.SyntheticStoreGenerator.GenerationResult;
import com.neo4j.bench.client.SyntheticStoreGenerator.Group;
import com.neo4j.bench.client.queries.CreateSchema;
import com.neo4j.bench.common.model.Benchmark;
import com.neo4j.bench.common.model.BenchmarkGroup;
import com.neo4j.bench.common.util.BenchmarkUtil;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.harness.junit.EnterpriseNeo4jRule;
import org.neo4j.harness.junit.Neo4jRule;
import org.neo4j.kernel.configuration.Settings;

import static com.neo4j.bench.common.model.Repository.LDBC_BENCH;
import static com.neo4j.bench.common.model.Repository.MICRO_BENCH;
import static com.neo4j.bench.common.model.Repository.NEO4J;
import static com.neo4j.bench.common.options.Edition.ENTERPRISE;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertTrue;

public class CommandsSmokeTestIT
{
    private static final QueryRetrier QUERY_RETRIER = new QueryRetrier( false );
    private static final BenchmarkGroup LDBC_READ = new BenchmarkGroup( "LdbcSnbInteractive-Read" );
    private static final BenchmarkGroup LDBC_WRITE = new BenchmarkGroup( "LdbcSnbInteractive-Write" );

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
        GenerationResult generationResult = createSyntheticResultsStore();

        Path outputDir = temporaryFolder.newFolder().toPath();
        List<String> versionComparisonArgs = CompareVersionsCommand.argsFor( USERNAME,
                                                                             PASSWORD,
                                                                             neo4j.boltURI(),
                                                                             "3.0.0",
                                                                             "3.0.1",
                                                                             1.0,
                                                                             outputDir );
        Main.main( versionComparisonArgs.stream().toArray( String[]::new ) );

        assertThat( fileCount( outputDir ), equalTo( 3L ) );
        Path microComparisonCsv = outputDir.resolve( CompareVersionsCommand.MICRO_COMPARISON_FILENAME );
        Path microCoverageCsv = outputDir.resolve( CompareVersionsCommand.MICRO_COVERAGE_FILENAME );
        Path ldbcComparisonCsv = outputDir.resolve( CompareVersionsCommand.LDBC_COMPARISON_FILENAME );

        assertTrue( Files.exists( microComparisonCsv ) );
        assertTrue( Files.exists( microCoverageCsv ) );
        assertTrue( Files.exists( ldbcComparisonCsv ) );

        // Check for Micro comparison CSV
        assertThat( "Micro comparison file should contain correct number of entries",
                    lineCount( microComparisonCsv ),
                    equalTo( generationResult.benchmarksInTool( MICRO_BENCH.projectName() ) + 1L /*header*/ ) );

        // Check Micro coverage CSV
        assertThat( "Micro coverage file should contain correct number of entries",
                    lineCount( microCoverageCsv ),
                    equalTo( generationResult.benchmarksInTool( MICRO_BENCH.projectName() ) + 1L /*header*/ ) );

        // Check for LDBC comparison CSV
        assertThat( "LDBC comparison file should contain correct number of entries",
                    lineCount( ldbcComparisonCsv ),
                    equalTo( generationResult.benchmarksIn( LDBC_BENCH.projectName(), LDBC_WRITE.name(), LDBC_READ.name() ) + 1L /*header*/ ) );
    }

    private static long fileCount( Path folder ) throws IOException
    {
        BenchmarkUtil.assertDirectoryExists( folder );
        try ( Stream<Path> files = Files.list( folder ) )
        {
            return files.count();
        }
    }

    private static long lineCount( Path file ) throws IOException
    {
        try ( Stream<String> lines = Files.lines( file ) )
        {
            return lines.count();
        }
    }

    private GenerationResult createSyntheticResultsStore()
    {
        SyntheticStoreGenerator generator = new SyntheticStoreGenerator.SyntheticStoreGeneratorBuilder()
                .withDays( 5 )
                .withResultsPerDay( 10 )
                .withBenchmarkGroups( Group.from( "group1", 50 ),
                                      Group.from( "group2", 50 ),
                                      Group.from( LDBC_WRITE,
                                                  ldbcBench( "api", "Core API", "scale_factor", "10" ) ),
                                      Group.from( LDBC_READ,
                                                  ldbcBench( "api", "Cypher", "scale_factor", "1" ) ) )
                .withNeo4jVersions( "3.0.1", "3.0.0" )
                .withNeo4jEditions( ENTERPRISE )
                .withSettingsInConfig( 1 )
                .withNeo4jBranchOwners( "neo4j" )
                .withToolBranchOwners( "neo-technology" )
                .withTools( MICRO_BENCH, LDBC_BENCH )
                .withProjects( NEO4J )
                .withPrintout( false )
                .build();
        try ( StoreClient client = StoreClient.connect( neo4j.boltURI(), USERNAME, PASSWORD, 1 ) )
        {
            QUERY_RETRIER.execute( client, new CreateSchema(), 0 );
            return generator.generate( client );
        }
    }

    private Benchmark ldbcBench( String... params )
    {
        assert params.length % 2 == 0;
        Map<String,String> paramsMap = new HashMap<>();
        for ( int i = 0; i < params.length; i += 2 )
        {
            paramsMap.put( params[i], params[i + 1] );
        }
        return Benchmark.benchmarkFor( "Description",
                                       "Summary",
                                       Benchmark.Mode.THROUGHPUT,
                                       paramsMap );
    }
}
