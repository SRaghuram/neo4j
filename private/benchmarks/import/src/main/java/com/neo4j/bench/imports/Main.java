/**
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.imports;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import io.airlift.airline.Command;
import io.airlift.airline.Option;
import io.airlift.airline.OptionType;
import io.airlift.airline.SingleCommand;
import org.apache.commons.compress.utils.IOUtils;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.HashMap;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import com.neo4j.bench.client.QueryRetrier;
import com.neo4j.bench.client.StoreClient;
import com.neo4j.bench.client.model.Benchmark;
import com.neo4j.bench.client.model.BenchmarkConfig;
import com.neo4j.bench.client.model.BenchmarkGroup;
import com.neo4j.bench.client.model.BenchmarkGroupBenchmarkMetrics;
import com.neo4j.bench.client.model.BenchmarkTool;
import com.neo4j.bench.client.model.BranchAndVersion;
import com.neo4j.bench.client.model.Edition;
import com.neo4j.bench.client.model.Environment;
import com.neo4j.bench.client.model.Java;
import com.neo4j.bench.client.model.Metrics;
import com.neo4j.bench.client.model.Neo4j;
import com.neo4j.bench.client.model.Neo4jConfig;
import com.neo4j.bench.client.model.TestRun;
import com.neo4j.bench.client.model.TestRunReport;
import com.neo4j.bench.client.queries.SubmitTestRun;
import org.neo4j.io.proc.ProcessUtil;
import org.neo4j.tooling.ImportTool;

import static java.util.stream.Collectors.joining;
import static com.neo4j.bench.client.model.Repository.IMPORT_BENCH;
import static com.neo4j.bench.client.model.Repository.NEO4J;

@Command( name = "import-benchmarks", description = "benchamrks for import performance" )
public class Main
{
    private static final String IMPORT_OWNER = "neo-technology";
    private static final String NEO4J_ENTERPRISE = "Enterprise";
    private static final String NEO4J_COMMUNITY = "Community";
    private static final String ARG_NEO4J_BRANCH = "--neo4j_branch";
    private static final String ARG_BRANCH_OWNER = "--branch_owner";
    private static final String CSV_LOCATION = "/mnt/ssds/csv/";
    @Option( type = OptionType.COMMAND,
             name = {"--results_store_user"},
             description = "Username for Neo4j database server that stores benchmarking results",
             title = "Results Store Username",
             required = true )
    private String resultsStoreUsername;
    @Option( type = OptionType.COMMAND,
             name = {"--results_store_pass"},
             description = "Password for Neo4j database server that stores benchmarking results",
             title = "Results Store Password",
             required = true )
    private String resultsStorePassword;
    @Option( type = OptionType.COMMAND,
             name = {"--results_store_uri"},
             description = "URI to Neo4j database server for storing benchmarking results",
             title = "Results Store",
             required = true )
    private URI resultsStoreUri;
    @Option( type = OptionType.COMMAND,
             name = {"--neo4j_commit"},
             description = "Commit of Neo4j that benchmark is run against",
             title = "Neo4j Commit",
             required = true )
    private String neo4jCommit;
    @Option( type = OptionType.COMMAND,
             name = {"--neo4j_version"},
             description = "Version of Neo4j that benchmark is run against (e.g., '3.0.2')",
             title = "Neo4j Version",
             required = true )
    private String neo4jVersion;
    @Option( type = OptionType.COMMAND,
             name = {"--neo4j_edition"},
             description = "Edition of Neo4j that benchmark is run against",
             title = "Neo4j Edition",
             allowedValues = {NEO4J_COMMUNITY, NEO4J_ENTERPRISE} )
    private Edition neo4jEdition = Edition.ENTERPRISE;

    @Option( type = OptionType.COMMAND,
             name = {ARG_NEO4J_BRANCH},
             description = "Neo4j branch name",
             title = "Neo4j Branch",
             required = true )
    private String neo4jBranch;

    @Option( type = OptionType.COMMAND,
             name = {ARG_BRANCH_OWNER},
             description = "Owner of repository containing Neo4j branch",
             title = "Branch Owner",
             required = true )
    private String neo4jBranchOwner;

    @Option( type = OptionType.COMMAND,
             name = {"--neo4j_config"},
             description = "Neo4j configuration used during benchmark",
             title = "Neo4j Configuration" )
    private File neo4jConfigFile;

    @Option( type = OptionType.COMMAND,
             name = {"--tool_commit"},
             description = "Commit of benchmarking tool used to run benchmark",
             title = "Benchmark Tool Commit",
             required = true )
    private String toolCommit;

    @Option( type = OptionType.COMMAND,
             name = {"--teamcity_parent_build"},
             description = "Build number of the TeamCity parent build that ran the packaging",
             title = "TeamCity Parent Build Number",
             required = true )
    private Long parentBuild;

    @Option( type = OptionType.COMMAND,
             name = {"--teamcity_build"},
             description = "Build number of the TeamCity build that ran the benchmarks",
             title = "TeamCity Build Number",
             required = true )
    private Long build;

    @Option( type = OptionType.COMMAND,
             name = {"--jvm_args"},
             description = "JVM arguments that benchmark was run with (e.g., '-XX:+UseG1GC -Xms4g -Xmx4g')",
             title = "JVM Args" )
    private String jvmArgs = "";

    private static final String BUCKET_NAME = "import-benchmarks-stores.neo4j.org";

    public static void main( String[] args ) throws Exception
    {
        Main runner = SingleCommand.singleCommand( Main.class ).parse( args );
        runner.run();
    }

    private void run() throws IOException, InterruptedException
    {
        BenchmarkGroupBenchmarkMetrics benchmarkGroupBenchmarkMetrics = new BenchmarkGroupBenchmarkMetrics();
        BenchmarkGroup group = new BenchmarkGroup( "Import" );
        Neo4jConfig neo4jConfig = (null == neo4jConfigFile) ? Neo4jConfig.empty() : Neo4jConfig.fromFile( neo4jConfigFile );
        long starttime = System.currentTimeMillis();

        String[] sizes = {"100m", "1bn", "10bn", "100bn"};
        for ( String size : sizes )
        {
            runImport( size, benchmarkGroupBenchmarkMetrics, group, neo4jConfig );
        }

        report( starttime, System.currentTimeMillis() - starttime, neo4jConfig, benchmarkGroupBenchmarkMetrics );
    }

    private static void compressDir( String rootDir, String sourceDir, ZipOutputStream out ) throws IOException
    {
        for ( File file : Objects.requireNonNull( new File( sourceDir ).listFiles() ) )
        {
            if ( file.isDirectory() )
            {
                compressDir( rootDir, sourceDir + File.separator + file.getName(), out );
            }
            else
            {
                ZipEntry entry = new ZipEntry( sourceDir.replace( rootDir, "" ) + file.getName() );
                out.putNextEntry( entry );

                try ( InputStream in = new BufferedInputStream( new FileInputStream( sourceDir + File.separator + file.getName() ) ) )
                {
                    IOUtils.copy( in, out );
                }
            }
        }
    }

    private void runImport( String size, BenchmarkGroupBenchmarkMetrics metrics, BenchmarkGroup group, Neo4jConfig neo4jConfig )
            throws IOException, InterruptedException
    {
        Benchmark benchmark = Benchmark.benchmarkFor( "import benchmark", size, size + "import", Benchmark.Mode.SINGLE_SHOT, new HashMap<>() );
        String[] importArgs = ("import --nodes " + CSV_LOCATION + size + "/nodes.csv --relationships " + CSV_LOCATION + size +
                               "/relationships.csv --bad-tolerance true --skip-bad-relationships true --skip-duplicate-nodes true --additional-config " +
                               CSV_LOCATION + size +
                               "/additional.conf --into " + size).split( " " );
        long starttime = System.currentTimeMillis();
        String[] pbArgs = {ProcessUtil.getJavaExecutable().toString(), "-cp", ProcessUtil.getClassPath(), ImportTool.class.getCanonicalName()};
        ProcessBuilder pb = new ProcessBuilder( Stream.of( pbArgs, importArgs ).flatMap( Stream::of ).toArray( String[]::new ) );
        pb.inheritIO();
        Process process = pb.start();
        process.waitFor();
        long time = System.currentTimeMillis() - starttime;
        Metrics runMetrics = new Metrics( TimeUnit.MILLISECONDS, time, time, time, 0, 1, 1, time, time, time, time, time, time, time );

        metrics.add( group, benchmark, runMetrics, neo4jConfig );
    }

    private void report( long start, long time, Neo4jConfig neo4jConfig, BenchmarkGroupBenchmarkMetrics metrics )
    {
        // trim anything like '-M01' from end of Neo4j version string
        neo4jVersion = BranchAndVersion.toSanitizeVersion( NEO4J, neo4jVersion );
        if ( !BranchAndVersion.isPersonalBranch( NEO4J, neo4jBranchOwner ) )
        {
            BranchAndVersion.assertBranchEqualsSeries( neo4jVersion, neo4jBranch );
        }
        BenchmarkTool tool = new BenchmarkTool( IMPORT_BENCH, toolCommit, IMPORT_OWNER, neo4jBranch );
        Java java = Java.current( Stream.of( jvmArgs ).collect( joining( " " ) ) );

        try ( StoreClient client = StoreClient.connect( resultsStoreUri, resultsStoreUsername, resultsStorePassword ) )
        {
            Neo4j neo4j = new Neo4j( neo4jCommit, neo4jVersion, neo4jEdition, neo4jBranch, neo4jBranchOwner );
            String id = UUID.randomUUID().toString();
            TestRun testRun = new TestRun( id, time, start, build, parentBuild, "import-benchmark" );
            TestRunReport report =
                    new TestRunReport( testRun, new BenchmarkConfig(), Sets.newHashSet( neo4j ), neo4jConfig, Environment.current(), metrics, tool, java,
                                       Lists.newArrayList() );
            SubmitTestRun submitTestRun = new SubmitTestRun( report );

            new QueryRetrier().execute( client, submitTestRun );
        }
        catch ( Exception e )
        {
            throw new RuntimeException( "Error submitting benchmark results to " + resultsStoreUri, e );
        }
    }
}
