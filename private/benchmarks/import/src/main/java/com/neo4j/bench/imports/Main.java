/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.imports;

import com.github.rvesse.airline.SingleCommand;
import com.github.rvesse.airline.annotations.Command;
import com.github.rvesse.airline.annotations.Option;
import com.github.rvesse.airline.annotations.OptionType;
import com.github.rvesse.airline.annotations.restrictions.AllowedEnumValues;
import com.github.rvesse.airline.annotations.restrictions.Required;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.neo4j.bench.client.QueryRetrier;
import com.neo4j.bench.client.StoreClient;
import com.neo4j.bench.client.queries.SubmitTestRun;
import com.neo4j.bench.common.Neo4jConfigBuilder;
import com.neo4j.bench.common.model.Benchmark;
import com.neo4j.bench.common.model.BenchmarkConfig;
import com.neo4j.bench.common.model.BenchmarkGroup;
import com.neo4j.bench.common.model.BenchmarkGroupBenchmarkMetrics;
import com.neo4j.bench.common.model.BenchmarkTool;
import com.neo4j.bench.common.model.BranchAndVersion;
import com.neo4j.bench.common.model.Environment;
import com.neo4j.bench.common.model.Java;
import com.neo4j.bench.common.model.Metrics;
import com.neo4j.bench.common.model.Neo4j;
import com.neo4j.bench.common.model.Neo4jConfig;
import com.neo4j.bench.common.model.TestRun;
import com.neo4j.bench.common.model.TestRunReport;
import com.neo4j.bench.common.options.Edition;
import org.apache.commons.compress.utils.IOUtils;
import org.neo4j.io.proc.ProcessUtil;
import org.neo4j.tooling.ImportTool;

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

import static com.neo4j.bench.common.model.Repository.IMPORT_BENCH;
import static com.neo4j.bench.common.model.Repository.NEO4J;
import static java.util.stream.Collectors.joining;

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
             title = "Results Store Username" )
    @Required
    private String resultsStoreUsername;
    @Option( type = OptionType.COMMAND,
             name = {"--results_store_pass"},
             description = "Password for Neo4j database server that stores benchmarking results",
             title = "Results Store Password" )
    @Required
    private String resultsStorePassword;
    @Option( type = OptionType.COMMAND,
             name = {"--results_store_uri"},
             description = "URI to Neo4j database server for storing benchmarking results",
             title = "Results Store" )
    @Required
    private URI resultsStoreUri;
    @Option( type = OptionType.COMMAND,
             name = {"--neo4j_commit"},
             description = "Commit of Neo4j that benchmark is run against",
             title = "Neo4j Commit" )
    @Required
    private String neo4jCommit;
    @Option( type = OptionType.COMMAND,
             name = {"--neo4j_version"},
             description = "Version of Neo4j that benchmark is run against (e.g., '3.0.2')",
             title = "Neo4j Version" )
    @Required
    private String neo4jVersion;
    @Option( type = OptionType.COMMAND,
             name = {"--neo4j_edition"},
             description = "Edition of Neo4j that benchmark is run against",
             title = "Neo4j Edition" )
    @AllowedEnumValues( Edition.class )
    private Edition neo4jEdition = Edition.ENTERPRISE;

    @Option( type = OptionType.COMMAND,
             name = {ARG_NEO4J_BRANCH},
             description = "Neo4j branch name",
             title = "Neo4j Branch" )
    @Required
    private String neo4jBranch;

    @Option( type = OptionType.COMMAND,
             name = {ARG_BRANCH_OWNER},
             description = "Owner of repository containing Neo4j branch",
             title = "Branch Owner" )
    @Required
    private String neo4jBranchOwner;

    @Option( type = OptionType.COMMAND,
             name = {"--neo4j_config"},
             description = "Neo4j configuration used during benchmark",
             title = "Neo4j Configuration" )
    private File neo4jConfigFile;

    @Option( type = OptionType.COMMAND,
             name = {"--tool_commit"},
             description = "Commit of benchmarking tool used to run benchmark",
             title = "Benchmark Tool Commit" )
    @Required
    private String toolCommit;

    @Option( type = OptionType.COMMAND,
             name = {"--teamcity_parent_build"},
             description = "Build number of the TeamCity parent build that ran the packaging",
             title = "TeamCity Parent Build Number" )
    @Required
    private Long parentBuild;

    @Option( type = OptionType.COMMAND,
             name = {"--teamcity_build"},
             description = "Build number of the TeamCity build that ran the benchmarks",
             title = "TeamCity Build Number" )
    @Required
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
        Neo4jConfig neo4jConfig = (null == neo4jConfigFile) ? Neo4jConfig.empty() : Neo4jConfigBuilder.fromFile( neo4jConfigFile ).build();
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

            new QueryRetrier( true ).execute( client, submitTestRun );
        }
        catch ( Exception e )
        {
            throw new RuntimeException( "Error submitting benchmark results to " + resultsStoreUri, e );
        }
    }
}
