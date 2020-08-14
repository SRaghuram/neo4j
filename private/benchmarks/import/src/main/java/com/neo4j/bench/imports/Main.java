/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
import com.neo4j.bench.client.env.InstanceDiscovery;
import com.neo4j.bench.client.queries.submit.SubmitTestRun;
import com.neo4j.bench.common.Neo4jConfigBuilder;
import com.neo4j.bench.common.options.Version;
import com.neo4j.bench.model.model.Benchmark;
import com.neo4j.bench.model.model.BenchmarkConfig;
import com.neo4j.bench.model.model.BenchmarkGroup;
import com.neo4j.bench.model.model.BenchmarkGroupBenchmarkMetrics;
import com.neo4j.bench.model.model.BenchmarkTool;
import com.neo4j.bench.model.model.BranchAndVersion;
import com.neo4j.bench.model.model.Environment;
import com.neo4j.bench.model.model.Instance;
import com.neo4j.bench.model.model.Java;
import com.neo4j.bench.model.model.Metrics;
import com.neo4j.bench.model.model.Neo4j;
import com.neo4j.bench.model.model.Neo4jConfig;
import com.neo4j.bench.model.model.TestRun;
import com.neo4j.bench.model.model.TestRunReport;
import com.neo4j.bench.model.options.Edition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.StringJoiner;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import org.neo4j.cli.AdminTool;
import org.neo4j.internal.helpers.collection.MapUtil;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.test.proc.ProcessUtil;

import static com.neo4j.bench.model.model.Repository.IMPORT_BENCH;
import static com.neo4j.bench.model.model.Repository.NEO4J;
import static java.util.stream.Collectors.joining;

@Command( name = "import-benchmarks", description = "benchmarks for import performance" )
public class Main
{
    private static final Logger LOG = LoggerFactory.getLogger( Main.class );

    private static final String[] sizes = {"100m", "1bn", "10bn", "100bn"};
    private static final String forceBlockBasedSize = "100bn";
    private static final String IMPORT_OWNER = "neo-technology";
    private static final String NEO4J_ENTERPRISE = "Enterprise";
    private static final String NEO4J_COMMUNITY = "Community";
    private static final String ARG_NEO4J_BRANCH = "--neo4j_branch";
    private static final String ARG_BRANCH_OWNER = "--branch_owner";
    private static final String ARG_REPORT_RESULT = "--report_result";

    @Option( type = OptionType.COMMAND,
            name = {ARG_REPORT_RESULT},
            description = "Should benchmark result be reported or not?",
            title = "Report result" )
    private String reportResult = "true";
    @Option( type = OptionType.COMMAND,
             name = {"--csv_location"},
             description = "Location for csv files",
             title = "Csv location" )
    @Required
    private String csvLocation = "";
    @Option( type = OptionType.COMMAND,
            name = {"--working_dir"},
            description = "Working directory for this benchmark where stores will be created.",
            title = "Working directory" )
    @Required
    private String workingDirName = "";
    @Option( type = OptionType.COMMAND,
             name = {"--results-store-user"},
             description = "Username for Neo4j database server that stores benchmarking results",
             title = "Results Store Username" )
    @Required
    private String resultsStoreUsername;
    @Option( type = OptionType.COMMAND,
             name = {"--results-store-pass"},
             description = "Password for Neo4j database server that stores benchmarking results",
             title = "Results Store Password" )
    @Required
    private String resultsStorePassword;
    @Option( type = OptionType.COMMAND,
             name = {"--results-store-uri"},
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

    public static void main( String[] args ) throws Exception
    {
        Main runner = SingleCommand.singleCommand( Main.class ).parse( args );
        runner.run();
    }

    private void run() throws IOException, InterruptedException
    {
        File workingDir = new File( workingDirName );
        File storeDir = new File( workingDir, "store" );
        File confDir = new File( workingDir, "conf" );
        File csvDir = new File( csvLocation );
        print( "workingDir = " + workingDir );
        print( "storeDir = " + storeDir );
        print( "confDir = " + confDir );
        print( "csvDir = " + csvDir );

        final DefaultFileSystemAbstraction fs = new DefaultFileSystemAbstraction();
        fs.mkdirs( storeDir.toPath() );
        fs.mkdirs( confDir.toPath() );
        if ( !fs.isDirectory( csvDir.toPath() ) )
        {
            throw new RuntimeException( "Csv directory doesn't exist, dir=" + csvDir.getAbsolutePath() );
        }

        print( "Setup benchmark group" );
        BenchmarkGroup importGroup = new BenchmarkGroup( "Import" );
        BenchmarkGroup indexGroup = new BenchmarkGroup( "Index" );
        print( "Setup Neo4jConfig" );
        Neo4jConfig neo4jConfig = (null == neo4jConfigFile) ? Neo4jConfig.empty() : Neo4jConfigBuilder.fromFile( neo4jConfigFile ).build();
        print( "Neo4jConfig: " + neo4jConfig );

        for ( String size : sizes )
        {
            String databaseName = "db" + size;
            long startTime = System.currentTimeMillis();
            BenchmarkGroupBenchmarkMetrics benchmarkGroupBenchmarkMetrics = new BenchmarkGroupBenchmarkMetrics();
            print( "Start import " + databaseName );
            int exitCode = runImport( size, storeDir, confDir, csvDir, databaseName, benchmarkGroupBenchmarkMetrics, importGroup, neo4jConfig );
            print( "End import " + databaseName );
            if ( exitCode == 0 )
            {
                print( "Start index population " + databaseName );
                createIndexes( size, storeDir, databaseName, benchmarkGroupBenchmarkMetrics, indexGroup, neo4jConfig );
                print( "End index population " + databaseName );
            }

            if ( Boolean.parseBoolean( reportResult ) )
            {
                report( startTime, System.currentTimeMillis() - startTime, neo4jConfig, benchmarkGroupBenchmarkMetrics );
            }
        }
    }

    private static void print( String message )
    {
        System.out.println( "[Main] " + message );
    }

    // nodes.csv header - :ID,:LABEL,name:string,nr:int,date:long,rank:string,other:int
    private static int createIndexes( String size, File storeDir, String databaseName, BenchmarkGroupBenchmarkMetrics metrics, BenchmarkGroup group,
            Neo4jConfig neo4jConfig ) throws IOException, InterruptedException
    {
        String name = "indexCreate" + size;
        Benchmark benchmark = Benchmark.benchmarkFor( "Index population on large store", name, name, Benchmark.Mode.SINGLE_SHOT, new HashMap<>() );
        StringJoiner indexPatterns = new StringJoiner( " " );
        indexPatterns.add( "Label1:name" );
        indexPatterns.add( "Label1:name,other" );
        indexPatterns.add( "Label2:nr" );
        indexPatterns.add( "Label2:nr,other" );
        indexPatterns.add( "Label3:date" );
        indexPatterns.add( "Label3:date,other" );
        indexPatterns.add( "Label4:rank" );
        indexPatterns.add( "Label4:rank,other" );
        String[] additionalJvmArgs = new String[0];
        if ( forceBlockBasedSize.equals( size ) )
        {
            additionalJvmArgs = new String[]{"-Dorg.neo4j.kernel.impl.index.schema.GenericNativeIndexPopulator.blockBasedPopulation=true"};
        }
        String[] indexCreateArgs =
                (String.format( "--storeDir %s --database %s %s", storeDir.getAbsolutePath(), databaseName, indexPatterns.toString() )).split( " " );
        Class<CreateIndex> targetClass = CreateIndex.class;
        return runProcess( metrics, group, neo4jConfig, benchmark, indexCreateArgs, additionalJvmArgs, Collections.emptyMap(), targetClass );
    }

    private static int runImport( String size, File storeDir, File confDir, File csvDir, String databaseName, BenchmarkGroupBenchmarkMetrics metrics,
            BenchmarkGroup group, Neo4jConfig neo4jConfig ) throws IOException, InterruptedException
    {
        Benchmark benchmark = Benchmark.benchmarkFor( "import benchmark", size, size + "import", Benchmark.Mode.SINGLE_SHOT, new HashMap<>() );
        File csvSizeDir = new File( csvDir, size );
        final File nodesCsv = new File( csvSizeDir, "nodes.csv" );
        final File relationshipsCsv = new File( csvSizeDir, "relationships.csv" );
        final File additionalCsv = new File( csvSizeDir, "additional.conf" );
        String[] importArgs = {
                "import",
                "--database", databaseName,
                "--nodes", nodesCsv.getAbsolutePath(),
                "--relationships", relationshipsCsv.getAbsolutePath(),
                "--skip-bad-relationships", String.valueOf( true ),
                "--skip-duplicate-nodes", String.valueOf( true ),
                "--additional-config", additionalCsv.getAbsolutePath()
        };
        Map<String,String> neo4jHomeEnvironment = MapUtil.stringMap(
                "NEO4J_HOME", storeDir.getAbsolutePath(),
                "NEO4J_CONF", confDir.getAbsolutePath() );
        Class<AdminTool> targetClass = AdminTool.class;
        return runProcess( metrics, group, neo4jConfig, benchmark, importArgs, new String[0], neo4jHomeEnvironment, targetClass );
    }

    private static int runProcess( BenchmarkGroupBenchmarkMetrics metrics, BenchmarkGroup group, Neo4jConfig neo4jConfig, Benchmark benchmark,
            String[] programArgs, String[] jvmArgs, Map<String,String> environmentVariables, Class<?> targetClass ) throws IOException, InterruptedException
    {
        long startTime = System.currentTimeMillis();
        String[] pbArgs = processBuilderArguments( targetClass, programArgs, jvmArgs );
        ProcessBuilder pb = new ProcessBuilder( pbArgs );
        pb.environment().putAll( environmentVariables );
        pb.inheritIO();
        Process process = pb.start();
        int exitCode = process.waitFor();
        if ( exitCode == 0 )
        {
            long time = System.currentTimeMillis() - startTime;
            Metrics runMetrics = new Metrics( TimeUnit.MILLISECONDS, time, time, time, 1, time, time, time, time, time, time, time );

            metrics.add( group, benchmark, runMetrics, null /*no auxiliary metrics*/, neo4jConfig );
        }
        return exitCode;
    }

    private static String[] processBuilderArguments( Class<?> targetClass, String[] programArgs, String[] jvmArgs )
    {
        List<String> pbArgs = new ArrayList<>();
        pbArgs.add( ProcessUtil.getJavaExecutable().toString() );
        pbArgs.add( "-cp" );
        pbArgs.add( ProcessUtil.getClassPath() );
        pbArgs.addAll( Arrays.asList( jvmArgs ) );
        pbArgs.add( targetClass.getCanonicalName() );
        pbArgs.addAll( Arrays.asList( programArgs ) );
        return pbArgs.toArray( new String[0] );
    }

    private void report( long start, long time, Neo4jConfig neo4jConfig, BenchmarkGroupBenchmarkMetrics metrics )
    {
        // trim anything like '-M01' from end of Neo4j version string
        neo4jVersion = Version.toSanitizeVersion( neo4jVersion );
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

            InstanceDiscovery instanceDiscovery = InstanceDiscovery.create();
            Instance instance = instanceDiscovery.currentInstance( System.getenv() );

            TestRunReport report =
                    new TestRunReport( testRun,
                                       new BenchmarkConfig(),
                                       Sets.newHashSet( neo4j ),
                                       neo4jConfig,
                                       Environment.from( instance ),
                                       metrics,
                                       tool,
                                       java,
                                       Lists.newArrayList() );
            SubmitTestRun submitTestRun = new SubmitTestRun( report );
            LOG.debug( "Test run reported: " + report );

            new QueryRetrier( true ).execute( client, submitTestRun );
        }
        catch ( Exception e )
        {
            throw new RuntimeException( "Error submitting benchmark results to " + resultsStoreUri, e );
        }
    }
}
