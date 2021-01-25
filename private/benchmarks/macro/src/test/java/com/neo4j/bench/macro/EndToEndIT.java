/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.macro;

import com.neo4j.bench.common.Neo4jConfigBuilder;
import com.neo4j.bench.common.database.Store;
import com.neo4j.bench.common.options.Planner;
import com.neo4j.bench.common.options.Runtime;
import com.neo4j.bench.common.profiling.FullBenchmarkName;
import com.neo4j.bench.common.profiling.ParameterizedProfiler;
import com.neo4j.bench.common.profiling.ProfilerRecordingDescriptor;
import com.neo4j.bench.common.profiling.ProfilerType;
import com.neo4j.bench.common.profiling.RecordingDescriptor;
import com.neo4j.bench.common.results.RunPhase;
import com.neo4j.bench.common.tool.macro.Deployment;
import com.neo4j.bench.common.tool.macro.DeploymentModes;
import com.neo4j.bench.common.tool.macro.ExecutionMode;
import com.neo4j.bench.common.util.ErrorReporter.ErrorPolicy;
import com.neo4j.bench.common.util.Jvm;
import com.neo4j.bench.common.util.Resources;
import com.neo4j.bench.macro.execution.Neo4jDeployment;
import com.neo4j.bench.macro.workload.Query;
import com.neo4j.bench.macro.workload.Workload;
import com.neo4j.bench.model.model.Parameters;
import com.neo4j.bench.model.options.Edition;
import com.neo4j.bench.model.profiling.RecordingType;
import com.neo4j.bench.test.BaseEndToEndIT;
import org.apache.commons.io.FileUtils;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

import static java.lang.String.format;
import static java.util.Arrays.asList;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.stream.Collectors.joining;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.io.FileMatchers.anExistingFile;

class EndToEndIT extends BaseEndToEndIT
{

    private static final String LOAD_CSV_WORKLOAD = "cineasts_csv";
    private static final String WRITE_WORKLOAD = "pokec_write";
    private static final String READ_WORKLOAD = "zero";

    private ExpectedRecordings expectedRecordingsFor( List<ProfilerType> profilers, ExecutionMode executionMode, Deployment deployment, int forks )
    {
        ExpectedRecordings expectedRecordings = ExpectedRecordings.from( profilers, deploymentParameters( deployment.deploymentModes(), forks ) );
        return (ExecutionMode.CARDINALITY.equals( executionMode ))
               ? expectedRecordings.with( RecordingType.ASCII_PLAN, clientParameters( deployment.deploymentModes() ) )
               : expectedRecordings;
    }

    @Test
    public void runReadWorkloadEmbedded() throws Exception
    {
        List<ProfilerType> profilers = asList( ProfilerType.JFR, ProfilerType.GC );
        Deployment deployment = Deployment.embedded();
        String workloadName = READ_WORKLOAD;
        ExecutionMode executionMode = ExecutionMode.CARDINALITY;
        int recordingDirsCount = 1;
        int forks = 1;

        Path resourcesPath = temporaryFolder.resolve( "resources" );
        Files.createDirectories( resourcesPath );
        try ( Resources resources = new Resources( resourcesPath ) )
        {
            runReportBenchmarks( resources,
                                 scriptName(),
                                 getJar(),
                                 profilers,
                                 processArgs( resources,
                                              profilers,
                                              workloadName,
                                              deployment,
                                              forks,
                                              executionMode ),
                                 assertOnRecordings( deployment, workloadName, forks, executionMode ),
                                 recordingDirsCount,
                                 expectedRecordingsFor( profilers, executionMode, deployment, forks ) );
        }
    }

    @Disabled( "long running test" )
    @Test
    public void runWriteWorkloadForkedWithEmbedded() throws Exception
    {

        List<ProfilerType> profilers = asList( ProfilerType.JFR, ProfilerType.GC );
        Deployment deployment = Deployment.embedded();
        String workloadName = WRITE_WORKLOAD;
        ExecutionMode executionMode = ExecutionMode.EXECUTE;
        int recordingDirsCount = 1;
        int forks = 1;
        Path resourcesPath = temporaryFolder.resolve( "resources" );
        Files.createDirectories( resourcesPath );
        try ( Resources resources = new Resources( resourcesPath ) )
        {
            runReportBenchmarks( resources,
                                 scriptName(),
                                 getJar(),
                                 profilers,
                                 processArgs( resources,
                                              profilers,
                                              workloadName,
                                              deployment,
                                              forks,
                                              executionMode ),
                                 assertOnRecordings( deployment, workloadName, forks, executionMode ),
                                 recordingDirsCount,
                                 expectedRecordingsFor( profilers, executionMode, deployment, forks ) );
        }
    }

    @Disabled( "timed out after 20 minutes" )
    @Test
    public void executeLoadCsvWorkloadForkedWithEmbedded() throws Exception
    {
        List<ProfilerType> profilers = asList( ProfilerType.JFR, ProfilerType.GC );
        Deployment deployment = Deployment.embedded();
        String workloadName = LOAD_CSV_WORKLOAD;
        ExecutionMode executionMode = ExecutionMode.EXECUTE;
        int recordingDirsCount = 1;
        int forks = 1;
        Path resourcesPath = temporaryFolder.resolve( "resources" );
        Files.createDirectories( resourcesPath );
        try ( Resources resources = new Resources( resourcesPath ) )
        {
            runReportBenchmarks( resources,
                                 scriptName(),
                                 getJar(),
                                 profilers,
                                 processArgs( resources,
                                              profilers,
                                              workloadName,
                                              deployment,
                                              forks,
                                              executionMode ),
                                 assertOnRecordings( deployment, workloadName, forks, executionMode ),
                                 recordingDirsCount,
                                 expectedRecordingsFor( profilers, executionMode, deployment, forks ) );
        }
    }

    // <><><><><><><><><><><><> Forked - Server <><><><><><><><><><><><>

    @Test
    public void executeReadWorkloadForkedWithServer() throws Exception
    {

        List<ProfilerType> profilers = asList( ProfilerType.JFR, ProfilerType.GC );
        Deployment deployment = Deployment.server( getNeo4jDir() );
        String workloadName = READ_WORKLOAD;
        ExecutionMode executionMode = ExecutionMode.CARDINALITY;
        int recordingDirsCount = 1;
        int forks = 1;
        Path resourcesPath = temporaryFolder.resolve( "resources" );
        Files.createDirectories( resourcesPath );
        try ( Resources resources = new Resources( resourcesPath ) )
        {
            runReportBenchmarks( resources,
                                 scriptName(),
                                 getJar(),
                                 profilers,
                                 processArgs( resources,
                                              profilers,
                                              workloadName,
                                              deployment,
                                              forks,
                                              executionMode ),
                                 assertOnRecordings( deployment, workloadName, forks, executionMode ),
                                 recordingDirsCount,
                                 expectedRecordingsFor( profilers, executionMode, deployment, forks ) );
        }
    }

    @Disabled( "long running test" )
    @Test
    public void executeWriteWorkloadsForkedWithServer() throws Exception
    {
        List<ProfilerType> profilers = asList( ProfilerType.JFR, ProfilerType.GC );
        Deployment deployment = Deployment.server( getNeo4jDir() );
        String workloadName = WRITE_WORKLOAD;
        ExecutionMode executionMode = ExecutionMode.EXECUTE;
        int recordingDirsCount = 1;
        int forks = 1;

        Path resourcesPath = temporaryFolder.resolve( "resources" );
        Files.createDirectories( resourcesPath );
        try ( Resources resources = new Resources( resourcesPath ) )
        {
            runReportBenchmarks( resources,
                                 scriptName(),
                                 getJar(),
                                 profilers,
                                 processArgs( resources,
                                              profilers,
                                              workloadName,
                                              deployment,
                                              forks,
                                              executionMode ),
                                 assertOnRecordings( deployment, workloadName, forks, executionMode ),
                                 recordingDirsCount,
                                 expectedRecordingsFor( profilers, executionMode, deployment, forks ) );
        }
    }

    @Disabled( "Executing queries that use periodic commit in an open transaction is not possible" )
    @Test
    public void executeLoadCsvWorkloadsForkedWithServer() throws Exception
    {
        List<ProfilerType> profilers = asList( ProfilerType.JFR, ProfilerType.GC );
        Deployment deployment = Deployment.server( getNeo4jDir() );
        String workloadName = LOAD_CSV_WORKLOAD;
        ExecutionMode executionMode = ExecutionMode.EXECUTE;
        int recordingDirsCount = 1;
        int forks = 1;

        Path resourcesPath = temporaryFolder.resolve( "resources" );
        Files.createDirectories( resourcesPath );
        try ( Resources resources = new Resources( resourcesPath ) )
        {
            runReportBenchmarks( resources,
                                 scriptName(),
                                 getJar(),
                                 profilers,
                                 processArgs( resources,
                                              profilers,
                                              workloadName,
                                              deployment,
                                              forks,
                                              executionMode ),
                                 assertOnRecordings( deployment, workloadName, forks, executionMode ),
                                 recordingDirsCount,
                                 expectedRecordingsFor( profilers, executionMode, deployment, forks ) );
        }
    }

    // <><><><><><><><><><><><> In-process - Embedded <><><><><><><><><><><><>

    @Test
    public void executeReadWorkloadInProcessWithEmbedded() throws Exception
    {

        List<ProfilerType> profilers = asList( ProfilerType.JFR, ProfilerType.GC );
        Deployment deployment = Deployment.embedded();
        String workloadName = READ_WORKLOAD;
        ExecutionMode executionMode = ExecutionMode.EXECUTE;
        int recordingDirsCount = 0;
        int forks = 0;

        Path resourcesPath = temporaryFolder.resolve( "resources" );
        Files.createDirectories( resourcesPath );
        try ( Resources resources = new Resources( resourcesPath ) )
        {
            runReportBenchmarks( resources,
                                 scriptName(),
                                 getJar(),
                                 profilers,
                                 processArgs( resources,
                                              profilers,
                                              workloadName,
                                              deployment,
                                              forks,
                                              executionMode ),
                                 assertOnRecordings( deployment, workloadName, forks, executionMode ),
                                 recordingDirsCount,
                                 expectedRecordingsFor( profilers, executionMode, deployment, forks ) );
        }
    }

    @Disabled( "long running test" )
    @Test
    public void executeWriteWorkloadInProcessWithEmbedded() throws Exception
    {
        List<ProfilerType> profilers = asList( ProfilerType.JFR, ProfilerType.GC );
        Deployment deployment = Deployment.embedded();
        String workloadName = WRITE_WORKLOAD;
        ExecutionMode executionMode = ExecutionMode.EXECUTE;
        int forks = 0;
        int recordingDirsCount = 0;

        try ( Resources resources = new Resources( temporaryFolder.resolve( "resources" ) ) )
        {
            runReportBenchmarks( resources,
                                 scriptName(),
                                 getJar(),
                                 profilers,
                                 processArgs( resources,
                                              profilers,
                                              workloadName,
                                              deployment,
                                              forks,
                                              executionMode ),
                                 assertOnRecordings( deployment, workloadName, forks, executionMode ),
                                 recordingDirsCount,
                                 expectedRecordingsFor( profilers, executionMode, deployment, forks ) );
        }
    }

    @Disabled( "timed out after 20 minutes" )
    @Test
    public void executeLoadCsvWorkloadInProcessWithEmbedded() throws Exception
    {

        List<ProfilerType> profilers = asList( ProfilerType.JFR, ProfilerType.GC );
        Deployment deployment = Deployment.embedded();
        String workloadName = LOAD_CSV_WORKLOAD;
        ExecutionMode executionMode = ExecutionMode.EXECUTE;
        int forks = 0;
        int recordingDirsCount = 0;

        try ( Resources resources = new Resources( temporaryFolder.resolve( "resources" ) ) )
        {
            runReportBenchmarks( resources,
                                 scriptName(),
                                 getJar(),
                                 profilers,
                                 processArgs( resources,
                                              profilers,
                                              workloadName,
                                              deployment,
                                              forks,
                                              executionMode ),
                                 assertOnRecordings( deployment, workloadName, forks, executionMode ),
                                 recordingDirsCount,
                                 expectedRecordingsFor( profilers, executionMode, deployment, forks ) );
        }
    }

    // <><><><><><><><><><><><> In-process - Server <><><><><><><><><><><><>
    @Test
    public void executeReadWorkloadInProcessWithServer() throws Exception
    {

        List<ProfilerType> profilers = asList( ProfilerType.JFR, ProfilerType.GC );
        Deployment deployment = Deployment.server( getNeo4jDir() );
        String workloadName = READ_WORKLOAD;
        ExecutionMode executionMode = ExecutionMode.CARDINALITY;
        int forks = 0;
        int recordingDirsCount = 1;
        Path resourcesPath = temporaryFolder.resolve( "resources" );
        Files.createDirectories( resourcesPath );
        try ( Resources resources = new Resources( resourcesPath ) )
        {
            runReportBenchmarks( resources,
                                 scriptName(),
                                 getJar(),
                                 profilers,
                                 processArgs( resources,
                                              profilers,
                                              workloadName,
                                              deployment,
                                              forks,
                                              executionMode ),
                                 assertOnRecordings( deployment, workloadName, forks, executionMode ),
                                 recordingDirsCount,
                                 expectedRecordingsFor( profilers, executionMode, deployment, forks ) );
        }
    }

    @Disabled( "long running test" )
    @Test
    public void executeWriteWorkloadInProcessWithServer() throws Exception
    {

        List<ProfilerType> profilers = asList( ProfilerType.JFR, ProfilerType.GC );
        Deployment deployment = Deployment.server( getNeo4jDir() );
        String workloadName = WRITE_WORKLOAD;
        ExecutionMode executionMode = ExecutionMode.EXECUTE;
        int forks = 0;
        int recordingDirsCount = 0;

        try ( Resources resources = new Resources( temporaryFolder.resolve( "resources" ) ) )
        {
            runReportBenchmarks( resources,
                                 scriptName(),
                                 getJar(),
                                 profilers,
                                 processArgs( resources,
                                              profilers,
                                              workloadName,
                                              deployment,
                                              forks,
                                              executionMode ),
                                 assertOnRecordings( deployment, workloadName, forks, executionMode ),
                                 recordingDirsCount,
                                 expectedRecordingsFor( profilers, executionMode, deployment, forks ) );
        }
    }

    @Disabled( "Executing queries that use periodic commit in an open transaction is not possible" )
    @Test
    public void executeLoadCsvWorkloadInProcessWithServer() throws Exception
    {

        List<ProfilerType> profilers = asList( ProfilerType.JFR, ProfilerType.GC );
        Deployment deployment = Deployment.server( getNeo4jDir() );
        String workloadName = LOAD_CSV_WORKLOAD;
        int forks = 0;
        int recordingDirsCount = 0;

        try ( Resources resources = new Resources( temporaryFolder.resolve( "resources" ) ) )
        {
            ExecutionMode executionMode = ExecutionMode.EXECUTE;
            runReportBenchmarks( resources,
                                 scriptName(),
                                 getJar(),
                                 profilers,
                                 processArgs( resources,
                                              profilers,
                                              workloadName,
                                              deployment,
                                              forks,
                                              executionMode ),
                                 assertOnRecordings( deployment, workloadName, forks, executionMode ),
                                 recordingDirsCount,
                                 expectedRecordingsFor( profilers, executionMode, deployment, forks ) );
        }
    }

    private static String scriptName()
    {
        return "run-report-benchmarks.sh";
    }

    private static Path getJar()
    {
        return Paths.get( "target/macro.jar" );
    }

    private String neo4jDirPathString;

    private String getNeo4jDir() throws IOException
    {
        if ( neo4jDirPathString == null )
        {
            Path neo4jDir = Paths.get( System.getenv( "NEO4J_DIR" ) );
            Path tempNeo4jDir = temporaryFolder.resolve( format( "neo4jDir-%s", UUID.randomUUID().toString() ) );
            FileUtils.copyDirectory( neo4jDir.toFile(), tempNeo4jDir.toFile() );
            neo4jDirPathString = tempNeo4jDir.toAbsolutePath().toString();
        }
        return neo4jDirPathString;
    }

    private List<String> processArgs( Resources resources,
                                      List<ProfilerType> profilers,
                                      String workloadName,
                                      Deployment deployment,
                                      int forks,
                                      ExecutionMode executionMode ) throws IOException
    {

        Jvm jvm = Jvm.defaultJvmOrFail();
        String awsEndpointUrl = getAWSEndpointURL();
        ResultStoreCredentials resultStoreCredentials = getResultStoreCredentials();
        // prepare neo4j config file
        Path neo4jConfig = temporaryFolder.resolve( "neo4j.config" );
        Neo4jConfigBuilder.withDefaults().writeToFile( neo4jConfig );

        // create empty store
        Path dbPath = temporaryFolder.resolve( "db" );
        Files.createDirectories(dbPath);
        Workload workload = Workload.fromName( workloadName, resources, deployment );
        Store emptyStoreFor = StoreTestUtil.createEmptyStoreFor( workload,
                                                                 dbPath, // store
                                                                 neo4jConfig );

        Neo4jDeployment neo4jDeployment = Neo4jDeployment.from( deployment );

        return asList( "./" + scriptName(),
                       // workload
                       workloadName,
                       // db
                       emptyStoreFor.topLevelDirectory().toString(),
                       // warmup_count
                       "1",
                       // measurement_count
                       "1",
                       // db_edition
                       Edition.ENTERPRISE.name(),
                       // jvm
                       jvm.launchJava(),
                       // neo4j_config
                       neo4jConfig.toString(),
                       // work_dir
                       temporaryFolder.toString(),
                       // profilers
                       profilers.stream().map( ProfilerType::name ).collect( joining( "," ) ),
                       // forks
                       Integer.toString( forks ),
                       // time_unit
                       MILLISECONDS.name(),
                       resultStoreCredentials.boltUri(),
                       resultStoreCredentials.user(),
                       resultStoreCredentials.pass(),
                       "neo4j_commit",
                       // neo4j_version
                       "3.5.1",
                       "neo4j_branch",
                       "neo4j_branch_owner",
                       // teamcity_build
                       "1",
                       // parent_teamcity_build
                       "0",
                       // execution_mode
                       executionMode.name(),
                       // jvm_args
                       "-Xmx1g",
                       // recreate_schema
                       "false",
                       // planner
                       Planner.DEFAULT.name(),
                       // runtime
                       Runtime.DEFAULT.name(),
                       "triggered_by",
                       // error_policy
                       ErrorPolicy.FAIL.name(),
                       // embedded OR server:<path>
                       neo4jDeployment.toString(),
                       "",
                       //Batch Job id
                       "123",
                       // AWS endpoint URL
                       "--aws-endpoint-url", awsEndpointUrl );
    }

    protected AssertOnRecordings assertOnRecordings( Deployment deployment, String workloadName, int forks, ExecutionMode executionMode )
    {
        return ( Path recordingDir, List<ProfilerType> profilers, Resources resources ) ->
        {
            Workload workload = Workload.fromName( workloadName, resources, deployment );
            // should find at least one recording per profiler per benchmark -- there may be more, due to secondary recordings
            int profilerRecordingCount = (int) Files.list( recordingDir ).count();
            int minimumExpectedProfilerRecordingCount = profilers.size() * workload.queries().size();
            assertThat( profilerRecordingCount, greaterThanOrEqualTo( minimumExpectedProfilerRecordingCount ) );
            Parameters[] parameters = deploymentParameters( deployment.deploymentModes(), forks );
            for ( Query query : workload.queries() )
            {
                query = query.copyWith( executionMode );
                for ( ProfilerType profilerType : profilers )
                {
                    for ( Parameters params : parameters )
                    {
                        ProfilerRecordingDescriptor profilerRecordingDescriptor = ProfilerRecordingDescriptor.create(
                                query.benchmarkGroup(),
                                query.benchmark(),
                                RunPhase.MEASUREMENT,
                                ParameterizedProfiler.defaultProfiler( profilerType ),
                                params );
                        for ( RecordingType recordingType : profilerType.allRecordingTypes() )
                        {
                            RecordingDescriptor recordingDescriptor = profilerRecordingDescriptor.recordingDescriptorFor( recordingType );
                            String profilerArtifactFilename = recordingDescriptor.filename();
                            File file = recordingDir.resolve( profilerArtifactFilename ).toFile();
                            assertThat( "File not found: " + file.getAbsolutePath(), file, anExistingFile() );
                        }
                    }
                }

                Parameters clientParameters = clientParameters( deployment.deploymentModes() ); // ASCII plans are only created on client side
                if ( executionMode.equals( ExecutionMode.CARDINALITY ) )
                {
                    RecordingDescriptor recordingDescriptor = new RecordingDescriptor( FullBenchmarkName.from( query.benchmarkGroup(), query.benchmark() ),
                                                                                       RunPhase.MEASUREMENT,
                                                                                       RecordingType.ASCII_PLAN,
                                                                                       clientParameters,
                                                                                       Collections.emptySet(),
                                                                                       true /*every fork will produce plans recording*/ );
                    String profilerArtifactFilename = recordingDescriptor.filename();
                    File file = recordingDir.resolve( profilerArtifactFilename ).toFile();
                    assertThat( "File not found: " + file.getAbsolutePath(), file, anExistingFile() );
                }
            }
        };
    }

    private static Parameters clientParameters( DeploymentModes deploymentModes )
    {
        switch ( deploymentModes )
        {
        case SERVER:
            return Parameters.CLIENT;
        case EMBEDDED:
            return Parameters.NONE;
        default:
            throw new IllegalArgumentException( format( "unsupported deployment mode %s", deploymentModes ) );
        }
    }

    private static Parameters[] deploymentParameters( DeploymentModes deploymentModes, int forks )
    {
        switch ( deploymentModes )
        {
        case SERVER:
            if ( forks == 0 )
            {
                return new Parameters[]{Parameters.SERVER};
            }
            return new Parameters[]{Parameters.CLIENT, Parameters.SERVER};
        case EMBEDDED:
            return new Parameters[]{Parameters.NONE};
        default:
            throw new IllegalArgumentException( format( "unsupported deployment mode %s", deploymentModes ) );
        }
    }
}
