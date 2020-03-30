/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.macro;

import com.neo4j.bench.common.Neo4jConfigBuilder;
import com.neo4j.bench.common.options.Planner;
import com.neo4j.bench.common.options.Runtime;
import com.neo4j.bench.common.profiling.ProfilerRecordingDescriptor;
import com.neo4j.bench.common.profiling.ProfilerType;
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
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

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

    // this is temporary workaround for
    // https://trello.com/c/VKR4yx64/4282-neo4j-admin-scripts-should-respect-javahome-env-var-and-ideally-print-which-jvm-theyre-running-with
    // thanks to this we can have benchmark working directory without space in path
    @TempDir
    Path workingDir;

    @Test
    public void runZeroWorkloadEmbedded() throws Exception
    {

        List<ProfilerType> profilers = asList( ProfilerType.JFR, ProfilerType.ASYNC, ProfilerType.GC );
        Deployment deployment = Deployment.embedded();
        String workloadName = "zero";
        int recordingDirsCount = 1;
        int forks = 1;

        try ( Resources resources = new Resources( temporaryFolder.directory( "resources" ).toPath() ) )
        {
            runReportBenchmarks( resources,
                                 scriptName(),
                                 getJar(),
                                 profilers,
                                 processArgs( resources,
                                              profilers,
                                              workloadName,
                                              deployment,
                                              forks ),
                                 assertOnRecordings( deployment, workloadName, forks ),
                                 recordingDirsCount );
        }
    }

    @Test
    @Disabled( "long running test" )
    public void runWriteWorkloadForkedWithEmbedded() throws Exception
    {

        List<ProfilerType> profilers = asList( ProfilerType.JFR, ProfilerType.GC );
        Deployment deployment = Deployment.embedded();
        String workloadName = "pokec_write";
        int recordingDirsCount = 1;
        int forks = 1;

        try ( Resources resources = new Resources( temporaryFolder.directory( "resources" ).toPath() ) )
        {
            runReportBenchmarks( resources,
                                 scriptName(),
                                 getJar(),
                                 profilers,
                                 processArgs( resources,
                                              profilers,
                                              workloadName,
                                              deployment,
                                              forks ),
                                 assertOnRecordings( deployment, workloadName, forks ),
                                 recordingDirsCount );
        }
    }

    @Disabled
    @Test
    public void executeLoadCsvWorkloadForkedWithEmbedded() throws Exception
    {
        List<ProfilerType> profilers = asList( ProfilerType.JFR, ProfilerType.GC );
        Deployment deployment = Deployment.embedded();
        String workloadName = LOAD_CSV_WORKLOAD;
        int recordingDirsCount = 1;
        int forks = 1;

        try ( Resources resources = new Resources( temporaryFolder.directory( "resources" ).toPath() ) )
        {
            runReportBenchmarks( resources,
                                 scriptName(),
                                 getJar(),
                                 profilers,
                                 processArgs( resources,
                                              profilers,
                                              workloadName,
                                              deployment,
                                              forks ),
                                 assertOnRecordings( deployment, workloadName, forks ),
                                 recordingDirsCount );
        }
    }

    // <><><><><><><><><><><><> Forked - Server <><><><><><><><><><><><>

    @Test
    @Disabled( "https://trello.com/c/l5xaVHck/1558-re-enable-server-tests-in-runworkloadcommandit-in-40" )
    public void executeReadWorkloadForkedWithServer() throws Exception
    {

        List<ProfilerType> profilers = asList( ProfilerType.JFR, ProfilerType.GC );
        Deployment deployment = Deployment.server( getNeo4jDir() );
        String workloadName = READ_WORKLOAD;
        int recordingDirsCount = 1;
        int forks = 1;

        try ( Resources resources = new Resources( temporaryFolder.directory( "resources" ).toPath() ) )
        {
            runReportBenchmarks( resources,
                                 scriptName(),
                                 getJar(),
                                 profilers,
                                 processArgs( resources,
                                              profilers,
                                              workloadName,
                                              deployment,
                                              forks ),
                                 assertOnRecordings( deployment, workloadName, forks ),
                                 recordingDirsCount );
        }
    }

    @Disabled
    @Test
    public void executeWriteWorkloadsForkedWithServer() throws Exception
    {
        List<ProfilerType> profilers = asList( ProfilerType.JFR, ProfilerType.GC );
        Deployment deployment = Deployment.server( getNeo4jDir() );
        String workloadName = WRITE_WORKLOAD;
        int recordingDirsCount = 1;
        int forks = 1;

        try ( Resources resources = new Resources( temporaryFolder.directory( "resources" ).toPath() ) )
        {
            runReportBenchmarks( resources,
                                 scriptName(),
                                 getJar(),
                                 profilers,
                                 processArgs( resources,
                                              profilers,
                                              workloadName,
                                              deployment,
                                              forks ),
                                 assertOnRecordings( deployment, workloadName, forks ),
                                 recordingDirsCount );
        }
    }

    @Disabled
    @Test
    public void executeLoadCsvWorkloadsForkedWithServer() throws Exception
    {
        List<ProfilerType> profilers = asList( ProfilerType.JFR, ProfilerType.GC );
        Deployment deployment = Deployment.server( getNeo4jDir() );
        String workloadName = LOAD_CSV_WORKLOAD;
        int recordingDirsCount = 1;
        int forks = 1;

        try ( Resources resources = new Resources( temporaryFolder.directory( "resources" ).toPath() ) )
        {
            runReportBenchmarks( resources,
                                 scriptName(),
                                 getJar(),
                                 profilers,
                                 processArgs( resources,
                                              profilers,
                                              workloadName,
                                              deployment,
                                              forks ),
                                 assertOnRecordings( deployment, workloadName, forks ),
                                 recordingDirsCount );
        }
    }

    // <><><><><><><><><><><><> In-process - Embedded <><><><><><><><><><><><>

    @Test
    public void executeReadWorkloadInProcessWithEmbedded() throws Exception
    {

        List<ProfilerType> profilers = asList( ProfilerType.JFR, ProfilerType.GC );
        Deployment deployment = Deployment.embedded();
        String workloadName = READ_WORKLOAD;
        int recordingDirsCount = 0;
        int forks = 0;

        try ( Resources resources = new Resources( temporaryFolder.directory( "resources" ).toPath() ) )
        {
            runReportBenchmarks( resources,
                                 scriptName(),
                                 getJar(),
                                 profilers,
                                 processArgs( resources,
                                              profilers,
                                              workloadName,
                                              deployment,
                                              forks ),
                                 assertOnRecordings( deployment, workloadName, forks ),
                                 recordingDirsCount );
        }
    }

    @Disabled
    @Test
    public void executeWriteWorkloadInProcessWithEmbedded() throws Exception
    {
        List<ProfilerType> profilers = asList( ProfilerType.JFR, ProfilerType.GC );
        Deployment deployment = Deployment.embedded();
        String workloadName = WRITE_WORKLOAD;
        int forks = 0;
        int recordingDirsCount = 0;

        try ( Resources resources = new Resources( temporaryFolder.directory( "resources" ).toPath() ) )
        {
            runReportBenchmarks( resources,
                                 scriptName(),
                                 getJar(),
                                 profilers,
                                 processArgs( resources,
                                              profilers,
                                              workloadName,
                                              deployment,
                                              forks ),
                                 assertOnRecordings( deployment, workloadName, forks ),
                                 recordingDirsCount );
        }
    }

    @Disabled
    @Test
    public void executeLoadCsvWorkloadInProcessWithEmbedded() throws Exception
    {

        List<ProfilerType> profilers = asList( ProfilerType.JFR, ProfilerType.GC );
        Deployment deployment = Deployment.embedded();
        String workloadName = LOAD_CSV_WORKLOAD;
        int forks = 0;
        int recordingDirsCount = 0;

        try ( Resources resources = new Resources( temporaryFolder.directory( "resources" ).toPath() ) )
        {
            runReportBenchmarks( resources,
                                 scriptName(),
                                 getJar(),
                                 profilers,
                                 processArgs( resources,
                                              profilers,
                                              workloadName,
                                              deployment,
                                              forks ),
                                 assertOnRecordings( deployment, workloadName, forks ),
                                 recordingDirsCount );
        }
    }

    // <><><><><><><><><><><><> In-process - Server <><><><><><><><><><><><>

    @Test
    @Disabled( "https://trello.com/c/l5xaVHck/1558-re-enable-server-tests-in-runworkloadcommandit-in-40" )
    public void executeReadWorkloadInProcessWithServer() throws Exception
    {

        List<ProfilerType> profilers = asList( ProfilerType.JFR, ProfilerType.GC );
        Deployment deployment = Deployment.server( getNeo4jDir() );
        String workloadName = READ_WORKLOAD;
        int forks = 0;
        int recordingDirsCount = 1;

        try ( Resources resources = new Resources( temporaryFolder.directory( "resources" ).toPath() ) )
        {
            runReportBenchmarks( resources,
                                 scriptName(),
                                 getJar(),
                                 profilers,
                                 processArgs( resources,
                                              profilers,
                                              workloadName,
                                              deployment,
                                              forks ),
                                 assertOnRecordings( deployment, workloadName, forks ),
                                 recordingDirsCount );
        }
    }

    @Disabled
    @Test
    public void executeWriteWorkloadInProcessWithServer() throws Exception
    {

        List<ProfilerType> profilers = asList( ProfilerType.JFR, ProfilerType.GC );
        Deployment deployment = Deployment.server( getNeo4jDir() );
        String workloadName = WRITE_WORKLOAD;
        int forks = 0;
        int recordingDirsCount = 0;

        try ( Resources resources = new Resources( temporaryFolder.directory( "resources" ).toPath() ) )
        {
            runReportBenchmarks( resources,
                                 scriptName(),
                                 getJar(),
                                 profilers,
                                 processArgs( resources,
                                              profilers,
                                              workloadName,
                                              deployment,
                                              forks ),
                                 assertOnRecordings( deployment, workloadName, forks ),
                                 recordingDirsCount );
        }
    }

    @Disabled
    @Test
    public void executeLoadCsvWorkloadInProcessWithServer() throws Exception
    {

        List<ProfilerType> profilers = asList( ProfilerType.JFR, ProfilerType.GC );
        Deployment deployment = Deployment.server( getNeo4jDir() );
        String workloadName = LOAD_CSV_WORKLOAD;
        int forks = 0;
        int recordingDirsCount = 0;

        try ( Resources resources = new Resources( temporaryFolder.directory( "resources" ).toPath() ) )
        {
            runReportBenchmarks( resources,
                                 scriptName(),
                                 getJar(),
                                 profilers,
                                 processArgs( resources,
                                              profilers,
                                              workloadName,
                                              deployment,
                                              forks ),
                                 assertOnRecordings( deployment, workloadName, forks ),
                                 recordingDirsCount );
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

    private static String getNeo4jDir()
    {
        return System.getenv( "NEO4J_DIR" );
    }

    private List<String> processArgs( Resources resources,
                                      List<ProfilerType> profilers,
                                      String workloadName,
                                      Deployment deployment,
                                      int forks )
    {

        Jvm jvm = Jvm.defaultJvmOrFail();
        String awsEndpointUrl = getAWSEndpointURL();
        ResultStoreCredentials resultStoreCredentials = getResultStoreCredentials();
        // prepare neo4j config file
        Path neo4jConfig = workingDir.resolve( "neo4j.config" );
        Neo4jConfigBuilder.withDefaults().writeToFile( neo4jConfig );

        // create empty store
        Path dbPath = temporaryFolder.directory( "db" ).toPath();
        Workload workload = Workload.fromName( workloadName, resources, deployment );
        StoreTestUtil.createEmptyStoreFor( workload,
                                           dbPath, // store
                                           neo4jConfig );

        Path resultsPath = temporaryFolder.createFile( "results.json" ).toPath();

        Neo4jDeployment neo4jDeployment = Neo4jDeployment.from( deployment );

        return asList( "./" + scriptName(),
                       // workload
                       workloadName,
                       // db
                       dbPath.toString(),
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
                       workingDir.toString(),
                       // profilers
                       profilers.stream().map( ProfilerType::name ).collect( joining( "," ) ),
                       // forks
                       Integer.toString( forks ),
                       // results_path
                       resultsPath.toString(),
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
                       "tool_commit",
                       "tool_branch_owner",
                       "tool_branch",
                       // teamcity_build
                       "1",
                       // parent_teamcity_build
                       "0",
                       // execution_mode
                       ExecutionMode.EXECUTE.name(),
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

    protected AssertOnRecordings assertOnRecordings( Deployment deployment, String workloadName, int forks ) throws Exception
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
                for ( ProfilerType profilerType : profilers )
                {
                    for ( Parameters params : parameters )
                    {
                        ProfilerRecordingDescriptor recordingDescriptor = ProfilerRecordingDescriptor.create(
                                query.benchmarkGroup(),
                                query.benchmark(),
                                RunPhase.MEASUREMENT,
                                profilerType,
                                params );
                        for ( RecordingType recordingType : profilerType.allRecordingTypes() )
                        {
                            String profilerArtifactFilename = recordingDescriptor.filename( recordingType );
                            File file = recordingDir.resolve( profilerArtifactFilename ).toFile();
                            assertThat( "File not found: " + file.getAbsolutePath(), file, anExistingFile() );
                        }
                    }
                }
            }
        };
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
