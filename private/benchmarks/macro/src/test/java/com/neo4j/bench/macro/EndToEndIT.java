/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.macro;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
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
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
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

import org.neo4j.configuration.connectors.BoltConnector;
import org.neo4j.configuration.connectors.HttpConnector;
import org.neo4j.test.ports.PortAuthority;

import static java.lang.String.format;
import static java.util.Arrays.asList;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.stream.Collectors.joining;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.io.FileMatchers.anExistingFile;
import static org.junit.jupiter.api.Assertions.assertTrue;

class EndToEndIT extends BaseEndToEndIT
{
    private static final int BOLT_PORT = PortAuthority.allocatePort();
    private static final int HTTP_PORT = PortAuthority.allocatePort();
    private static final String LOAD_CSV_WORKLOAD = "cineasts_csv";
    private static final String WRITE_WORKLOAD = "pokec_write";
    private static final String READ_WORKLOAD = "zero";
    private static final String SCRIPT_NAME = "run-report-benchmarks.sh";
    private static final Path JAR = Paths.get( "target/macro.jar" );

    private Path neo4jDir;

    @BeforeEach
    public void copyNeo4j() throws Exception
    {
        Path originalDir = Paths.get( System.getenv( "NEO4J_DIR" ) );
        this.neo4jDir = temporaryFolder.resolve( format( "neo4jDir-%s", UUID.randomUUID().toString() ) );
        FileUtils.copyDirectory( originalDir.toFile(), neo4jDir.toFile() );
    }

    @AfterEach
    public void deleteTemporaryNeo4j() throws IOException
    {
        FileUtils.deleteDirectory( neo4jDir.toFile() );
    }

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
        Deployment deployment = Deployment.embedded();
        String workloadName = READ_WORKLOAD;
        ExecutionMode executionMode = ExecutionMode.CARDINALITY;
        int recordingDirsCount = 1;
        int forks = 1;

        test( deployment, workloadName, executionMode, recordingDirsCount, forks );
    }

    @Disabled( "long running test" )
    @Test
    public void runWriteWorkloadForkedWithEmbedded() throws Exception
    {
        Deployment deployment = Deployment.embedded();
        String workloadName = WRITE_WORKLOAD;
        ExecutionMode executionMode = ExecutionMode.EXECUTE;
        int recordingDirsCount = 1;
        int forks = 1;

        test( deployment, workloadName, executionMode, recordingDirsCount, forks );
    }

    @Disabled( "timed out after 20 minutes" )
    @Test
    public void executeLoadCsvWorkloadForkedWithEmbedded() throws Exception
    {
        Deployment deployment = Deployment.embedded();
        String workloadName = LOAD_CSV_WORKLOAD;
        ExecutionMode executionMode = ExecutionMode.EXECUTE;
        int recordingDirsCount = 1;
        int forks = 1;

        test( deployment, workloadName, executionMode, recordingDirsCount, forks );
    }

    // <><><><><><><><><><><><> Forked - Server <><><><><><><><><><><><>

    @Test
    public void executeReadWorkloadForkedWithServer() throws Exception
    {
        Deployment deployment = serverDeployment();
        String workloadName = READ_WORKLOAD;
        ExecutionMode executionMode = ExecutionMode.CARDINALITY;
        int recordingDirsCount = 1;
        int forks = 1;

        test( deployment, workloadName, executionMode, recordingDirsCount, forks );
    }

    @Disabled( "long running test" )
    @Test
    public void executeWriteWorkloadsForkedWithServer() throws Exception
    {
        Deployment deployment = serverDeployment();
        String workloadName = WRITE_WORKLOAD;
        ExecutionMode executionMode = ExecutionMode.EXECUTE;
        int recordingDirsCount = 1;
        int forks = 1;

        test( deployment, workloadName, executionMode, recordingDirsCount, forks );
    }

    @Disabled( "Executing queries that use periodic commit in an open transaction is not possible" )
    @Test
    public void executeLoadCsvWorkloadsForkedWithServer() throws Exception
    {
        Deployment deployment = serverDeployment();
        String workloadName = LOAD_CSV_WORKLOAD;
        ExecutionMode executionMode = ExecutionMode.EXECUTE;
        int recordingDirsCount = 1;
        int forks = 1;

        test( deployment, workloadName, executionMode, recordingDirsCount, forks );
    }

    // <><><><><><><><><><><><> In-process - Embedded <><><><><><><><><><><><>

    @Test
    public void executeReadWorkloadInProcessWithEmbedded() throws Exception
    {
        Deployment deployment = Deployment.embedded();
        String workloadName = READ_WORKLOAD;
        ExecutionMode executionMode = ExecutionMode.EXECUTE;
        int recordingDirsCount = 0;
        int forks = 0;

        test( deployment, workloadName, executionMode, recordingDirsCount, forks );
    }

    @Disabled( "long running test" )
    @Test
    public void executeWriteWorkloadInProcessWithEmbedded() throws Exception
    {
        Deployment deployment = Deployment.embedded();
        String workloadName = WRITE_WORKLOAD;
        ExecutionMode executionMode = ExecutionMode.EXECUTE;
        int recordingDirsCount = 0;
        int forks = 0;

        test( deployment, workloadName, executionMode, recordingDirsCount, forks );
    }

    @Disabled( "timed out after 20 minutes" )
    @Test
    public void executeLoadCsvWorkloadInProcessWithEmbedded() throws Exception
    {
        Deployment deployment = Deployment.embedded();
        String workloadName = LOAD_CSV_WORKLOAD;
        ExecutionMode executionMode = ExecutionMode.EXECUTE;
        int recordingDirsCount = 0;
        int forks = 0;

        test( deployment, workloadName, executionMode, recordingDirsCount, forks );
    }

    // <><><><><><><><><><><><> In-process - Server <><><><><><><><><><><><>
    @Test
    public void executeReadWorkloadInProcessWithServer() throws Exception
    {
        Deployment deployment = serverDeployment();
        String workloadName = READ_WORKLOAD;
        ExecutionMode executionMode = ExecutionMode.CARDINALITY;
        int recordingDirsCount = 1;
        int forks = 0;

        test( deployment, workloadName, executionMode, recordingDirsCount, forks );
    }

    @Disabled( "long running test" )
    @Test
    public void executeWriteWorkloadInProcessWithServer() throws Exception
    {
        Deployment deployment = serverDeployment();
        String workloadName = WRITE_WORKLOAD;
        ExecutionMode executionMode = ExecutionMode.EXECUTE;
        int recordingDirsCount = 0;
        int forks = 0;

        test( deployment, workloadName, executionMode, recordingDirsCount, forks );
    }

    @Disabled( "Executing queries that use periodic commit in an open transaction is not possible" )
    @Test
    public void executeLoadCsvWorkloadInProcessWithServer() throws Exception
    {
        Deployment deployment = serverDeployment();
        String workloadName = LOAD_CSV_WORKLOAD;
        ExecutionMode executionMode = ExecutionMode.EXECUTE;
        int recordingDirsCount = 0;
        int forks = 0;

        test( deployment, workloadName, executionMode, recordingDirsCount, forks );
    }

    private Deployment serverDeployment()
    {
        return Deployment.server( neo4jDir.toString() );
    }

    // <><><><><><><><><><><><> VmStat profiler <><><><><><><><><><><><>

    @Test
    public void executeWithVmStatProfiler() throws Exception
    {
        Deployment deployment = Deployment.embedded();
        String workloadName = READ_WORKLOAD;
        ExecutionMode executionMode = ExecutionMode.EXECUTE;
        int recordingDirsCount = 1;
        int forks = 1;

        test( deployment, workloadName, executionMode, recordingDirsCount, forks, Collections.singletonList( ProfilerType.VM_STAT ) );
    }

    private void test( Deployment deployment, String workloadName, ExecutionMode executionMode, int recordingDirsCount, int forks ) throws Exception
    {
        test( deployment, workloadName, executionMode, recordingDirsCount, forks, asList( ProfilerType.JFR, ProfilerType.GC ) );
    }

    private void test( Deployment deployment,
                       String workloadName,
                       ExecutionMode executionMode,
                       int recordingDirsCount,
                       int forks,
                       List<ProfilerType> profilers ) throws Exception
    {
        ExpectedRecordings expectedRecordings = expectedRecordingsFor( profilers, executionMode, deployment, forks );
        Path resourcesPath = temporaryFolder.resolve( "resources" );
        Files.createDirectories( resourcesPath );
        try ( Resources resources = new Resources( resourcesPath ) )
        {
            runReportBenchmarks( SCRIPT_NAME,
                                 JAR,
                                 profilers,
                                 processArgs( resources, profilers, workloadName, deployment, forks, executionMode ),
                                 assertOnRecordings( resources, deployment, workloadName, forks, executionMode ),
                                 recordingDirsCount,
                                 expectedRecordings );
        }
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
        Neo4jConfigBuilder.withDefaults()
                          .withSetting( BoltConnector.listen_address, ":" + BOLT_PORT )
                          .withSetting( HttpConnector.listen_address, ":" + HTTP_PORT )
                          .writeToFile( neo4jConfig );

        // create empty store
        Path dbPath = temporaryFolder.resolve( "db" );
        Files.createDirectories( dbPath );
        Workload workload = Workload.fromName( workloadName, resources, deployment );
        Store emptyStoreFor = StoreTestUtil.createEmptyStoreFor( workload,
                                                                 dbPath, // store
                                                                 neo4jConfig );

        Neo4jDeployment neo4jDeployment = Neo4jDeployment.from( deployment );

        return asList( "./" + SCRIPT_NAME,
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
                       "--aws-endpoint-url",
                       awsEndpointUrl );
    }

    protected AssertOnRecordings assertOnRecordings( Resources resources, Deployment deployment, String workloadName, int forks, ExecutionMode executionMode )
    {
        return ( Path recordingDir, List<ProfilerType> profilers ) ->
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
                        if ( profilerType == ProfilerType.VM_STAT )
                        {
                            assertVmStatRecordings( profilerRecordingDescriptor, recordingDir );
                        }
                    }
                }
                if ( executionMode.equals( ExecutionMode.CARDINALITY ) )
                {
                    assertCardinalityRecordings( deployment, recordingDir, query );
                }
            }
        };
    }

    private void assertVmStatRecordings( ProfilerRecordingDescriptor profilerRecordingDescriptor, Path recordingDir ) throws IOException
    {
        RecordingDescriptor recordingDescriptor = profilerRecordingDescriptor.recordingDescriptorFor( RecordingType.TRACE_VMSTAT_CHART );
        String profilerArtifactFilename = recordingDescriptor.filename();
        File file = recordingDir.resolve( profilerArtifactFilename ).toFile();
        assertThat( "File not found: " + file.getAbsolutePath(), file, anExistingFile() );
        JsonNode chart = new ObjectMapper().readTree( file );
        assertTrue( chart.isArray(), "Chart file should be a JSON array" );
    }

    private void assertCardinalityRecordings( Deployment deployment, Path recordingDir, Query query )
    {
        Parameters clientParameters = clientParameters( deployment.deploymentModes() ); // ASCII plans are only created on client side
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
