/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.macro;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.AnonymousAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.neo4j.bench.client.StoreClient;
import com.neo4j.bench.client.queries.CreateSchema;
import com.neo4j.bench.client.queries.VerifyStoreSchema;
import com.neo4j.bench.common.Neo4jConfigBuilder;
import com.neo4j.bench.common.model.Parameters;
import com.neo4j.bench.common.options.Edition;
import com.neo4j.bench.common.options.Planner;
import com.neo4j.bench.common.options.Runtime;
import com.neo4j.bench.common.profiling.ProfilerRecordingDescriptor;
import com.neo4j.bench.common.profiling.ProfilerType;
import com.neo4j.bench.common.profiling.RecordingType;
import com.neo4j.bench.common.results.RunPhase;
import com.neo4j.bench.common.tool.macro.ExecutionMode;
import com.neo4j.bench.common.util.ErrorReporter.ErrorPolicy;
import com.neo4j.bench.common.util.Jvm;
import com.neo4j.bench.common.util.Resources;
import com.neo4j.bench.macro.execution.Neo4jDeployment;
import com.neo4j.bench.macro.workload.Query;
import com.neo4j.bench.macro.workload.Workload;
import com.neo4j.harness.junit.extension.CommercialNeo4jExtension;

import com.neo4j.common.util.TestSupport;
import io.findify.s3mock.S3Mock;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.lang.ProcessBuilder.Redirect;
import java.net.ServerSocket;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.configuration.connectors.ConnectorPortRegister;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.harness.junit.extension.Neo4jExtension;
import org.neo4j.internal.helpers.HostnamePort;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

import static com.neo4j.bench.common.util.TestDirectorySupport.createTempDirectoryPath;
import static com.neo4j.bench.macro.execution.Neo4jDeployment.DeploymentMode;
import static java.lang.String.format;
import static java.util.Arrays.asList;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.core.IsCollectionContaining.hasItem;
import static org.hamcrest.io.FileMatchers.anExistingFile;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.configuration.SettingValueParsers.FALSE;

@ExtendWith( TestDirectoryExtension.class )
class EndToEndIT
{
    private static final String RUN_REPORT_BENCHMARKS_SH = "run-report-benchmarks.sh";

    @RegisterExtension
    static Neo4jExtension neo4jExtension =
            CommercialNeo4jExtension.builder().withConfig( GraphDatabaseSettings.auth_enabled, FALSE ).build();

    @Inject
    private TestDirectory temporaryFolder;

    private URI boltUri;

    @BeforeEach
    void setUp( GraphDatabaseService databaseService )
    {
        HostnamePort address = ((GraphDatabaseAPI) databaseService).getDependencyResolver()
                                                                   .resolveDependency( ConnectorPortRegister.class ).getLocalAddress( "bolt" );
        boltUri = URI.create( "bolt://" + address.toString() );
    }

    @AfterEach
    void cleanUpDb( GraphDatabaseService databaseService )
    {
        // this is hacky HACK, needs to be fixed in Neo4jExtension
        databaseService.execute( "MATCH (n) DETACH DELETE n" ).close();
    }

    @Test
    @Tag( "endtoend" )
    @Timeout( value = 15, unit = TimeUnit.MINUTES )
    void runReportBenchmarks() throws Exception
    {
        // fail fast, check if we have proper artifacts in place
        Path baseDir = Paths.get( System.getProperty( "user.dir" ) ).toAbsolutePath();
        Path runReportScript = baseDir.resolve( RUN_REPORT_BENCHMARKS_SH );

        // we can be running in forked process (if run from Maven) look for base dir
        while ( !Files.isRegularFile( runReportScript ) )
        {
            baseDir = baseDir.getParent();
            runReportScript = baseDir.resolve( RUN_REPORT_BENCHMARKS_SH );
        }

        assertNotNull( baseDir, format( "%s is not valid base dir", baseDir ) );
        assertTrue( Files.exists( runReportScript ),
                    format( "%s not found, your are running tests from invalid location", runReportScript.getFileName() ) );

        Path macroJar = baseDir.resolve( "target/macro.jar" );
        assertTrue( Files.exists( macroJar ),
                    "macro.jar not found, make sure you have assembly in place, by running mvn package" );

        // assert if environment is setup
        List<ProfilerType> profilers = asList( ProfilerType.JFR, ProfilerType.ASYNC, ProfilerType.GC );

        assertSysctlParameter( asList( 1, -1 ), "kernel.perf_event_paranoid" );
        assertSysctlParameter( asList( 0 ), "kernel.kptr_restrict" );

        // setup results store schema
        try ( StoreClient storeClient = StoreClient.connect( boltUri, "", "" ) )
        {
            storeClient.execute( new CreateSchema() );
        }

        // start s3 storage mock
        Path absoluteTempPath = temporaryFolder.absolutePath().toPath();
        Path s3Path = absoluteTempPath.resolve( "s3" );
        int s3Port = randomLocalPort();
        S3Mock api = new S3Mock.Builder()
                .withPort( s3Port )
                .withFileBackend( s3Path.toString() )
                .build();
        AmazonS3 client = null;
        try
        {
            api.start();
            // make sure we have a s3 bucket created
            String endpointUrl = String.format( "http://localhost:%d", s3Port );
            EndpointConfiguration endpoint = new EndpointConfiguration( endpointUrl, "us-west-2" );
            client = AmazonS3ClientBuilder.standard()
                                                   .withPathStyleAccessEnabled( true )
                                                   .withEndpointConfiguration( endpoint )
                                                   .withCredentials( new AWSStaticCredentialsProvider( new AnonymousAWSCredentials() ) )
                                                   .build();
            client.createBucket( "benchmarking.neo4j.com" );

            // prepare neo4j config file
            Path neo4jConfig = absoluteTempPath.resolve( "neo4j.config" );
            Neo4jConfigBuilder.withDefaults().writeToFile( neo4jConfig );

            // create empty store
            Path dbPath = absoluteTempPath.resolve( "db" );
            TestSupport.createEmptyStore( dbPath, neo4jConfig );

            Path resultsPath = absoluteTempPath.resolve( "results.json" );

            Path workPath = temporaryFolder.directory( "work" ).toPath();

            Neo4jDeployment deployment = Neo4jDeployment.embedded();

            ProcessBuilder processBuilder = new ProcessBuilder( asList( "./run-report-benchmarks.sh",
                                                                        // workload
                                                                        "zero",
                                                                        // db
                                                                        dbPath.toString(),
                                                                        // warmup_count
                                                                        "1",
                                                                        // measurement_count
                                                                        "1",
                                                                        // db_edition
                                                                        Edition.ENTERPRISE.name(),
                                                                        // jvm
                                                                        Jvm.defaultJvmOrFail().launchJava(),
                                                                        // neo4j_config
                                                                        neo4jConfig.toString(),
                                                                        // work_dir
                                                                        workPath.toString(),
                                                                        // profilers
                                                                        profilers.stream().map( ProfilerType::name ).collect( joining( "," ) ),
                                                                        // forks
                                                                        "1",
                                                                        // results_path
                                                                        resultsPath.toString(),
                                                                        // time_unit
                                                                        MILLISECONDS.name(),
                                                                        // results_store_uri
                                                                        boltUri.toString(),
                                                                        "results_store_user",
                                                                        "results_store_password",
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
                                                                        deployment.toString(),
                                                                        // AWS endpoint URL
                                                                        endpointUrl ) )
                    .directory( baseDir.toFile() )
                    .redirectOutput( Redirect.PIPE )
                    .redirectErrorStream( true );

            Process process = processBuilder.start();
            BufferedReader reader = new BufferedReader( new InputStreamReader( process.getInputStream() ) );
            String line;
            while ( (line = reader.readLine()) != null )
            {
                System.out.println( line );
            }
            assertEquals( 0, process.waitFor(), "run-report-benchmarks.sh finished with non-zero code" );
            assertStoreSchema();
            assertRecordingFilesExist( s3Path, profilers, deployment.mode() );
        }
        finally
        {
            api.shutdown();
            if ( client != null )
            {
                client.shutdown();
            }
        }
    }

    private void assertStoreSchema()
    {
        try ( StoreClient storeClient = StoreClient.connect( boltUri, "", "" ) )
        {
            storeClient.execute( new VerifyStoreSchema() );
        }
    }

    private static void assertSysctlParameter( List<Integer> allowedValues, String kernelParameter ) throws IOException
    {
        ProcessBuilder processBuilder = new ProcessBuilder( "sysctl", kernelParameter );
        try ( BufferedReader reader =
                      new BufferedReader( new InputStreamReader( processBuilder.start().getInputStream() ) ) )
        {
            Integer kernelParameterValue = reader.lines()
                                                 .findFirst()
                                                 .map( s -> s.split( " = " ) )
                                                 .flatMap( s -> s.length == 2 ? Optional.of( s[1] ) : Optional.empty() )
                                                 .map( Integer::parseInt )
                                                 .orElseThrow( () -> new RuntimeException( "sysctl output is not parsable" ) );
            assertThat( format( "incorrect value of kernel parameter %s = %d", kernelParameter, kernelParameterValue ),
                        allowedValues,
                        hasItem( kernelParameterValue.intValue() ) );
        }
    }

    private void assertRecordingFilesExist( Path s3Path,
                                            List<ProfilerType> profilers,
                                            DeploymentMode deployment ) throws IOException
    {
        Path recordingsBasePath = s3Path.resolve( "benchmarking.neo4j.com/recordings" );
        List<Path> recordingDirs = Files
                .list( recordingsBasePath )
                .map( Path::toFile )
                .filter( File::isDirectory )
                .map( File::toPath )
                .collect( toList() );

        assertEquals( 1, recordingDirs.size() );
        Path recordingDir = recordingDirs.get( 0 );
        assertThat( recordingsBasePath.resolve( recordingDir.getFileName() + ".tar.gz" ).toFile(), anExistingFile() );

        try ( Resources resources = new Resources( createTempDirectoryPath( temporaryFolder.absolutePath() ) ) )
        {
            Workload workload = Workload.fromName( "zero", resources, deployment );
            // should find at least one recording per profiler per benchmark -- there may be more, due to secondary recordings
            int profilerRecordingCount = (int) Files.list( recordingDir ).count();
            int minimumExpectedProfilerRecordingCount = profilers.size() * workload.queries().size();
            assertThat( profilerRecordingCount, greaterThanOrEqualTo( minimumExpectedProfilerRecordingCount ) );

            for ( Query query : workload.queries() )
            {
                for ( ProfilerType profilerType : profilers )
                {
                    ProfilerRecordingDescriptor recordingDescriptor = ProfilerRecordingDescriptor.create(
                            query.benchmarkGroup(),
                            query.benchmark(),
                            RunPhase.MEASUREMENT,
                            profilerType,
                            Parameters.NONE );
                    for ( RecordingType recordingType : profilerType.allRecordingTypes() )
                    {
                        String profilerArtifactFilename = recordingDescriptor.filename( recordingType );
                        File file = recordingDir.resolve( profilerArtifactFilename ).toFile();
                        assertThat( "File not found: " + file.getAbsolutePath(), file, anExistingFile() );
                    }
                }
            }
        }
    }

    private static int randomLocalPort() throws IOException
    {
        try ( ServerSocket socket = new ServerSocket( 0 ) )
        {
            return socket.getLocalPort();
        }
    }
}
