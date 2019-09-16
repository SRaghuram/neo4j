/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.micro;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.AnonymousAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.google.common.collect.ImmutableSet;
import com.neo4j.bench.client.StoreClient;
import com.neo4j.bench.client.queries.CreateSchema;
import com.neo4j.bench.client.queries.VerifyStoreSchema;
import com.neo4j.bench.common.Neo4jConfigBuilder;
import com.neo4j.bench.common.profiling.ProfilerType;
import com.neo4j.bench.common.util.Jvm;
import com.neo4j.bench.jmh.api.config.BenchmarkConfigFile;
import com.neo4j.bench.jmh.api.config.SuiteDescription;
import com.neo4j.bench.jmh.api.config.Validation;
import com.neo4j.bench.micro.benchmarks.test.NoOpBenchmark;
import com.neo4j.harness.junit.extension.EnterpriseNeo4jExtension;
import io.findify.s3mock.S3Mock;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
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
import java.util.stream.Collectors;

import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.configuration.connectors.BoltConnector;
import org.neo4j.configuration.connectors.ConnectorPortRegister;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.harness.junit.extension.Neo4jExtension;
import org.neo4j.internal.helpers.HostnamePort;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

import static java.lang.String.format;
import static java.util.Arrays.asList;
import static java.util.stream.Collectors.toList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.io.FileMatchers.anExistingFile;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@TestDirectoryExtension
class EndToEndIT extends AnnotationsFixture
{
    private static final String RUN_REPORT_BENCHMARKS_SH = "run-report-benchmarks.sh";

    @RegisterExtension
    static Neo4jExtension neo4jExtension =
            EnterpriseNeo4jExtension.builder()
                                    .withConfig( GraphDatabaseSettings.auth_enabled, false )
                                    .withConfig( BoltConnector.enabled, true )
                                    .withConfig( BoltConnector.encryption_level, BoltConnector.EncryptionLevel.OPTIONAL )
                                    .build();

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
        try ( Transaction transaction = databaseService.beginTx() )
        {
            // this is hacky HACK, needs to be fixed in Neo4jExtension
            transaction.execute( "MATCH (n) DETACH DELETE n" ).close();
            transaction.commit();
        }
    }

    @Test
    @Tag( "endtoend" )
    @Timeout( value = 15, unit = TimeUnit.MINUTES )
    void runReportBenchmarks() throws Exception
    {
        System.out.println( "Working Directory = " + System.getProperty( "user.dir" ) );

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
        assertTrue(
                Files.exists( runReportScript ),
                format( "%s not found, your are running tests from invalid location", runReportScript.getFileName() ) );

        Path microJar = baseDir.resolve( "target/micro-benchmarks.jar" );
        assertTrue(
                Files.exists( microJar ),
                "micro-benchmarks.jar not found, make sure you have assembly in place, by running mvn package" );

        // copy files into temporary location
        Path workPath = temporaryFolder.absolutePath().toPath();
        Files.copy( runReportScript, workPath.resolve( RUN_REPORT_BENCHMARKS_SH ) );
        Files.createDirectories( workPath.resolve( "micro/target" ) );
        Files.copy( microJar, workPath.resolve( "micro/target/micro-benchmarks.jar" ) );

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
        Path s3Path = temporaryFolder.directory( "s3" ).toPath();
        int s3Port = randomLocalPort();
        S3Mock api = new S3Mock.Builder().withPort( s3Port ).withFileBackend( s3Path.toString() ).build();
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
            Path neo4jConfig = temporaryFolder.file( "neo4j.config" ).toPath();
            Neo4jConfigBuilder.withDefaults().writeToFile( neo4jConfig );

            File benchmarkConfig = createBenchmarkConfig();

            ProcessBuilder processBuilder = new ProcessBuilder( asList( "./run-report-benchmarks.sh",
                                                                        // neo4j_version
                                                                        "3.3.0",
                                                                        // neo4j_commit
                                                                        "neo4j_commit",
                                                                        // neo4j_branch
                                                                        "neo4j_branch",
                                                                        // neo4j_branch_owner
                                                                        "neo4j_branch_owner",
                                                                        // tool_branch
                                                                        "tool_branch",
                                                                        // tool_branch_owner
                                                                        "tool_branch_owner",
                                                                        // tool_commit
                                                                        "tool_commit",
                                                                        // results_store_uri
                                                                        boltUri.toString(),
                                                                        // results_store_user
                                                                        "neo4j",
                                                                        // results_store_password
                                                                        "neo4j",
                                                                        // benchmark_config
                                                                        benchmarkConfig.toString(),
                                                                        // teamcity_build_id
                                                                        "0",
                                                                        // parent_teamcity_build_id
                                                                        "1",
                                                                        // jvm_args
                                                                        "",
                                                                        // jmh_args
                                                                        "",
                                                                        // neo4j_config_path
                                                                        neo4jConfig.toString(),
                                                                        // jvm_path
                                                                        Jvm.defaultJvmOrFail().launchJava(),
                                                                        // profilers
                                                                        ProfilerType.serializeProfilers( profilers ),
                                                                        // triggered_by
                                                                        "triggered_by",
                                                                        endpointUrl ) )
                    .directory( workPath.toFile() )
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
            assertRecordingFilesExist( s3Path, profilers );
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

    private File createBenchmarkConfig() throws IOException
    {
        File benchmarkConfig = temporaryFolder.file( "benchmarkConfig" );

        Validation validation = new Validation();
        SuiteDescription suiteDescription = SuiteDescription.fromAnnotations( getAnnotations(), validation );
        Validation.assertValid( validation );
        BenchmarkConfigFile.write(
                suiteDescription,
                ImmutableSet.of( NoOpBenchmark.class.getName() ),
                false,
                false,
                benchmarkConfig.toPath() );
        return benchmarkConfig;
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

    private static void assertRecordingFilesExist( Path s3Path, List<ProfilerType> profilers ) throws IOException
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

        // all recordings
        List<Path> recordings = Files.list( recordingDir ).collect( Collectors.toList() );

        // all expected recordings
        long expectedRecordingsCount = profilers.stream()
                                                .mapToLong( profiler -> profiler.allRecordingTypes().size() )
                                                .sum();

        long existingRecordingsCount = profilers.stream()
                                                .flatMap( profiler -> profiler.allRecordingTypes().stream() )
                                                .filter( recording ->
                                                                 recordings.stream()
                                                                           .map( file -> file.getFileName().toString() )
                                                                           .anyMatch( filename -> filename.endsWith( recording.extension() ) ) )
                                                .count();

        assertEquals( expectedRecordingsCount, existingRecordingsCount, "number of existing recordings differs from expected number of recordings" );
    }

    private static int randomLocalPort() throws IOException
    {
        try ( ServerSocket socket = new ServerSocket( 0 ) )
        {
            return socket.getLocalPort();
        }
    }
}
