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
import com.neo4j.bench.client.StoreClient;
import com.neo4j.bench.client.model.Neo4jConfig;
import com.neo4j.bench.client.profiling.ProfilerType;
import com.neo4j.bench.client.queries.CreateSchema;
import com.neo4j.bench.client.queries.VerifyStoreSchema;
import com.neo4j.bench.client.util.Jvm;
import com.neo4j.bench.client.util.TestSupport;
import io.findify.s3mock.S3Mock;
import org.junit.Rule;
import org.junit.Test;
import org.junit.jupiter.api.Tag;
import org.junit.rules.TemporaryFolder;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.lang.ProcessBuilder.Redirect;
import java.net.ServerSocket;
import java.nio.file.CopyOption;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Optional;

import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.harness.junit.EnterpriseNeo4jRule;
import org.neo4j.harness.junit.Neo4jRule;
import org.neo4j.kernel.configuration.Settings;

import static org.hamcrest.io.FileMatchers.anExistingFile;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.jupiter.api.Assertions.assertEquals;

import static java.lang.String.format;
import static java.util.Arrays.asList;
import static java.util.stream.Collectors.toList;

public class EndToEndIT
{
    private static final String RUN_REPORT_BENCHMARKS_SH = "run-report-benchmarks.sh";
    @Rule
    public Neo4jRule neo4jBootstrap =
            new EnterpriseNeo4jRule().withConfig( GraphDatabaseSettings.auth_enabled, Settings.FALSE );
    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    @Test
    @Tag( "endtoend" )
    public void runReportBenchmarks() throws Exception
    {

        System.out.println("Working Directory = " +
                System.getProperty("user.dir"));

        // fail fast, check if we have proper artifacts in place
        Path baseDir = Paths.get( System.getProperty( "user.dir" ) ).toAbsolutePath();
        Path runReportScript = baseDir.resolve( RUN_REPORT_BENCHMARKS_SH );

        // we can be running in forked process (if run from Maven) look for base dir
        while ( baseDir != null && !Files.isRegularFile( runReportScript ) )
        {
            baseDir = baseDir.getParent();
            runReportScript = baseDir.resolve( RUN_REPORT_BENCHMARKS_SH );
        }

        assertNotNull( format( "%s is not valid base dir", baseDir ), baseDir );
        assertTrue(format( "%s not found, your are running tests from invalid location", runReportScript.getFileName() ),
                    Files.exists( runReportScript ) );

        Path microJar = baseDir.resolve( "target/micro-benchmarks.jar" );
        assertTrue( "micro-benchmarks.jar not found, make sure you have assembly in place, by running mvn package",
                    Files.exists( microJar ) );

        // copy files into temporary location
        Path workPath = temporaryFolder.newFolder( ).toPath();
        Files.copy( runReportScript, workPath.resolve( RUN_REPORT_BENCHMARKS_SH ) );
        Files.createDirectories( workPath.resolve( "micro/target" ) );
        Files.copy( microJar, workPath.resolve( "micro/target/micro-benchmarks.jar" ) );

        // assert if environment is setup
        List<ProfilerType> profilers = asList( ProfilerType.JFR, ProfilerType.ASYNC, ProfilerType.GC );
//        for ( ProfilerType profiler : profilers )
//        {
//            profiler.assertEnvironmentVariablesPresent( true );
//        }

        assertSysctlParameter( 1, "kernel.perf_event_paranoid" );
        assertSysctlParameter( 0, "kernel.kptr_restrict");

        // setup results store schema
        try ( StoreClient storeClient = StoreClient.connect( neo4jBootstrap.boltURI(), "", "" ) )
        {
            storeClient.execute( new CreateSchema() );
        }

        // start s3 storage mock
        Path s3Path = temporaryFolder.newFolder( "s3" ).toPath();
        int s3Port = randomLocalPort();
        S3Mock api = new S3Mock.Builder().withPort( s3Port ).withFileBackend( s3Path.toString() ).build();
        api.start();

        // make sure we have a s3 bucket created
        String endpointUrl = String.format( "http://localhost:%d", s3Port );
        EndpointConfiguration endpoint = new EndpointConfiguration( endpointUrl, "us-west-2" );
        AmazonS3 client = AmazonS3ClientBuilder.standard().withPathStyleAccessEnabled( true )
                .withEndpointConfiguration( endpoint )
                .withCredentials( new AWSStaticCredentialsProvider( new AnonymousAWSCredentials() ) ).build();
        client.createBucket( "benchmarking.neo4j.com" );

        // prepare neo4j config file
        Path neo4jConfig = temporaryFolder.newFile( "neo4j.config" ).toPath();
        Neo4jConfig.withDefaults().writeAsProperties( neo4jConfig );

        File benchmarkConfig = temporaryFolder.newFile( "benchmarkConfig" );
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
                neo4jBootstrap.boltURI().toString(),
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
                // tarball
                "tarball",
                // jvm_args
                "",
                // jmh_args
                "",
                // neo4j_config_path
                "neo4j_config_path",
                // jvm_path
                Jvm.defaultJvmOrFail().launchJava(),
                // with_jfr
                "true",
                // with_async
                "true",
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

    private void assertStoreSchema()
    {
        try ( StoreClient storeClient = StoreClient.connect( neo4jBootstrap.boltURI(), "", "" ) )
        {
            storeClient.execute( new VerifyStoreSchema() );
        }
    }

    private static void assertSysctlParameter( int expectecKernelParemeterValue, String kernelParameter ) throws IOException
    {
        ProcessBuilder processBuilder = new ProcessBuilder( "sysctl",kernelParameter ) ;
        try ( BufferedReader reader =
                new BufferedReader( new InputStreamReader( processBuilder.start().getInputStream() ) ) )
        {
            Integer kernelParameterValue = reader.lines()
            .findFirst()
            .map( s -> s.split( " = " ) )
            .flatMap( s -> s.length == 2 ? Optional.of( s[1] ) : Optional.empty() )
            .map( Integer::parseInt )
            .orElseThrow( () -> new RuntimeException( "sysctl output is not parsable" ));
            assertEquals( expectecKernelParemeterValue, kernelParameterValue.intValue(),
                          format( "incorrect value of kernel parameter %s = %d", kernelParameter, kernelParameterValue ) );
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
        assertThat( recordingsBasePath.resolve( recordingDir.getFileName() + ".tar.gz" ).toFile(),anExistingFile() );

//        Workload workload = Workload.fromName( "zero", new Resources() );
//        for ( Query query : workload.queries() )
//        {
//            for ( ProfilerType profilerType : profilers )
//            {
//                ProfilerRecordingDescriptor recordingDescriptor = new ProfilerRecordingDescriptor(
//                        query.benchmarkGroup(), query.benchmark(), RunPhase.MEASUREMENT, profilerType );
//                for ( RecordingType recordingType : profilerType.allRecordingTypes() )
//                {
//                    String profilerArtifactFilename = recordingDescriptor.filename( recordingType );
//                    File file = recordingDir.resolve( profilerArtifactFilename ).toFile();
//                    assertThat( file, anExistingFile() );
//                }
//            }
//        }
    }

    private static int randomLocalPort() throws IOException
    {
        try ( ServerSocket socket = new ServerSocket( 0 ); )
        {
            return socket.getLocalPort();
        }
    }
}
