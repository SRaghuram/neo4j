/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.test;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.AnonymousAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.neo4j.bench.client.StoreClient;
import com.neo4j.bench.client.queries.schema.CreateSchema;
import com.neo4j.bench.client.queries.schema.VerifyStoreSchema;
import com.neo4j.bench.common.profiling.ProfilerType;
import com.neo4j.bench.common.util.Jvm;
import com.neo4j.bench.common.util.Resources;
import com.neo4j.harness.junit.extension.EnterpriseNeo4jExtension;
import io.findify.s3mock.S3Mock;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.ServerSocket;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.configuration.connectors.BoltConnector;
import org.neo4j.configuration.connectors.ConnectorPortRegister;
import org.neo4j.graphdb.GraphDatabaseService;
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
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.io.FileMatchers.anExistingFile;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@TestDirectoryExtension
public abstract class BaseEndToEndIT
{
    @RegisterExtension
    static Neo4jExtension neo4jExtension =
            EnterpriseNeo4jExtension.builder()
                                    .withConfig( GraphDatabaseSettings.auth_enabled, false )
                                    .withConfig( BoltConnector.enabled, true )
                                    .withConfig( BoltConnector.encryption_level, BoltConnector.EncryptionLevel.OPTIONAL )
                                    .build();

    @Inject
    protected TestDirectory temporaryFolder;

    private URI boltUri;

    @BeforeEach
    void setUp( GraphDatabaseService databaseService )
    {
        HostnamePort address = ((GraphDatabaseAPI) databaseService).getDependencyResolver()
                                                                   .resolveDependency( ConnectorPortRegister.class ).getLocalAddress( "bolt" );
        boltUri = URI.create( "bolt://" + address.toString() );
    }

    protected static class ResultStoreCredentials
    {
        private final String boltUri;
        private final String user;
        private final String pass;

        private ResultStoreCredentials( URI boltUri, String user, String pass )
        {
            this.boltUri = boltUri.toString();
            this.user = user;
            this.pass = pass;
        }

        public String boltUri()
        {
            return boltUri;
        }

        public String user()
        {
            return user;
        }

        public String pass()
        {
            return pass;
        }
    }

    /**
     * Called before launching benchmark process.
     *
     * @return filename of main, entry-point script
     */
    protected abstract String scriptName();

    /**
     * Called before launching benchmark process.
     *
     * @param baseDir the folder containing main, entry-point script
     * @return path to runnable jar
     */
    protected abstract Path getJar( Path baseDir );

    /**
     * Called before launching benchmark process.
     *
     * @param resources utility for retrieving files located in resources/ folder
     * @param profilers list of profilers to run with
     * @param endpointUrl S3 end-point (for test purposes only)
     * @param baseDir the folder containing main, entry-point script
     * @param jvm JVM to use for executing the benchmark tool
     * @param resultStoreCredentials URI, username, and password for connecting to benchmark results store
     * @return process arguments, for launching main, entry-point script
     */
    protected abstract List<String> processArgs( Resources resources,
                                                 List<ProfilerType> profilers,
                                                 String endpointUrl,
                                                 Path baseDir,
                                                 Jvm jvm,
                                                 ResultStoreCredentials resultStoreCredentials ) throws Exception;

    /**
     * Called after benchmark process completes.
     *
     * @param recordingDir folder containing profiler recordings
     * @param profilers list of profilers that were used
     * @param resources utility for retrieving files located in resources/ folder
     */
    protected abstract void assertOnRecordings( Path recordingDir,
                                                List<ProfilerType> profilers,
                                                Resources resources ) throws Exception;

    @Test
    @Tag( "endtoend" )
    @Timeout( value = 15, unit = TimeUnit.MINUTES )
    void runReportBenchmarks() throws Exception
    {
        try ( Resources resources = new Resources( temporaryFolder.directory( "resources" ).toPath() ) )
        {
            // fail fast, check if we have proper artifacts in place
            Path baseDir = Paths.get( System.getProperty( "user.dir" ) ).toAbsolutePath();
            Path runReportScript = baseDir.resolve( scriptName() );

            // we can be running in forked process (if run from Maven) look for base dir
            while ( !Files.isRegularFile( runReportScript ) )
            {
                baseDir = baseDir.getParent();
                runReportScript = baseDir.resolve( scriptName() );
            }

            assertNotNull( baseDir, format( "%s is not valid base dir", baseDir ) );
            assertTrue( Files.exists( runReportScript ),
                        format( "%s not found, your are running tests from invalid location", runReportScript.getFileName() ) );

            Path jar = getJar( baseDir );
            assertTrue( Files.exists( jar ),
                        format( "%s not found\n" +
                                "Make sure you have assembly in place, by running 'mvn package -P fullBenchmarks'",
                                jar.toAbsolutePath() ) );

            // assert if environment is setup
            List<ProfilerType> profilers = asList( ProfilerType.JFR, ProfilerType.ASYNC, ProfilerType.GC );

            assertSysctlParameter( asList( 1, -1 ), "kernel.perf_event_paranoid" );
            assertSysctlParameter( asList( 0 ), "kernel.kptr_restrict" );

            // setup results store schema
            try ( StoreClient storeClient = StoreClient.connect( boltUri, "neo4j", "neo4j" ) )
            {
                storeClient.execute( new CreateSchema() );
            }

            // start s3 storage mock
            Path s3Path = temporaryFolder.directory( "s3" ).toPath();
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
                EndpointConfiguration endpoint = new EndpointConfiguration( endpointUrl, "eu-north-1" );
                client = AmazonS3ClientBuilder.standard()
                                              .withPathStyleAccessEnabled( true )
                                              .withEndpointConfiguration( endpoint )
                                              .withCredentials( new AWSStaticCredentialsProvider( new AnonymousAWSCredentials() ) )
                                              .build();
                client.createBucket( "benchmarking.neo4j.com" );

                Jvm jvm = Jvm.defaultJvmOrFail();
                Process process = new ProcessBuilder( processArgs( resources,
                                                                   profilers,
                                                                   endpointUrl,
                                                                   baseDir,
                                                                   jvm,
                                                                   new ResultStoreCredentials( boltUri, "neo4j", "neo4j" ) ) )
                        .directory( baseDir.toFile() )
                        .redirectOutput( ProcessBuilder.Redirect.PIPE )
                        .redirectError( ProcessBuilder.Redirect.PIPE )
                        .start();
                BufferedReader reader = new BufferedReader( new InputStreamReader( process.getInputStream() ) );
                int processExitCode = process.waitFor();
                String line;
                while ( (line = reader.readLine()) != null )
                {
                    System.out.println( line );
                }
                assertEquals( 0, processExitCode, scriptName() + " finished with non-zero code" );
                assertStoreSchema( boltUri );
                assertRecordingFilesExist( s3Path, profilers, resources );
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
    }

    private static void assertStoreSchema( URI neo4jBoltUri )
    {
        try ( StoreClient storeClient = StoreClient.connect( neo4jBoltUri, "", "" ) )
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
                        hasItem( kernelParameterValue ) );
        }
    }

    private void assertRecordingFilesExist( Path s3Path, List<ProfilerType> profilers, Resources resources ) throws Exception
    {
        Path recordingsBasePath = s3Path.resolve( "benchmarking.neo4j.com/recordings" );
        try ( Stream<Path> files = Files.list( recordingsBasePath ) )
        {
            List<Path> recordingDirs = files
                    .peek( file -> System.out.println( file.toAbsolutePath() ) )
                    .filter( Files::isDirectory )
                    .collect( toList() );

            assertThat( "Should be exactly one new (test run) recordings folder", recordingDirs.size(), equalTo( 1 ) );
            Path recordingDir = recordingDirs.get( 0 );
            Path testRunId = recordingDir.getFileName();
            assertThat( recordingsBasePath.resolve( testRunId + ".tar.gz" ).toFile(), anExistingFile() );

            // Print out all files, to assist with debugging
            System.out.println( format( "Files in '%s':", recordingDir.toAbsolutePath() ) );
            try ( Stream<Path> recordingFiles = Files.list( recordingDir ) )
            {
                recordingFiles.forEach( file -> System.out.println( file.toAbsolutePath() ) );
            }

            assertOnRecordings( recordingDir, profilers, resources );
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
