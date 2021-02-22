/*
 * Copyright (c) "Neo4j"
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
import com.neo4j.bench.client.queries.Query;
import com.neo4j.bench.client.queries.schema.CreateSchema;
import com.neo4j.bench.client.queries.schema.VerifyStoreSchema;
import com.neo4j.bench.common.profiling.ProfilerType;
import com.neo4j.bench.model.model.Parameters;
import com.neo4j.bench.model.profiling.ProfilerRecordings;
import com.neo4j.bench.model.profiling.RecordingType;
import com.neo4j.harness.junit.extension.EnterpriseNeo4jExtension;
import io.findify.s3mock.S3Mock;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.input.Tailer;
import org.apache.commons.io.input.TailerListenerAdapter;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.URI;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.configuration.connectors.BoltConnector;
import org.neo4j.configuration.connectors.ConnectorPortRegister;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.Record;
import org.neo4j.driver.Session;
import org.neo4j.driver.SessionConfig;
import org.neo4j.driver.Value;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.harness.junit.extension.Neo4jExtension;
import org.neo4j.internal.helpers.HostnamePort;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

import static java.lang.String.format;
import static java.util.Arrays.asList;
import static java.util.stream.Collectors.toList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.io.FileMatchers.anExistingFile;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.driver.AccessMode.READ;

public abstract class BaseEndToEndIT
{
    private static final Logger LOG = LoggerFactory.getLogger( BaseEndToEndIT.class );

    public interface AssertOnRecordings
    {
        /**
         * Called after benchmark process completes.
         *
         * @param recordingDir folder containing profiler recordings
         * @param profilers    list of profilers that were used
         */
        void assertOnRecordings( Path recordingDir, List<ProfilerType> profilers ) throws Exception;
    }

    @RegisterExtension
    static Neo4jExtension neo4jExtension =
            EnterpriseNeo4jExtension.builder()
                                    .withConfig( GraphDatabaseSettings.auth_enabled, false )
                                    .withConfig( BoltConnector.enabled, true )
                                    .withConfig( BoltConnector.encryption_level, BoltConnector.EncryptionLevel.OPTIONAL )
                                    .build();

    @TempDir
    protected Path temporaryFolder;

    private URI boltUri;
    private InetSocketAddress awsEndpointLocalAddress;
    private S3Mock s3api;
    private Path s3Path;

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

    protected static Path findBaseDir( String scriptName )
    {
        Path baseDir = Paths.get( System.getProperty( "user.dir" ) ).toAbsolutePath();
        Path runReportScript = baseDir.resolve( scriptName );

        while ( !Files.isRegularFile( runReportScript ) )
        {
            baseDir = baseDir.getParent();
            runReportScript = baseDir.resolve( scriptName );
        }
        return baseDir;
    }

    @BeforeEach
    public void setUp( GraphDatabaseService databaseService ) throws Exception
    {
        HostnamePort address = ((GraphDatabaseAPI) databaseService).getDependencyResolver()
                                                                   .resolveDependency( ConnectorPortRegister.class ).getLocalAddress( "bolt" );
        boltUri = URI.create( "bolt://" + address.toString() );

        // setup results store schema
        try ( StoreClient storeClient = StoreClient.connect( boltUri, "neo4j", "neo4j" ) )
        {
            storeClient.execute( new CreateSchema() );
        }

        // setup S3 mock
        s3Path = temporaryFolder.resolve( "s3" );
        s3api = new S3Mock.Builder()
                .withPort( randomLocalPort() )
                .withFileBackend( s3Path.toString() )
                .build();
        awsEndpointLocalAddress = s3api.start().localAddress();
        AmazonS3 s3client = AmazonS3ClientBuilder.standard()
                                                 .withPathStyleAccessEnabled( true )
                                                 .withEndpointConfiguration( new EndpointConfiguration( getAWSEndpointURL(), "eu-north-1" ) )
                                                 .withCredentials( new AWSStaticCredentialsProvider( new AnonymousAWSCredentials() ) )
                                                 .build();
        try
        {
            s3client.createBucket( "benchmarking.neo4j.com" );
        }
        finally
        {
            s3client.shutdown();
        }
    }

    @AfterEach
    public void tearDown( GraphDatabaseService databaseService )
    {
        s3api.shutdown();
        // this is hacky HACK, needs to be fixed in Neo4jExtension
        Transaction transaction = databaseService.beginTx();
        try ( transaction )
        {
            transaction.execute( "MATCH (n) DETACH DELETE n" ).close();
            transaction.commit();
        }
    }

    /**
     * Executes run report benchmarks test.
     *
     * @param scriptName          name of the run script
     * @param toolJar             path to tool toolJar
     * @param profilers           list of profilers to run with
     * @param processArgs         tool process arguments
     * @param recordingsAssertion additional assertions to perform on profiler recordings
     * @param recordingDirsCount  number of recording directories
     * @param expectedRecording   profiler recordings that should be uploaded to S3 and written to results store
     */
    public void runReportBenchmarks( String scriptName,
                                     Path toolJar,
                                     List<ProfilerType> profilers,
                                     List<String> processArgs,
                                     AssertOnRecordings recordingsAssertion,
                                     int recordingDirsCount,
                                     ExpectedRecordings expectedRecording ) throws Exception
    {
        runReportBenchmarks(
                scriptName,
                toolJar,
                profilers,
                processArgs,
                recordingsAssertion,
                recordingDirsCount,
                0, expectedRecording );
    }

    /**
     * Executes run report benchmarks test.
     *
     * @param scriptName          name of the run script
     * @param toolJar             path to tool toolJar
     * @param profilers           list of profilers to run with
     * @param processArgs         tool process arguments
     * @param recordingsAssertion custom assertions
     * @param recordingDirsCount
     * @param expectedExitCode    expected eun report script exit code
     * @param expectedRecording
     */
    public void runReportBenchmarks( String scriptName,
                                     Path toolJar,
                                     List<ProfilerType> profilers,
                                     List<String> processArgs,
                                     AssertOnRecordings recordingsAssertion,
                                     int recordingDirsCount,
                                     int expectedExitCode,
                                     ExpectedRecordings expectedRecording ) throws Exception
    {
        // we can be running in forked process (if run from Maven) look for base dir
        Path baseDir = findBaseDir( scriptName );
        Path resolvedToolJar = baseDir.resolve( toolJar );

        // fail fast, check if we have proper artifacts in place
        assertNotNull( baseDir, format( "%s is not valid base dir", baseDir ) );
        Path runReportScript = baseDir.resolve( scriptName );
        assertTrue( Files.exists( runReportScript ),
                    format( "%s not found, your are running tests from invalid location", runReportScript.getFileName() ) );

        assertTrue( Files.exists( resolvedToolJar ),
                    format( "%s not found\n" +
                            "Make sure you have assembly in place, by running 'mvn package -P fullBenchmarks'",
                            resolvedToolJar.toAbsolutePath() ) );

        // assert if environment is setup
        assertSysctlParameter( asList( 1, -1 ), "kernel.perf_event_paranoid" );
        assertSysctlParameter( asList( 0 ), "kernel.kptr_restrict" );

        // logs tailer
        File outputLog = temporaryFolder.resolve( "endtoend.out.log" ).toFile();
        Tailer tailer = Tailer.create( outputLog, new TailerListenerAdapter()
        {
            @Override
            public void handle( String line )
            {
                LOG.debug( format( "=> %s", line ) );
            }
        } );
        ExecutorService tailerExecutor = Executors.newSingleThreadExecutor();
        try
        {
            Process process = new ProcessBuilder( processArgs )
                    .directory( baseDir.toFile() )
                    .redirectErrorStream( true )
                    .redirectOutput( outputLog )
                    .start();
            // start watching outputlog
            tailerExecutor.submit( tailer );
            int processExitCode = process.waitFor();
            assertEquals( expectedExitCode,
                          processExitCode,
                          scriptName + " finished with unexpected exit code\n" + FileUtils.readFileToString( outputLog, Charset.defaultCharset() ) );
            assertStoreSchema( boltUri );
            assertRecordingFilesExist( s3Path, profilers, recordingsAssertion, recordingDirsCount );
            assertProfilingNodesAreSameAsS3( boltUri, s3Path, expectedRecording.expectedRecordingKeys() );
        }
        finally
        {
            try
            {
                tailer.stop();
                tailerExecutor.shutdown();
                tailerExecutor.awaitTermination( 1, TimeUnit.MINUTES );
            }
            catch ( Exception e )
            {
                LOG.debug( format( "cannot stop logs tailer\n%s", e ) );
            }
        }
    }

    protected ResultStoreCredentials getResultStoreCredentials()
    {
        return new ResultStoreCredentials( boltUri, "neo4j", "neo4j" );
    }

    protected String getAWSEndpointURL()
    {
        return format( "http://localhost:%d", awsEndpointLocalAddress.getPort() );
    }

    private static void assertStoreSchema( URI neo4jBoltUri )
    {
        try ( StoreClient storeClient = StoreClient.connect( neo4jBoltUri, "", "" ) )
        {
            storeClient.execute( new VerifyStoreSchema() );
        }
    }

    private static void assertProfilingNodesAreSameAsS3( URI neo4jBoltUri,
                                                         Path s3Path,
                                                         List<String> expectedRecordingKeys )
    {
        try ( StoreClient storeClient = StoreClient.connect( neo4jBoltUri, "", "" ) )
        {
            storeClient.execute( new VerifyProfileNodes( s3Path, expectedRecordingKeys ) );
        }
    }

    private static void assertSysctlParameter( List<Integer> allowedValues, String kernelParameter ) throws IOException
    {
        ProcessBuilder processBuilder = new ProcessBuilder( "sysctl", kernelParameter );
        try ( BufferedReader reader =
                      new BufferedReader( new InputStreamReader( processBuilder.start().getInputStream() ) ) )
        {
            var kernelParameterValues = reader.lines().collect( Collectors.toList() );
            var kernelParameterValue = kernelParameterValues.stream()
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

    private void assertRecordingFilesExist( Path s3Path,
                                            List<ProfilerType> profilers,
                                            AssertOnRecordings recordingsAssertion,
                                            int recordingDirsCount ) throws Exception
    {
        Path recordingsBasePath = s3Path.resolve( "benchmarking.neo4j.com/recordings" );
        try ( Stream<Path> files = Files.list( recordingsBasePath ) )
        {
            List<Path> recordingDirs = files
                    .peek( file -> LOG.debug( file.toAbsolutePath().toString() ) )
                    .filter( Files::isDirectory )
                    .collect( toList() );

            assertThat( format( "Should be exactly %s new (test run) recordings folders", recordingDirsCount ),
                        recordingDirs.size(),
                        equalTo( recordingDirsCount ) );

            if ( recordingDirsCount == 0 )
            {
                return;
            }

            for ( Path recordingDir : recordingDirs )
            {
                Path testRunId = recordingDir.getFileName();
                assertThat( recordingsBasePath.resolve( testRunId + ".tar.gz" ).toFile(), anExistingFile() );

                // Print out all files, to assist with debugging
                LOG.debug( format( "Files in '%s':", recordingDir.toAbsolutePath() ) );
                try ( Stream<Path> recordingFiles = Files.list( recordingDir ) )
                {
                    recordingFiles.forEach( file -> LOG.debug( file.toAbsolutePath().toString() ) );
                }

                recordingsAssertion.assertOnRecordings( recordingDir, profilers );
            }
        }
    }

    protected void assertErrorNodeCount( int nodeCount )
    {
        try ( Driver driver = GraphDatabase.driver( boltUri );
              Session session = driver.session() )
        {

            Long errCount = session
                    .readTransaction( tx -> tx.run( "MATCH (err:Error) RETURN count(err) as errCount" )
                                              .single()
                                              .get( "errCount", 0L ) );
            assertEquals( nodeCount, errCount.longValue() );
        }
    }

    private static int randomLocalPort() throws IOException
    {
        try ( ServerSocket socket = new ServerSocket( 0 ) )
        {
            return socket.getLocalPort();
        }
    }

    private static class VerifyProfileNodes implements Query<Void>
    {
        private final Path recordingsBasePath;
        private final List<String> expectedRecordingKeys;

        VerifyProfileNodes( Path recordingsBasePath, List<String> expectedRecordingKeys )
        {
            this.recordingsBasePath = recordingsBasePath;
            this.expectedRecordingKeys = expectedRecordingKeys;
        }

        @Override
        public Void execute( Driver driver )
        {
            String query = "MATCH (p:Profiles) RETURN p{.*} as profiles";
            LOG.debug( query );
            try ( Session session = driver.session( SessionConfig.builder().withDefaultAccessMode( READ ).build() ) )
            {
                List<Record> results = session.run( query ).list();
                for ( Record record : results )
                {
                    Map<String,String> profiles = record.get( "profiles" ).asMap( Value::asString );
                    assertResultsStoreEntriesExistInS3( profiles );
                    assertAllExpectedRecordingsExistInResultsStore( profiles );
                }
            }
            return null;
        }

        private void assertResultsStoreEntriesExistInS3( Map<String,String> profiles )
        {
            for ( String storePath : profiles.values() )
            {
                Path s3ProfilerRecordingPath = recordingsBasePath.resolve( storePath );
                assertTrue( Files.exists( s3ProfilerRecordingPath ), () -> format( "Expected to find '%s'", s3ProfilerRecordingPath.toString() ) );
            }
        }

        private void assertAllExpectedRecordingsExistInResultsStore( Map<String,String> profiles )
        {
            String[] actualRecordingKeys = profiles.keySet().toArray( new String[0] );
            assertThat( format( ":Profiles node should have properties %s but had %s", expectedRecordingKeys, profiles.keySet() ),
                        expectedRecordingKeys,
                        containsInAnyOrder( actualRecordingKeys ) );
        }

        @Override
        public Optional<String> nonFatalError()
        {
            return Optional.empty();
        }
    }

    public static class ExpectedRecordings
    {
        public static ExpectedRecordings from( Collection<ProfilerType> profilers )
        {
            return from( profilers, Parameters.NONE );
        }

        public static ExpectedRecordings from( Collection<ProfilerType> profilers, Parameters... parameters )
        {
            List<RecordingType> recordingTypes = profilers.stream().flatMap( p -> p.allRecordingTypes().stream() ).collect( toList() );
            List<String> expectedRecordingKeys = recordingTypes.stream()
                                                               .flatMap( r -> Arrays.stream( parameters )
                                                                                    .map( p -> ProfilerRecordings.profilesPropertyKeyFor( r, p ) ) )
                                                               .collect( toList() );
            return new ExpectedRecordings( expectedRecordingKeys );
        }

        private final List<String> expectedRecordingTypes;

        private ExpectedRecordings( List<String> expectedRecordingTypes )
        {
            this.expectedRecordingTypes = expectedRecordingTypes;
        }

        public ExpectedRecordings with( RecordingType recordingType, Parameters parameters )
        {
            expectedRecordingTypes.add( ProfilerRecordings.profilesPropertyKeyFor( recordingType, parameters ) );
            return this;
        }

        private List<String> expectedRecordingKeys()
        {
            return expectedRecordingTypes;
        }
    }
}
