/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.agent;

import com.neo4j.bench.agent.client.PrepareRequest;
import com.neo4j.bench.agent.client.StartDatabaseRequest;
import com.neo4j.bench.agent.database.DatabaseServerConnection;
import com.neo4j.bench.agent.database.DatabaseServerWrapper;
import com.neo4j.bench.agent.server.AgentInstance;
import com.neo4j.bench.common.process.Pid;
import com.neo4j.bench.common.util.BenchmarkUtil;
import com.neo4j.bench.common.util.Jvm;
import com.neo4j.bench.infra.Extractor;
import com.neo4j.bench.model.model.Neo4jConfig;
import com.neo4j.bench.model.util.JsonUtil;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.net.ConnectException;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.UUID;

import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.ports.PortAuthority;
import org.neo4j.test.rule.TestDirectory;

import static com.neo4j.bench.agent.AgentTestHelper.DATASET_NAME;
import static com.neo4j.bench.agent.AgentTestHelper.VERSION;
import static com.neo4j.bench.agent.AgentTestHelper.assertEnvironmentVariables;
import static com.neo4j.bench.common.util.BenchmarkUtil.assertDirectoryIsNotEmpty;
import static com.neo4j.bench.common.util.BenchmarkUtil.assertFileExists;
import static com.neo4j.bench.common.util.BenchmarkUtil.assertFileNotEmpty;
import static java.time.Duration.ofSeconds;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;

@TestDirectoryExtension
public class AgentInstanceTest
{
    private static final URI FIX_BOLT_URI = URI.create( "bolt://localhost:9007" );
    private static final Pid FIX_PID = new Pid( 0 );

    private S3TestSupport s3TestSupport;

    @Inject
    private TestDirectory temporaryFolder;

    private Path tempDirectory;
    private AgentInstance agentInstance;
    private StubDatabaseServerWrapper databaseServerWrapper = new StubDatabaseServerWrapper();

    @BeforeEach
    void setup() throws IOException
    {
        tempDirectory = temporaryFolder.directory( "temp" );
        s3TestSupport = new S3TestSupport( temporaryFolder.directory( "s3" ), DATASET_NAME );
        agentInstance = new AgentInstance( PortAuthority.allocatePort(), tempDirectory, s3TestSupport::artifactStorage , ignored -> databaseServerWrapper );
        agentInstance.start();
        assertTrue( agentInstance.isRunning() );
    }

    @AfterEach
    void tearDown()
    {
        agentInstance.stop();
        s3TestSupport.close();
    }

    @Test
    void shouldStartAndStopWebServer() throws IOException
    {
        // when
        String pingAnswer = get( "ping" );
        // then
        assertEquals( "I am alive!", pingAnswer );

        // when
        String stopAnswer = post( "stopAgent" );
        // then
        assertEquals( "Will stop soon!", stopAnswer );

        await().atMost( ofSeconds( 2 ) ).untilAsserted( () ->
                                                        {
                                                            assertFalse( agentInstance.isRunning() );
                                                            try
                                                            {
                                                                get( "ping" );
                                                                fail();
                                                            }
                                                            catch ( Exception ce )
                                                            {
                                                                // should not be able to connect
                                                                assertThat( ce, instanceOf( ConnectException.class ) );
                                                            }
                                                        } );
    }

    @Test
    void lifecycle() throws IOException
    {
        // given
        assertEnvironmentVariables();
        s3TestSupport.setupPackage();
        s3TestSupport.setupDataset( VERSION );

        shouldPrepare();

        shouldStartDatabase();

        shouldStopDatabase();
    }

    private void shouldPrepare() throws IOException
    {
        assertLifecycle( () -> checkResponseCode( "startDatabase", this::setupStartDatabase ),
                         () -> checkResponseCode( "stopDatabase" ) );

        String answer = post( "prepare", this::setupPrepare );
        assertEquals( "Preparation done!", answer );

        assertDirectoryIsNotEmpty( tempDirectory.resolve( s3TestSupport.packageName() ) );
        assertDirectoryIsNotEmpty( tempDirectory.resolve( s3TestSupport.datasetName() ) );

        assertLifecycle( () -> checkResponseCode( "prepare", this::setupPrepare ) );
    }

    public void shouldStartDatabase() throws IOException
    {
        assertLifecycle( () -> checkResponseCode( "prepare", this::setupPrepare ),
                         () -> checkResponseCode( "stopDatabase" ) );

        String answer = post( "startDatabase", this::setupStartDatabase );
        assertEquals( FIX_BOLT_URI, URI.create( answer ) );
        assertTrue( databaseServerWrapper.started );
        assertFalse( databaseServerWrapper.logsCopied );
        assertFalse( databaseServerWrapper.stopped );

        assertLifecycle( () -> checkResponseCode( "startDatabase", this::setupStartDatabase ) );
    }

    public void shouldStopDatabase() throws IOException
    {
        assertLifecycle( () -> checkResponseCode( "prepare", this::setupPrepare ),
                         () -> checkResponseCode( "startDatabase", this::setupStartDatabase ) );

        String archiveName = post( "stopDatabase" );

        Path archive = tempDirectory.resolve( "static" ).resolve( archiveName );
        assertFileExists( archive );
        Path extract = temporaryFolder.directory( "random" + UUID.randomUUID() );
        Extractor.extract( extract, new FileInputStream( archive.toFile() ) );
        assertFileNotEmpty( extract.resolve( "placeholder" ) );

        assertTrue( databaseServerWrapper.started );
        assertTrue( databaseServerWrapper.logsCopied );
        assertTrue( databaseServerWrapper.stopped );

        assertLifecycle( () -> checkResponseCode( "stopDatabase" ) );
    }

    private void assertLifecycle( CheckResponseCode... checks ) throws IOException
    {
        for ( CheckResponseCode check : checks )
        {
            assertEquals( HttpURLConnection.HTTP_CONFLICT, check.check() );
        }
    }

    private void setupPrepare( HttpURLConnection httpURLConnection ) throws IOException
    {
        httpURLConnection.setRequestMethod( "POST" );
        httpURLConnection.setDoOutput( true );
        try ( OutputStream os = httpURLConnection.getOutputStream() )
        {
            PrepareRequest prepareRequest = PrepareRequest.from( S3TestSupport.ARTIFACT_BASE_URI,
                                                                 s3TestSupport.packageArchive(),
                                                                 S3TestSupport.DATASET_BASE_URI,
                                                                 s3TestSupport.datasetName(),
                                                                 VERSION );
            os.write( JsonUtil.serializeJson( prepareRequest ).getBytes() );
            os.flush();
        }
    }

    private void setupStartDatabase( HttpURLConnection httpURLConnection ) throws IOException
    {
        httpURLConnection.setRequestMethod( "POST" );
        httpURLConnection.setDoOutput( true );
        try ( OutputStream os = httpURLConnection.getOutputStream() )
        {
            StartDatabaseRequest startDatabaseRequest = StartDatabaseRequest.from( "Placeholder",
                                                                                   true,
                                                                                   Neo4jConfig.empty() );
            os.write( JsonUtil.serializeJson( startDatabaseRequest ).getBytes() );
            os.flush();
        }
    }

    private int checkResponseCode( String command ) throws IOException
    {
        return checkResponseCode( command, ignored ->
        {
        } );
    }

    private int checkResponseCode( String command, RequestBuilder setup ) throws IOException
    {
        HttpURLConnection connection = baseCall( "POST", command, setup );
        return connection.getResponseCode();
    }

    private String get( String command ) throws IOException
    {
        return call( "GET", command, ignored ->
        {
        } );
    }

    private String post( String command ) throws IOException
    {
        return post( command, ignored ->
        {
        } );
    }

    private String post( String command, RequestBuilder setup ) throws IOException
    {
        return call( "POST", command, setup );
    }

    private String call( String method, String command, RequestBuilder setup ) throws IOException
    {
        HttpURLConnection connection = baseCall( method, command, setup );
        BufferedReader pingReader = new BufferedReader( new InputStreamReader( connection.getInputStream() ) );
        return pingReader.readLine();
    }

    private HttpURLConnection baseCall( String method, String command, RequestBuilder setup ) throws IOException
    {
        URL url = new URL( "http://localhost:" + agentInstance.port() + "/" + command );
        HttpURLConnection connection = (HttpURLConnection) url.openConnection();
        connection.setRequestMethod( method );
        setup.setup( connection );
        return connection;
    }

    @FunctionalInterface
    private interface CheckResponseCode
    {
        int check() throws IOException;
    }

    @FunctionalInterface
    private interface RequestBuilder
    {
        void setup( HttpURLConnection con ) throws IOException;
    }

    private static class StubDatabaseServerWrapper implements DatabaseServerWrapper
    {
        boolean started;
        boolean logsCopied;
        boolean stopped;

        @Override
        public DatabaseServerConnection start( Jvm jvm, Path neo4jConfigFile, ProcessBuilder.Redirect outputRedirect, ProcessBuilder.Redirect errorRedirect )
        {
            started = true;
            return new DatabaseServerConnection()
            {
                @Override
                public URI boltUri()
                {
                    return FIX_BOLT_URI;
                }

                @Override
                public Pid pid()
                {
                    return FIX_PID;
                }
            };
        }

        @Override
        public void copyLogsTo( Path destinationFolder )
        {
            try
            {
                Path placeholder = destinationFolder.resolve( "placeholder" );
                Files.createFile( placeholder );
                BenchmarkUtil.stringToFile( "placeholder", placeholder );
            }
            catch ( IOException e )
            {
                throw new UncheckedIOException( e );
            }
            logsCopied = true;
        }

        @Override
        public void stop()
        {
            stopped = true;
        }
    }
}
