/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.agent;

import com.neo4j.bench.agent.client.AgentClient;
import com.neo4j.bench.agent.database.DatabaseServerWrapper;
import com.neo4j.bench.agent.server.AgentInstance;
import com.neo4j.bench.common.options.Version;
import com.neo4j.bench.model.model.Neo4jConfig;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Path;
import java.util.UUID;

import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.ports.PortAuthority;
import org.neo4j.test.rule.TestDirectory;

import static com.neo4j.bench.agent.AgentTestHelper.alterDatabase;
import static com.neo4j.bench.agent.AgentTestHelper.assertArchiveContent;
import static com.neo4j.bench.agent.AgentTestHelper.assertDatabaseIsAvailable;
import static com.neo4j.bench.agent.AgentTestHelper.assertDatabaseIsNotAvailable;
import static com.neo4j.bench.agent.AgentTestHelper.assertEnvironmentVariables;
import static com.neo4j.bench.agent.AgentTestHelper.assertLogLineInFileWithTextExist;
import static com.neo4j.bench.agent.AgentTestHelper.assertSingleStore;
import static com.neo4j.bench.common.util.BenchmarkUtil.assertDirectoryIsEmpty;
import static com.neo4j.bench.common.util.BenchmarkUtil.assertDirectoryIsNotEmpty;
import static com.neo4j.bench.common.util.BenchmarkUtil.assertDoesNotExist;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

@Disabled( "Ignored until build configuration is updated" )
@TestDirectoryExtension
public class AgentServerTest
{
    private static final String DATASET_NAME = "zero";
    private static final Version VERSION = new Version( "3.5.27" );

    private S3TestSupport s3TestSupport;

    @Inject
    public TestDirectory temporaryFolder;

    private final int boltPort = PortAuthority.allocatePort();
    private final String randomString = UUID.randomUUID().toString();
    private final Neo4jConfig neo4jConfig = Neo4jConfig.empty()
            .withSetting( AgentTestHelper.BOLT_LISTEN_ADDRESS, ":" + boltPort )
            .withSetting( AgentTestHelper.HTTP_LISTEN_ADDRESS, ":" + PortAuthority.allocatePort() )
            .addJvmArg( "-Drandom=" + randomString );
    private Path tempDirectory;
    private AgentInstance agentInstance;
    private Agent agent;

    @BeforeEach
    void setup() throws IOException
    {
        assertEnvironmentVariables();
        tempDirectory = temporaryFolder.directory( "temp" );
        s3TestSupport = new S3TestSupport( temporaryFolder.directory( "s3" ), DATASET_NAME );
        s3TestSupport.setupPackage();
        s3TestSupport.setupDataset( VERSION );
        agentInstance = new AgentInstance( PortAuthority.allocatePort(), tempDirectory, s3TestSupport::artifactStorage, DatabaseServerWrapper::neo4j );
        agentInstance.start();
        assertTrue( agentInstance.isRunning() );

        agent = AgentClient.client( URI.create( "http://localhost:" + agentInstance.port() + "/" ) );
        assertAgentUninitialized( agent );
    }

    @AfterEach
    void tearDown()
    {
        agentInstance.stop();
        s3TestSupport.close();
    }

    @Test
    void readLifecycle() throws IOException
    {
        shouldPrepare();

        // first round
        URI firstUri = shouldStartDatabase( false );
        assertSingleStore( tempDirectory, DATASET_NAME );
        shouldStopDatabase( firstUri );
        shouldDownloadResults();

        // second round
        URI secondUri = shouldStartDatabase( false );
        assertSingleStore( tempDirectory, DATASET_NAME );
        shouldStopDatabase( secondUri );
        shouldDownloadResults();

        assertEquals( firstUri, secondUri );
    }

    @Test
    void writeLifecycle() throws IOException
    {
        shouldPrepare();

        // first round
        URI firstUri = shouldStartDatabase( true );
        alterDatabase( firstUri );
        shouldStopDatabase( firstUri );
        shouldDownloadResults();

        // second round
        URI secondUri = shouldStartDatabase( true );
        alterDatabase( secondUri );
        shouldStopDatabase( secondUri );
        shouldDownloadResults();

        assertEquals( firstUri, secondUri );
    }

    @Test
    void uncleanStop() throws IOException
    {
        assertDownloadThrows();
        shouldPrepare();

        assertDownloadThrows();
        shouldStartDatabase( true );

        assertDownloadThrows();
        agentInstance.stop();
        assertDirectoryIsEmpty( tempDirectory );
    }

    private void shouldPrepare()
    {
        assertTrue( agent.prepare( S3TestSupport.ARTIFACT_BASE_URI,
                s3TestSupport.packageArchive(),
                S3TestSupport.DATASET_BASE_URI,
                s3TestSupport.datasetName(),
                VERSION ) );

        assertDirectoryIsNotEmpty( tempDirectory.resolve( s3TestSupport.packageName() ) );
        assertDirectoryIsNotEmpty( tempDirectory.resolve( s3TestSupport.datasetName() ) );

        assertAgentInitialized( agent );
    }

    public URI shouldStartDatabase( boolean copyStore ) throws IOException
    {
        URI boltUri = agent.startDatabase( "Placeholder",
                copyStore,
                neo4jConfig );
        assertEquals( boltPort, boltUri.getPort() );
        assertDatabaseIsAvailable( boltUri );

        assertAgentStarted( agent, copyStore, neo4jConfig, boltUri );

        Path debugLog = tempDirectory.resolve( s3TestSupport.packageName() ).resolve( "logs" ).resolve( "debug.log" );
        assertLogLineInFileWithTextExist( debugLog, "-Drandom=" + randomString );
        assertLogLineInFileWithTextExist( debugLog, "dbms.connector.bolt.listen_address.*" + boltPort );

        return boltUri;
    }

    public void shouldStopDatabase( URI boltUri )
    {
        agent.stopDatabase();

        assertDatabaseIsNotAvailable( boltUri );
        assertSingleStore( tempDirectory, DATASET_NAME );
    }

    private void shouldDownloadResults() throws IOException
    {
        Path resultDirectory = temporaryFolder.directory( "random" + UUID.randomUUID() );

        agent.downloadResults( resultDirectory );

        assertDirectoryIsNotEmpty( resultDirectory );
        assertArchiveContent( resultDirectory );

        assertDoesNotExist( tempDirectory.resolve( "results" ) );

        assertLogLineInFileWithTextExist( resultDirectory.resolve( "debug.log" ), "-Drandom=" + randomString );
        assertLogLineInFileWithTextExist( resultDirectory.resolve( "debug.log" ), "dbms.connector.bolt.listen_address.*" + boltPort );

        assertAgentInitialized( agent );
    }

    private void assertAgentUninitialized( Agent agent )
    {
        AgentTestHelper.assertAgentUninitialized( agent.state() );
    }

    private void assertAgentInitialized( Agent agent )
    {
        AgentTestHelper.assertAgentInitialized( agent.state(), s3TestSupport.packageName(), DATASET_NAME );
    }

    private void assertAgentStarted( Agent agent, Boolean copyStore, Neo4jConfig neo4jConfig, URI boltUri )
    {
        AgentTestHelper.assertAgentStarted( agent.state(), s3TestSupport.packageName(), DATASET_NAME, copyStore, neo4jConfig, boltUri );
    }

    private void assertDownloadThrows()
    {
        try
        {
            agent.downloadResults( temporaryFolder.directory( "random" + UUID.randomUUID() ) );
            fail( "Download should not be possible" );
        }
        catch ( Exception e )
        {
            assertThat( e, Matchers.instanceOf( IllegalStateException.class ) );
        }
    }
}
