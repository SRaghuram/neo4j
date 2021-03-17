/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.agent;

import com.neo4j.bench.agent.client.AgentClient;
import com.neo4j.bench.agent.client.AgentDeployer;
import com.neo4j.bench.common.options.Version;
import com.neo4j.bench.infra.InfraParams;
import com.neo4j.bench.model.model.Neo4jConfig;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.images.builder.ImageFromDockerfile;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.shaded.com.google.common.collect.ImmutableList;

import java.io.IOException;
import java.net.InetAddress;
import java.net.URI;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.SkipThreadLeakageGuard;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

import static com.neo4j.bench.agent.AgentTestHelper.assertDatabaseIsAvailable;
import static com.neo4j.bench.agent.AgentTestHelper.assertDatabaseIsNotAvailable;
import static com.neo4j.bench.agent.AgentTestHelper.assertEnvironmentVariables;
import static com.neo4j.bench.agent.AgentTestHelper.assertLogLineInFileWithTextExist;
import static com.neo4j.bench.common.util.BenchmarkUtil.assertDirectoryIsNotEmpty;
import static com.neo4j.bench.common.util.BenchmarkUtil.assertFileExists;
import static com.neo4j.bench.common.util.BenchmarkUtil.assertFileNotEmpty;
import static java.lang.String.format;
import static java.time.Duration.ofSeconds;
import static org.awaitility.Awaitility.await;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.not;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Disabled( "Ignored until build configuration is updated" )
@TestDirectoryExtension
@Testcontainers
@SkipThreadLeakageGuard // this is added because of test containers resource reaper
public class AgentClientIT
{
    private static final String DATASET_NAME = "zero";
    private static final Version VERSION = new Version( "3.5.27" );
    private static final int BOLT_PORT = 9007;

    private static final String PROCESS_NAME_OF_AGENT = AgentDeployer.AGENT_JAR_NAME + ".jar";
    private static final String PROCESS_NAME_OF_DB_SERVER = "EnterpriseEntryPoint";

    private S3TestSupport s3TestSupport;

    private final String randomString = UUID.randomUUID().toString();
    private final Neo4jConfig neo4jConfig = Neo4jConfig.empty()
                                                       .withSetting( AgentTestHelper.BOLT_LISTEN_ADDRESS, "0.0.0.0:" + BOLT_PORT )
                                                       .addJvmArg( "-Drandom=" + randomString );
    @Inject
    private TestDirectory temporaryFolder;

    @org.testcontainers.junit.jupiter.Container
    public GenericContainer<?> container =
            new GenericContainer<>( new ImageFromDockerfile().withFileFromClasspath( "Dockerfile", "Dockerfile" ) );
    private URI mappedBoltUri;

    @BeforeEach
    void setUp() throws Exception
    {
        assertEnvironmentVariables();
        s3TestSupport = new S3TestSupport( temporaryFolder.directory( "s3" ), DATASET_NAME );
        s3TestSupport.setupPackage();
        s3TestSupport.setupDataset( VERSION );
        mappedBoltUri = URI.create( format( "bolt://%s:%d", InetAddress.getLocalHost().getHostName(), container.getMappedPort( BOLT_PORT ) ) );
    }

    @AfterEach
    void tearDown()
    {
        s3TestSupport.close();
    }

    @Test
    void lifecycle() throws IOException, InterruptedException
    {
        Agent agent = shouldUploadAndStartAgent();
        shouldPrepare( agent );
        shouldStartDatabase( agent, false, neo4jConfig );
        shouldStopDatabase( agent );
        shouldDownloadResults( agent );
        shouldStopAgent( agent );
    }

    @Test
    void reinstall() throws IOException, InterruptedException
    {
        Agent firstAgent = shouldUploadAndStartAgent();
        shouldPrepare( firstAgent );
        shouldStartDatabase( firstAgent, false, neo4jConfig );

        AgentState firstAgentState = firstAgent.state();

        Agent secondAgent = shouldUploadAndStartAgent();
        shouldPrepare( secondAgent );
        Neo4jConfig modifiedNeo4jConfig = neo4jConfig.withSetting( AgentTestHelper.HTTP_LISTEN_ADDRESS, ":8080" )
                                                     .addJvmArg( "-DaddingDifferenceToConfig" );
        shouldStartDatabase( secondAgent, true, modifiedNeo4jConfig );

        AgentState secondAgentState = secondAgent.state();

        assertThat( firstAgentState.state(), equalTo( secondAgentState.state() ) );
        assertThat( firstAgentState.productName(), equalTo( secondAgentState.productName() ) );
        assertThat( firstAgentState.datasetName(), equalTo( secondAgentState.datasetName() ) );
        assertThat( firstAgentState.boltUri(), equalTo( secondAgentState.boltUri() ) );

        assertThat( firstAgentState.agentStartedAt(), not( equalTo( secondAgentState.agentStartedAt() ) ) );
        assertThat( firstAgentState.databaseStartedAt(), not( equalTo( secondAgentState.databaseStartedAt() ) ) );
        assertThat( firstAgentState.databaseConfig(), not( equalTo( secondAgentState.databaseConfig() ) ) );
        assertThat( firstAgentState.copyStore(), not( equalTo( secondAgentState.copyStore() ) ) );
    }

    private Agent shouldUploadAndStartAgent() throws IOException, InterruptedException
    {
        int advertisedTerminalPort = container.getMappedPort( AgentDeployer.DEFAULT_SSH_PORT );
        int advertisedWebPort = container.getMappedPort( Agent.DEFAULT_PORT );
        Path sourcePath = Paths.get( "target" );
        AgentDeployer agentDeployer = new AgentDeployer( InetAddress.getLocalHost(), sourcePath, advertisedTerminalPort, advertisedWebPort );
        URI agentUrl = agentDeployer.deploy( ImmutableList.of( InfraParams.CMD_AWS_ENDPOINT_URL, s3TestSupport.awsEndpoint() ) );

        Agent agent = AgentClient.client( agentUrl );
        assertNotNull( agent );
        assertJavaProcesses( PROCESS_NAME_OF_AGENT );

        assertTrue( agent.ping() );
        assertAgentUninitialized( agent );

        return agent;
    }

    private void shouldPrepare( Agent agent ) throws IOException, InterruptedException
    {
        assertTrue( agent.prepare( S3TestSupport.ARTIFACT_BASE_URI,
                                   s3TestSupport.packageArchive(),
                                   S3TestSupport.DATASET_BASE_URI,
                                   s3TestSupport.datasetName(),
                                   VERSION ) );
        assertThat( "Package is extracted", filesCountInDirectory( "/tmp/agentWork/" + s3TestSupport.packageName() ), greaterThan( 0 ) );
        assertThat( "Dataset is extracted", filesCountInDirectory( "/tmp/agentWork/" + s3TestSupport.datasetName() ), greaterThan( 0 ) );

        assertAgentInitialized( agent );
    }

    private void shouldStartDatabase( Agent agent, boolean copyStore, Neo4jConfig neo4jConfig ) throws IOException, InterruptedException
    {

        URI boltUri = agent.startDatabase( "Placeholder",
                                           copyStore,
                                           neo4jConfig );

        assertEquals( BOLT_PORT, boltUri.getPort() );
        assertDatabaseIsAvailable( mappedBoltUri );
        assertJavaProcesses( PROCESS_NAME_OF_AGENT, PROCESS_NAME_OF_DB_SERVER );
        assertAgentStarted( agent, copyStore, neo4jConfig, boltUri );

        assertLogLineWithTextExist( "-Drandom=" + randomString );
        assertLogLineWithTextExist( "dbms.connector.bolt.listen_address.*" + BOLT_PORT );
    }

    private void shouldStopDatabase( Agent agent ) throws IOException, InterruptedException
    {
        agent.stopDatabase();

        assertDatabaseIsNotAvailable( mappedBoltUri );
        assertJavaProcesses( PROCESS_NAME_OF_AGENT );

        assertAgentInitialized( agent );
    }

    private void shouldDownloadResults( Agent agent ) throws IOException, InterruptedException
    {
        Path resultDirectory = temporaryFolder.directory( "random" + UUID.randomUUID() );

        agent.downloadResults( resultDirectory );

        assertDirectoryIsNotEmpty( resultDirectory );
        assertFileNotEmpty( resultDirectory.resolve( "neo4j.conf" ) );
        assertFileNotEmpty( resultDirectory.resolve( "debug.log" ) );
        assertFileNotEmpty( resultDirectory.resolve( "neo4j.log" ) );
        assertFileNotEmpty( resultDirectory.resolve( "neo4j-out.log" ) );
        assertFileExists( resultDirectory.resolve( "neo4j-error.log" ) );
        assertFileNotEmpty( resultDirectory.resolve( Paths.get( AgentDeployer.AGENT_OUT ).getFileName() ) );
        assertFileExists( resultDirectory.resolve( Paths.get( AgentDeployer.AGENT_ERR ).getFileName() ) );

        assertLogLineInFileWithTextExist( resultDirectory.resolve( "debug.log" ), "-Drandom=" + randomString );
        assertLogLineInFileWithTextExist( resultDirectory.resolve( "debug.log" ), "dbms.connector.bolt.listen_address.*" + BOLT_PORT );
    }

    private void shouldStopAgent( Agent agent )
    {
        assertTrue( agent.stopAgent() );
        await().atMost( ofSeconds( 2 ) ).untilAsserted( this::assertJavaProcesses );
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

    private void assertJavaProcesses( String... expectedJavaProcesses ) throws IOException, InterruptedException
    {
        Set<String> runningJavaProcesses = Stream.of( container.execInContainer( "jps" )
                                                               .getStdout()
                                                               .split( "\\n" ) )
                                                 .filter( line -> !line.matches( "(?i)^\\d+ jps$" ) )
                                                 .map( line -> line.replaceAll( "^\\d+ ", "" ) )
                                                 .collect( Collectors.toSet() );
        assertThat( runningJavaProcesses, equalTo( Stream.of( expectedJavaProcesses ).collect( Collectors.toSet() ) ) );
    }

    private int filesCountInDirectory( String dir ) throws IOException, InterruptedException
    {
        Container.ExecResult result = container.execInContainer( "ls", "-1", dir );
        if ( !result.getStderr().isEmpty() )
        {
            return -1;
        }
        return result.getStdout().split( "\\n" ).length;
    }

    private void assertLogLineWithTextExist( String content ) throws IOException, InterruptedException
    {
        String debugLog = "/tmp/agentWork/" + s3TestSupport.packageName() + "/logs/debug.log";
        long filteredLogLinesCount = Stream.of( container.execInContainer( "grep", content, debugLog )
                                                         .getStdout()
                                                         .split( "\\n" ) )
                                           .count();
        assertThat( filteredLogLinesCount, greaterThan( 0L ) );
    }
}
