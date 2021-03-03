/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.macro.execution;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.AnonymousAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.google.common.collect.ImmutableMap;
import com.neo4j.bench.client.reporter.TarGzArchive;
import com.neo4j.bench.common.Neo4jConfigBuilder;
import com.neo4j.bench.common.database.DatabaseName;
import com.neo4j.bench.common.database.Neo4jStore;
import com.neo4j.bench.common.options.Version;
import com.neo4j.bench.common.process.Pid;
import com.neo4j.bench.common.results.BenchmarkGroupDirectory;
import com.neo4j.bench.common.results.ForkDirectory;
import com.neo4j.bench.common.tool.macro.Deployment;
import com.neo4j.bench.common.tool.macro.ExecutionMode;
import com.neo4j.bench.common.util.BenchmarkUtil;
import com.neo4j.bench.common.util.Jvm;
import com.neo4j.bench.common.util.Resources;
import com.neo4j.bench.infra.ArtifactStorage;
import com.neo4j.bench.infra.aws.AWSS3ArtifactStorage;
import com.neo4j.bench.macro.StoreTestUtil;
import com.neo4j.bench.macro.agent.WorkspaceState;
import com.neo4j.bench.macro.agent.WorkspaceStorage;
import com.neo4j.bench.macro.cli.RunMacroWorkloadCommand;
import com.neo4j.bench.macro.execution.database.EmbeddedDatabase;
import com.neo4j.bench.macro.execution.database.Neo4jServerDatabase;
import com.neo4j.bench.macro.execution.database.Schema;
import com.neo4j.bench.macro.execution.database.ServerDatabase;
import com.neo4j.bench.macro.execution.process.DatabaseLauncher;
import com.neo4j.bench.macro.execution.process.MeasurementOptions;
import com.neo4j.bench.macro.workload.Workload;
import com.neo4j.bench.model.model.Benchmark;
import com.neo4j.bench.model.model.BenchmarkGroup;
import com.neo4j.bench.model.model.Neo4jConfig;
import com.neo4j.bench.model.options.Edition;
import io.findify.s3mock.S3Mock;
import org.apache.commons.io.FileUtils;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.stream.Stream;

import org.neo4j.configuration.connectors.BoltConnector;
import org.neo4j.configuration.connectors.HttpConnector;
import org.neo4j.driver.exceptions.ServiceUnavailableException;
import org.neo4j.graphdb.Transaction;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.ports.PortAuthority;
import org.neo4j.test.rule.TestDirectory;

import static com.neo4j.bench.model.model.Benchmark.benchmarkFor;
import static java.lang.String.format;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

@TestDirectoryExtension
public class Neo4jDeploymentIT
{
    private static final int BOLT_PORT = PortAuthority.allocatePort();

    private static final Jvm JVM = Jvm.defaultJvmOrFail();
    private static final Neo4jConfig NEO_4J_CONFIG = Neo4jConfigBuilder.empty()
                                                                       .withSetting( BoltConnector.listen_address, ":" + BOLT_PORT )
                                                                       .withSetting( HttpConnector.listen_address, ":" + PortAuthority.allocatePort() )
                                                                       .build();
    private static final String DATASET_NAME = "ldbc_sf001";
    private static final String S3_BUCKET = "benchmarking.neo4j.com";
    private static final URI ARTIFACT_BASE_URI = URI.create( String.format( "s3://%s/artifacts", S3_BUCKET ) );
    private static final URI DATASET_BASE_URI = URI.create( String.format( "s3://%s/datasets", S3_BUCKET ) );
    private static final BenchmarkGroup BENCHMARK_GROUP = new BenchmarkGroup( "group" );
    private static final Benchmark BENCHMARK = benchmarkFor( "description", "simpleName", Benchmark.Mode.THROUGHPUT, Collections.emptyMap() );
    private static final ExecutionMode EXECUTION_MODE = ExecutionMode.EXECUTE;
    private static final Version VERSION = new Version( "4.0.27" );
    private static final Edition EDITION = Edition.ENTERPRISE;
    private static final MeasurementOptions MEASUREMENT_OPTIONS = new MeasurementOptions( 0, 0, Duration.ZERO, Duration.ZERO );

    @Inject
    private TestDirectory temporaryFolder;

    private final long randomId = new Random().nextLong();
    private Path emptyConfig;
    private S3Mock s3api;
    private ArtifactStorage artifactStorage;
    private String packageName;
    private String packageArchive;
    private Schema expectedSchema;
    private Path results;
    private Pid pid;
    private DatabaseName databaseName;

    @BeforeEach
    void setUp() throws Exception
    {
        this.emptyConfig = temporaryFolder.createFile( "config" );

        Path s3Path = temporaryFolder.directory( "s3" );
        this.s3api = createS3( s3Path );
        this.artifactStorage = createArtifactStorage( s3api );

        Path originalDir = Paths.get( System.getenv( "NEO4J_DIR" ) );
        this.packageName = originalDir.getFileName().toString();
        this.packageArchive = packageName + ".tar.gz";
        Path packageDir = temporaryFolder.directory( "package" );
        FileUtils.copyDirectory( originalDir.toFile(), packageDir.resolve( packageName ).toFile() );

        Path dbDir = temporaryFolder.directory( "db" );
        Path resourcesDir = temporaryFolder.directory( "resources" );
        this.expectedSchema = createDataset( resourcesDir, dbDir, emptyConfig, randomId );
        this.databaseName = getDatabaseName( resourcesDir );

        uploadDirectoryToS3AndRemove( s3Path, packageDir, "/artifacts", packageArchive );
        uploadDirectoryToS3AndRemove( s3Path, dbDir, "/datasets/4.0-enterprise-datasets", DATASET_NAME + ".tgz" );

        this.results = temporaryFolder.directory( "results" );
    }

    private static S3Mock createS3( Path s3Path )
    {
        return new S3Mock.Builder()
                .withPort( PortAuthority.allocatePort() )
                .withFileBackend( s3Path.toString() )
                .build();
    }

    private static ArtifactStorage createArtifactStorage( S3Mock s3api )
    {
        InetSocketAddress awsEndpointLocalAddress = s3api.start().localAddress();
        String awsEndpointURI = format( "http://localhost:%d", awsEndpointLocalAddress.getPort() );
        AmazonS3 s3client = AmazonS3ClientBuilder.standard()
                                                 .withPathStyleAccessEnabled( true )
                                                 .withEndpointConfiguration( new AwsClientBuilder.EndpointConfiguration( awsEndpointURI, "eu-north-1" ) )
                                                 .withCredentials( new AWSStaticCredentialsProvider( new AnonymousAWSCredentials() ) )
                                                 .build();
        s3client.createBucket( S3_BUCKET );
        s3client.shutdown();
        return AWSS3ArtifactStorage.create( new AwsClientBuilder.EndpointConfiguration( awsEndpointURI, "eu-north-1" ) );
    }

    private static Schema createDataset( Path resourcesDir, Path dbDir, Path neo4jConfigFile, long randomId ) throws IOException
    {
        try ( Resources resources = new Resources( resourcesDir ) )
        {
            Workload workload = Workload.fromName( DATASET_NAME, resources, Deployment.embedded() );

            Path dataset = dbDir.resolve( DATASET_NAME );
            Files.createDirectories( dataset );
            StoreTestUtil.createEmptyStoreFor( workload, dataset, neo4jConfigFile );
            Neo4jStore store = Neo4jStore.createFrom( dataset );
            try ( EmbeddedDatabase database = EmbeddedDatabase.startWith( store, EDITION, neo4jConfigFile ) )
            {
                database.executeAndGetRows( "CREATE (:foo {id:$id})", ImmutableMap.of( "id", randomId ), false, false );
            }
            EmbeddedDatabase.recreateSchema( store, EDITION, neo4jConfigFile, workload.expectedSchema() );
            return workload.expectedSchema();
        }
    }

    private static DatabaseName getDatabaseName( Path resourcesDir )
    {
        try ( Resources resources = new Resources( resourcesDir ) )
        {
            Workload workload = Workload.fromName( DATASET_NAME, resources, Deployment.embedded() );
            return workload.getDatabaseName();
        }
    }

    private static void uploadDirectoryToS3AndRemove( Path s3Root, Path sourceDir, String targetDir, String targetName ) throws IOException
    {
        Path s3Directory = s3Root.resolve( S3_BUCKET + targetDir );
        FileUtils.forceMkdir( s3Directory.toFile() );
        TarGzArchive.compress( s3Directory.resolve( targetName ), sourceDir );
        FileUtils.deleteDirectory( sourceDir.toFile() );
    }

    @AfterEach
    void tearDown()
    {
        s3api.shutdown();
        artifactStorage.close();
    }

    @AfterEach
    void killDatabase()
    {
        if ( pid != null )
        {
            kill( pid );
        }
    }

    @Test
    void shouldDownloadPrepareStartAndStopForReadWorkload()
    {
        Path workspace = temporaryFolder.directory( "workspace" );
        Path baseNeo4jConfigFile = workspace.resolve( UUID.randomUUID() + "-neo4j.conf" );
        writeConfig( baseNeo4jConfigFile );

        Neo4jDeployment<?> deployment = shouldSetup( workspace, baseNeo4jConfigFile );

        // first round
        URI uri;
        ForkDirectory forkDirectory1 = forkDir();
        try ( ServerDatabase database = shouldStart( baseNeo4jConfigFile, deployment, false, forkDirectory1 ) )
        {
            uri = database.boltUri();
            assertRunning( database, forkDirectory1 );
        }
        shouldStop( workspace, uri );

        // second round
        ForkDirectory forkDirectory2 = forkDir();
        try ( ServerDatabase database = shouldStart( baseNeo4jConfigFile, deployment, false, forkDirectory2 ) )
        {
            assertRunning( database, forkDirectory2 );
        }
        shouldStop( workspace, uri );
    }

    @Test
    void shouldDownloadPrepareStartAndStopForWriteWorkload()
    {
        Path workspace = temporaryFolder.directory( "workspace" );
        Path baseNeo4jConfigFile = workspace.resolve( UUID.randomUUID() + "-neo4j.conf" );
        writeConfig( baseNeo4jConfigFile );

        Neo4jDeployment<?> deployment = shouldSetup( workspace, baseNeo4jConfigFile );

        // first round
        URI uri;
        ForkDirectory forkDirectory1 = forkDir();
        try ( ServerDatabase database = shouldStart( baseNeo4jConfigFile, deployment, true, forkDirectory1 ) )
        {
            uri = database.boltUri();
            assertRunning( database, forkDirectory1 );
            updateDatabase( database.boltUri() );
        }
        shouldStop( workspace, uri );

        // second round
        ForkDirectory forkDirectory2 = forkDir();
        try ( ServerDatabase database = shouldStart( baseNeo4jConfigFile, deployment, true, forkDirectory2 ) )
        {
            assertRunning( database, forkDirectory2 );
        }
        shouldStop( workspace, uri );
    }

    private ForkDirectory forkDir()
    {
        return BenchmarkGroupDirectory.findOrCreateAt( results, BENCHMARK_GROUP )
                                      .findOrCreate( BENCHMARK )
                                      .findOrCreate( UUID.randomUUID().toString() );
    }

    private Neo4jDeployment<?> shouldSetup( Path workspace, Path baseNeo4jConfigFile )
    {
        WorkspaceState workspaceState = shouldDownloadWorkspace( workspace );
        Path dataset = workspaceState.dataset();
        // deployment won't be started so this path does not matter
        Neo4jDeployment<?> deployment = Neo4jDeployment.from( Deployment.server( workspaceState.product().toAbsolutePath().toString() ),
                                                              EDITION,
                                                              MEASUREMENT_OPTIONS,
                                                              JVM,
                                                              dataset );
        RunMacroWorkloadCommand.verifySchema( dataset,
                                              EDITION,
                                              baseNeo4jConfigFile,
                                              expectedSchema,
                                              false );
        return deployment;
    }

    private WorkspaceState shouldDownloadWorkspace( Path workspace )
    {
        WorkspaceStorage workspaceStorage = new WorkspaceStorage( artifactStorage,
                                                                  workspace,
                                                                  ARTIFACT_BASE_URI,
                                                                  packageArchive,
                                                                  DATASET_BASE_URI,
                                                                  DATASET_NAME,
                                                                  VERSION );
        WorkspaceState workspaceState = workspaceStorage.download();

        assertThat( workspaceState.product(), equalTo( workspace.resolve( packageName ) ) );
        BenchmarkUtil.assertDirectoryIsNotEmpty( workspaceState.product() );

        assertThat( workspaceState.dataset(), equalTo( workspace.resolve( DATASET_NAME ) ) );
        BenchmarkUtil.assertDirectoryIsNotEmpty( workspaceState.dataset() );
        return workspaceState;
    }

    private ServerDatabase shouldStart( Path baseNeo4jConfigFile, Neo4jDeployment<?> deployment, boolean copyStore, ForkDirectory forkDirectory )
    {
        Neo4jConfig neo4jConfig = RunMacroWorkloadCommand.prepareConfig( EXECUTION_MODE, baseNeo4jConfigFile );
        Path neo4jConfigFile = forkDirectory.create( "neo4j.conf" );
        Neo4jConfigBuilder.writeToFile( neo4jConfig, neo4jConfigFile );
        List<String> additionalJvmArgs = Collections.emptyList();

        DatabaseLauncher<ServerDatabase> databaseLauncher =
                (DatabaseLauncher<ServerDatabase>) deployment.launcherFor( copyStore, neo4jConfigFile, forkDirectory );
        return databaseLauncher.initDatabaseServer( additionalJvmArgs );
    }

    private void assertRunning( ServerDatabase serverDatabase, ForkDirectory forkDirectory )
    {
        pid = serverDatabase.pid();
        assertDatabaseIsAvailable( serverDatabase.boltUri() );
        BenchmarkUtil.assertFileExists( forkDirectory.pathFor( "neo4j-error.log" ) );
        BenchmarkUtil.assertFileNotEmpty( forkDirectory.pathFor( "neo4j-out.log" ) );
    }

    private void assertDatabaseIsAvailable( URI boltUri )
    {
        assertEquals( BOLT_PORT, boltUri.getPort() );
        try ( ServerDatabase client = Neo4jServerDatabase.connectClient( boltUri, databaseName, new Pid( 0 ) ) )
        {
            long id = client.session().run( "MATCH (n:foo) RETURN n.id AS i" ).single().get( "i", -1L );
            assertEquals( randomId, id );
        }
    }

    private void assertDatabaseIsNotAvailable( URI boltUri )
    {
        try ( ServerDatabase client = Neo4jServerDatabase.connectClient( boltUri, databaseName, new Pid( 0 ) ) )
        {
            client.session().run( "MATCH (n:foo) RETURN n.id AS i" ).consume();
            fail( "Client should not be able to connect" );
        }
        catch ( Exception e )
        {
            assertThat( e, Matchers.instanceOf( ServiceUnavailableException.class ) );
        }
    }

    private void updateDatabase( URI boltUri )
    {
        try ( ServerDatabase client = Neo4jServerDatabase.connectClient( boltUri, databaseName, new Pid( 0 ) ) )
        {
            client.session().run( "MATCH (n:foo) SET n.id = $id", ImmutableMap.of( "id", randomId + 1 ) ).consume();
        }
    }

    private void shouldStop( Path workspace, URI boltUri )
    {
        assertDatabaseIsNotAvailable( boltUri );
        assertOriginalStoreIsNotChanged( workspace );
        assertSingleStore( workspace );
    }

    private void assertOriginalStoreIsNotChanged( Path workspace )
    {
        try ( EmbeddedDatabase database = EmbeddedDatabase.startWith( Neo4jStore.createFrom( workspace.resolve( DATASET_NAME ) ),
                                                                      EDITION,
                                                                      emptyConfig );
              Transaction transaction = database.db().beginTx() )
        {
            long id = ((Number) transaction.execute( "MATCH (n:foo) RETURN n.id AS i" ).next().get( "i" )).longValue();
            assertEquals( randomId, id );
        }
    }

    private void assertSingleStore( Path workspace )
    {
        assertThat( countStoresOrStoreCopies( workspace ), equalTo( 1L ) );
    }

    private long countStoresOrStoreCopies( Path workspace )
    {
        try
        {
            try ( Stream<Path> list = Files.list( workspace ) )
            {
                return list.filter( file -> file.getFileName().toString().startsWith( DATASET_NAME ) ).count();
            }
        }
        catch ( IOException e )
        {
            throw new UncheckedIOException( e );
        }
    }

    private static void writeConfig( Path neo4jConfigFile )
    {
        Neo4jConfigBuilder neo4jConfigBuilder = Neo4jConfigBuilder.empty();
        neo4jConfigBuilder.mergeWith( NEO_4J_CONFIG );
        Neo4jConfigBuilder.writeToFile( NEO_4J_CONFIG, neo4jConfigFile );
    }

    private static void kill( Pid pid )
    {
        String[] command = System.getProperty( "os.name" ).toLowerCase().contains( "windows" ) ?
                           new String[]{"taskkill", "/F", "/t", "/FI", "/PID", String.valueOf( pid.get() )} :
                           new String[]{"kill", "-KILL", String.valueOf( pid.get() )};

        try
        {
            new ProcessBuilder( command ).start().waitFor();
        }
        catch ( InterruptedException | IOException e )
        {
            throw new RuntimeException( e );
        }
    }
}
