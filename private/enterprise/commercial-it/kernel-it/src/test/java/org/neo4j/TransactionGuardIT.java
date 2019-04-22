/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j;

import com.neo4j.kernel.impl.enterprise.configuration.OnlineBackupSettings;
import com.neo4j.kernel.impl.enterprise.id.CommercialIdTypeConfigurationProvider;
import com.neo4j.server.enterprise.CommercialNeoServer;
import com.neo4j.server.enterprise.helpers.CommercialServerBuilder;
import com.neo4j.test.TestCommercialDatabaseManagementServiceBuilder;
import org.junit.After;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.LongSupplier;
import java.util.function.Supplier;
import java.util.stream.Stream;

import org.neo4j.collection.Dependencies;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.configuration.Settings;
import org.neo4j.configuration.connectors.BoltConnector;
import org.neo4j.configuration.connectors.ConnectorPortRegister;
import org.neo4j.configuration.connectors.HttpConnector;
import org.neo4j.dbms.database.DatabaseManagementService;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.GraphDatabase;
import org.neo4j.driver.v1.Session;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.TransactionTerminatedException;
import org.neo4j.graphdb.config.Setting;
import org.neo4j.graphdb.facade.ExternalDependencies;
import org.neo4j.graphdb.factory.DatabaseManagementServiceBuilder;
import org.neo4j.graphdb.factory.module.id.IdContextFactory;
import org.neo4j.graphdb.factory.module.id.IdContextFactoryBuilder;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.internal.id.DefaultIdGeneratorFactory;
import org.neo4j.internal.id.IdGenerator;
import org.neo4j.internal.id.IdGeneratorFactory;
import org.neo4j.internal.id.IdRange;
import org.neo4j.internal.id.IdType;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.FileUtils;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.impl.api.transaction.monitor.KernelTransactionMonitor;
import org.neo4j.kernel.impl.scheduler.JobSchedulerFactory;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.server.CommunityNeoServer;
import org.neo4j.server.database.SimpleGraphFactory;
import org.neo4j.server.web.HttpHeaderUtils;
import org.neo4j.test.rule.CleanupRule;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.test.rule.fs.EphemeralFileSystemRule;
import org.neo4j.test.server.HTTP;
import org.neo4j.time.Clocks;
import org.neo4j.time.FakeClock;

import static org.hamcrest.Matchers.startsWith;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.configuration.GraphDatabaseSettings.transaction_timeout;
import static org.neo4j.graphdb.facade.GraphDatabaseDependencies.newDependencies;
import static org.neo4j.kernel.api.exceptions.Status.Transaction.TransactionNotFound;
import static org.neo4j.test.server.HTTP.RawPayload.quotedJson;

public class TransactionGuardIT
{
    @ClassRule
    public static final CleanupRule cleanupRule = new CleanupRule();
    @ClassRule
    public static final TestDirectory testDirectory = TestDirectory.testDirectory();

    @Rule
    public final EphemeralFileSystemRule fileSystemRule = new EphemeralFileSystemRule();

    private static final String BOLT_CONNECTOR_KEY = "bolt";

    private static final FakeClock fakeClock = Clocks.fakeClock();
    private static GraphDatabaseAPI databaseWithTimeout;
    private static GraphDatabaseAPI databaseWithoutTimeout;
    private static CommercialNeoServer neoServer;
    private static int boltPortDatabaseWithTimeout;
    private static final String DEFAULT_TIMEOUT = "2s";
    private static final KernelTransactionTimeoutMonitorSupplier monitorSupplier = new
            KernelTransactionTimeoutMonitorSupplier();
    private static final IdInjectionFunctionAction getIdInjectionFunction = new IdInjectionFunctionAction( monitorSupplier );
    private DatabaseManagementService customManagementService;

    @After
    public void tearDown()
    {
        databaseWithTimeout = null;
        databaseWithoutTimeout = null;
        if ( neoServer != null )
        {
            neoServer.stop();
            neoServer = null;
        }
        customManagementService.shutdown();
        monitorSupplier.clear();
    }

    @Test
    public void terminateLongRunningTransaction()
    {
        GraphDatabaseAPI database = startDatabaseWithTimeout();
        KernelTransactionMonitor timeoutMonitor =
                database.getDependencyResolver().resolveDependency( KernelTransactionMonitor.class );
        try ( Transaction transaction = database.beginTx() )
        {
            fakeClock.forward( 3, TimeUnit.SECONDS );
            transaction.success();
            timeoutMonitor.run();
            database.createNode();
            fail( "Transaction should be already terminated." );
        }
        catch ( TransactionTerminatedException e )
        {
            assertThat( e.getMessage(), startsWith( "The transaction has been terminated." ) );
            assertEquals( e.status(), Status.Transaction.TransactionTimedOut );
        }

        assertDatabaseDoesNotHaveNodes( database );
    }

    @Test
    public void terminateLongRunningTransactionWithPeriodicCommit() throws Exception
    {
        GraphDatabaseAPI database = startDatabaseWithTimeout();
        KernelTransactionMonitor timeoutMonitor =
                database.getDependencyResolver().resolveDependency( KernelTransactionMonitor.class );
        monitorSupplier.setTransactionMonitor( timeoutMonitor );
        try
        {
            URL url = prepareTestImportFile( 8 );
            database.execute( "USING PERIODIC COMMIT 5 LOAD CSV FROM '" + url + "' AS line CREATE ();" );
            fail( "Transaction should be already terminated." );
        }
        catch ( TransactionTerminatedException ignored )
        {
        }
        assertDatabaseDoesNotHaveNodes( database );
    }

    @Test
    public void terminateTransactionWithCustomTimeoutWithoutConfiguredDefault()
    {
        GraphDatabaseAPI database = startDatabaseWithoutTimeout();
        KernelTransactionMonitor timeoutMonitor =
                database.getDependencyResolver().resolveDependency( KernelTransactionMonitor.class );
        try ( Transaction transaction = database.beginTx( 27, TimeUnit.SECONDS ) )
        {
            fakeClock.forward( 26, TimeUnit.SECONDS );
            timeoutMonitor.run();
            database.createNode();
            transaction.failure();
        }

        try ( Transaction transaction = database.beginTx( 27, TimeUnit.SECONDS ) )
        {
            fakeClock.forward( 28, TimeUnit.SECONDS );
            timeoutMonitor.run();
            database.createNode();
            fail( "Transaction should be already terminated." );
        }
        catch ( TransactionTerminatedException e )
        {
            assertThat( e.getMessage(), startsWith( "The transaction has been terminated." ) );
        }

        assertDatabaseDoesNotHaveNodes( database );
    }

    @Test
    public void terminateLongRunningQueryTransaction()
    {
        GraphDatabaseAPI database = startDatabaseWithTimeout();
        KernelTransactionMonitor timeoutMonitor =
                database.getDependencyResolver().resolveDependency( KernelTransactionMonitor.class );
        monitorSupplier.setTransactionMonitor( timeoutMonitor );

        try ( Transaction transaction = database.beginTx() )
        {
            fakeClock.forward( 3, TimeUnit.SECONDS );
            timeoutMonitor.run();
            transaction.success();
            database.execute( "create (n)" );
            fail( "Transaction should be already terminated." );
        }
        catch ( TransactionTerminatedException e )
        {
            assertThat( e.getMessage(), startsWith( "The transaction has been terminated." ) );
        }

        assertDatabaseDoesNotHaveNodes( database );
    }

    @Test
    public void terminateLongRunningQueryWithCustomTimeoutWithoutConfiguredDefault()
    {
        GraphDatabaseAPI database = startDatabaseWithoutTimeout();
        KernelTransactionMonitor timeoutMonitor =
                database.getDependencyResolver().resolveDependency( KernelTransactionMonitor.class );
        try ( Transaction transaction = database.beginTx( 5, TimeUnit.SECONDS ) )
        {
            fakeClock.forward( 4, TimeUnit.SECONDS );
            timeoutMonitor.run();
            database.execute( "create (n)" );
            transaction.failure();
        }

        try ( Transaction transaction = database.beginTx( 6, TimeUnit.SECONDS ) )
        {
            fakeClock.forward( 7, TimeUnit.SECONDS );
            timeoutMonitor.run();
            transaction.success();
            database.execute( "create (n)" );
            fail( "Transaction should be already terminated." );
        }
        catch ( TransactionTerminatedException e )
        {
            assertThat( e.getMessage(), startsWith( "The transaction has been terminated." ) );
        }

        assertDatabaseDoesNotHaveNodes( database );
    }

    @Test
    public void terminateLongRunningRestTransactionalEndpointQuery() throws Exception
    {
        GraphDatabaseAPI database = startDatabaseWithTimeout();
        KernelTransactionMonitor timeoutMonitor =
                database.getDependencyResolver().resolveDependency( KernelTransactionMonitor.class );
        CommercialNeoServer neoServer = startNeoServer( customManagementService );
        String transactionEndPoint = HTTP.POST( transactionUri( neoServer ) ).location();

        fakeClock.forward( 3, TimeUnit.SECONDS );
        timeoutMonitor.run();

        HTTP.Response response =
                HTTP.POST( transactionEndPoint, quotedJson( "{ 'statements': [ { 'statement': 'CREATE (n)' } ] }" ) );
        assertEquals( "Response should be successful.", 200, response.status() );

        HTTP.Response commitResponse = HTTP.POST( transactionEndPoint + "/commit" );
        assertEquals( "Transaction should be already closed and not found.", 404, commitResponse.status() );

        assertEquals( "Transaction should be forcefully closed.", TransactionNotFound.code().serialize(),
                commitResponse.get( "errors" ).findValue( "code" ).asText() );
        assertDatabaseDoesNotHaveNodes( database );
    }

    @Test
    public void terminateLongRunningRestTransactionalEndpointWithCustomTimeoutQuery() throws Exception
    {
        GraphDatabaseAPI database = startDatabaseWithTimeout();
        KernelTransactionMonitor timeoutMonitor =
                database.getDependencyResolver().resolveDependency( KernelTransactionMonitor.class );
        CommercialNeoServer neoServer = startNeoServer( customManagementService );
        long customTimeout = TimeUnit.SECONDS.toMillis( 10 );
        HTTP.Response beginResponse = HTTP
                .withHeaders( HttpHeaderUtils.MAX_EXECUTION_TIME_HEADER, String.valueOf( customTimeout ) )
                .POST( transactionUri( neoServer ),
                        quotedJson( "{ 'statements': [ { 'statement': 'CREATE (n)' } ] }" ) );
        assertEquals( "Response should be successful.", 201, beginResponse.status() );

        String transactionEndPoint = beginResponse.location();
        fakeClock.forward( 3, TimeUnit.SECONDS );

        HTTP.Response response =
                HTTP.POST( transactionEndPoint, quotedJson( "{ 'statements': [ { 'statement': 'CREATE (n)' } ] }" ) );
        assertEquals( "Response should be successful.", 200, response.status() );

        fakeClock.forward( 11, TimeUnit.SECONDS );
        timeoutMonitor.run();

        response =
                HTTP.POST( transactionEndPoint, quotedJson( "{ 'statements': [ { 'statement': 'CREATE (n)' } ] }" ) );
        assertEquals( "Response should be successful.", 200, response.status() );

        HTTP.Response commitResponse = HTTP.POST( transactionEndPoint + "/commit" );
        assertEquals( "Transaction should be already closed and not found.", 404, commitResponse.status() );

        assertEquals( "Transaction should be forcefully closed.", TransactionNotFound.code().serialize(),
                commitResponse.get( "errors" ).findValue( "code" ).asText() );
        assertDatabaseDoesNotHaveNodes( database );
    }

    @Test
    public void terminateLongRunningDriverQuery() throws Exception
    {
        GraphDatabaseAPI database = startDatabaseWithTimeout();
        KernelTransactionMonitor timeoutMonitor =
                database.getDependencyResolver().resolveDependency( KernelTransactionMonitor.class );
        CommercialNeoServer neoServer = startNeoServer( customManagementService );

        org.neo4j.driver.v1.Config driverConfig = getDriverConfig();

        try ( Driver driver = GraphDatabase.driver( "bolt://localhost:" + boltPortDatabaseWithTimeout, driverConfig );
                Session session = driver.session() )
        {
            org.neo4j.driver.v1.Transaction transaction = session.beginTransaction();
            transaction.run( "create (n)" ).consume();
            transaction.success();
            fakeClock.forward( 3, TimeUnit.SECONDS );
            timeoutMonitor.run();
            try
            {
                transaction.run( "create (n)" ).consume();
                fail( "Transaction should be already terminated by execution guard." );
            }
            catch ( Exception expected )
            {
                // ignored
            }
        }
        assertDatabaseDoesNotHaveNodes( database );
    }

    @Test
    public void terminateLongRunningDriverPeriodicCommitQuery() throws Exception
    {
        GraphDatabaseAPI database = startDatabaseWithTimeout();
        KernelTransactionMonitor timeoutMonitor =
                database.getDependencyResolver().resolveDependency( KernelTransactionMonitor.class );
        monitorSupplier.setTransactionMonitor( timeoutMonitor );
        CommercialNeoServer neoServer = startNeoServer( customManagementService );

        org.neo4j.driver.v1.Config driverConfig = getDriverConfig();

        try ( Driver driver = GraphDatabase.driver( "bolt://localhost:" + boltPortDatabaseWithTimeout, driverConfig );
                Session session = driver.session() )
        {
            URL url = prepareTestImportFile( 8 );
            session.run( "USING PERIODIC COMMIT 5 LOAD CSV FROM '" + url + "' AS line CREATE ();" ).consume();
            fail( "Transaction should be already terminated by execution guard." );
        }
        catch ( Exception expected )
        {
            //
        }
        assertDatabaseDoesNotHaveNodes( database );
    }

    @Test
    public void changeTimeoutAtRuntime()
    {
        GraphDatabaseAPI database = startDatabaseWithTimeout();
        KernelTransactionMonitor timeoutMonitor =
                database.getDependencyResolver().resolveDependency( KernelTransactionMonitor.class );
        try ( Transaction transaction = database.beginTx() )
        {
            fakeClock.forward( 3, TimeUnit.SECONDS );
            timeoutMonitor.run();
            transaction.success();
            database.execute( "create (n)" );
            fail( "Transaction should be already terminated." );
        }
        catch ( TransactionTerminatedException e )
        {
            assertThat( e.getMessage(), startsWith( "The transaction has been terminated." ) );
        }

        assertDatabaseDoesNotHaveNodes( database );

        // Increase timeout
        try ( Transaction transaction = database.beginTx() )
        {
            database.execute( "CALL dbms.setConfigValue('" + transaction_timeout.name() + "', '5s')" );
            transaction.success();
        }

        try ( Transaction transaction = database.beginTx() )
        {
            fakeClock.forward( 3, TimeUnit.SECONDS );
            timeoutMonitor.run();
            transaction.success();
            database.execute( "create (n)" );
        }

        // Assert node successfully created
        try ( Transaction ignored = database.beginTx() )
        {
            assertEquals( 1, database.getAllNodes().stream().count() );
        }

        // Reset timeout and cleanup
        try ( Transaction transaction = database.beginTx() )
        {
            database.execute( "CALL dbms.setConfigValue('" + transaction_timeout.name() + "', '" + DEFAULT_TIMEOUT + "')" );
            try ( Stream<Node> stream = database.getAllNodes().stream() )
            {
                stream.findFirst().map( node ->
                {
                    node.delete();
                    return node;
                } );
            }
            transaction.success();
        }
    }

    private GraphDatabaseAPI startDatabaseWithTimeout()
    {
        if ( databaseWithTimeout == null )
        {
            Map<Setting<?>,String> configMap = getSettingsWithTimeoutAndBolt();
            databaseWithTimeout = startCustomDatabase( testDirectory.directory( "dbWithTimeout" ), configMap );
            boltPortDatabaseWithTimeout = getBoltConnectorPort( databaseWithTimeout );
        }
        return databaseWithTimeout;
    }

    private static int getBoltConnectorPort( GraphDatabaseAPI databaseAPI )
    {
        ConnectorPortRegister connectorPortRegister = databaseAPI.getDependencyResolver()
                .resolveDependency( ConnectorPortRegister.class );
        return connectorPortRegister.getLocalAddress( BOLT_CONNECTOR_KEY ).getPort();
    }

    private GraphDatabaseAPI startDatabaseWithoutTimeout()
    {
        if ( databaseWithoutTimeout == null )
        {
            Map<Setting<?>,String> configMap = getSettingsWithoutTransactionTimeout();
            databaseWithoutTimeout = startCustomDatabase( testDirectory.directory( "dbWithoutTimeout" ),
                    configMap );
        }
        return databaseWithoutTimeout;
    }

    private static org.neo4j.driver.v1.Config getDriverConfig()
    {
        return org.neo4j.driver.v1.Config.build()
                .withEncryptionLevel( org.neo4j.driver.v1.Config.EncryptionLevel.NONE )
                .toConfig();
    }

    private CommercialNeoServer startNeoServer( DatabaseManagementService databaseManagementService ) throws IOException
    {
        if ( neoServer == null )
        {
            GuardingServerBuilder serverBuilder = new GuardingServerBuilder( databaseManagementService );
            BoltConnector boltConnector = new BoltConnector( BOLT_CONNECTOR_KEY );
            serverBuilder.withProperty( boltConnector.type.name(), "BOLT" )
                    .withProperty( boltConnector.enabled.name(), Settings.TRUE )
                    .withProperty( boltConnector.encryption_level.name(),
                            BoltConnector.EncryptionLevel.DISABLED.name() )
                    .withProperty( GraphDatabaseSettings.auth_enabled.name(), Settings.FALSE );
            serverBuilder.withProperty( new HttpConnector( "http" ).listen_address.name(), "localhost:0" );
            neoServer = serverBuilder.build();
            cleanupRule.add( neoServer );
            neoServer.start();
        }
        return neoServer;
    }

    private static Map<Setting<?>,String> getSettingsWithTimeoutAndBolt()
    {
        BoltConnector boltConnector = new BoltConnector( BOLT_CONNECTOR_KEY );
        return MapUtil.genericMap(
                transaction_timeout, DEFAULT_TIMEOUT,
                boltConnector.address, "localhost:0",
                boltConnector.type, "BOLT",
                boltConnector.enabled, "true",
                boltConnector.encryption_level, BoltConnector.EncryptionLevel.DISABLED.name(),
                OnlineBackupSettings.online_backup_enabled, Settings.FALSE,
                GraphDatabaseSettings.auth_enabled, Settings.FALSE );
    }

    private static Map<Setting<?>,String> getSettingsWithoutTransactionTimeout()
    {
        return MapUtil.genericMap();
    }

    private static String transactionUri( CommercialNeoServer neoServer )
    {
        return neoServer.baseUri().toString() + "db/data/transaction";
    }

    private static URL prepareTestImportFile( int lines ) throws IOException
    {
        File tempFile = File.createTempFile( "testImport", ".csv" );
        try ( PrintWriter writer = FileUtils.newFilePrintWriter( tempFile, StandardCharsets.UTF_8 ) )
        {
            for ( int i = 0; i < lines; i++ )
            {
                writer.println( "a,b,c" );
            }
        }
        return tempFile.toURI().toURL();
    }

    private static void assertDatabaseDoesNotHaveNodes( GraphDatabaseAPI database )
    {
        try ( Transaction ignored = database.beginTx() )
        {
            assertEquals( 0, database.getAllNodes().stream().count() );
        }
    }

    private GraphDatabaseAPI startCustomDatabase( File storeDir, Map<Setting<?>,String> configMap )
    {
        configMap.put( GraphDatabaseSettings.record_id_batch_size, "1" );

        // Inject IdContextFactory
        Dependencies dependencies = new Dependencies();
        dependencies.satisfyDependencies( createIdContextFactory( configMap, fileSystemRule.get() ) );

        DatabaseManagementServiceBuilder databaseBuilder = new TestCommercialDatabaseManagementServiceBuilder()
                .setClock( fakeClock )
                .setExternalDependencies( dependencies )
                .setFileSystem( fileSystemRule.get() )
                .newImpermanentDatabaseBuilder( storeDir );
        configMap.forEach( databaseBuilder::setConfig );

        customManagementService = databaseBuilder.newDatabaseManagementService();
        return (GraphDatabaseAPI) customManagementService.database( DEFAULT_DATABASE_NAME );
    }

    private IdContextFactory createIdContextFactory( Map<Setting<?>,String> configMap, FileSystemAbstraction fileSystem )
    {
        Config config = Config.defaults();
        configMap.forEach( config::augment );

        return IdContextFactoryBuilder.of( new CommercialIdTypeConfigurationProvider( config ), JobSchedulerFactory.createScheduler() )
                .withIdGenerationFactoryProvider(
                        any -> new TerminationIdGeneratorFactory( new DefaultIdGeneratorFactory( fileSystem ) ) )
                .build();
    }

    private static class KernelTransactionTimeoutMonitorSupplier implements Supplier<KernelTransactionMonitor>
    {
        private volatile KernelTransactionMonitor transactionMonitor;

        void setTransactionMonitor( KernelTransactionMonitor transactionMonitor )
        {
            this.transactionMonitor = transactionMonitor;
        }

        @Override
        public KernelTransactionMonitor get()
        {
            return transactionMonitor;
        }

        public void clear()
        {
            setTransactionMonitor( null );
        }
    }

    private static class IdInjectionFunctionAction
    {
        private final Supplier<KernelTransactionMonitor> monitorSupplier;

        IdInjectionFunctionAction( Supplier<KernelTransactionMonitor> monitorSupplier )
        {
            this.monitorSupplier = monitorSupplier;
        }

        void tickAndCheck()
        {
            KernelTransactionMonitor timeoutMonitor = monitorSupplier.get();
            if ( timeoutMonitor != null )
            {
                fakeClock.forward( 1, TimeUnit.SECONDS );
                timeoutMonitor.run();
            }
        }
    }

    private class GuardingServerBuilder extends CommercialServerBuilder
    {
        private final DatabaseManagementService databaseManagementService;

        GuardingServerBuilder( DatabaseManagementService databaseManagementService )
        {
            super( NullLogProvider.getInstance() );
            this.databaseManagementService = databaseManagementService;
        }

        @Override
        protected CommunityNeoServer build( File configFile, Config config, ExternalDependencies dependencies )
        {
            return new GuardTestServer( config, newDependencies(dependencies).userLogProvider( NullLogProvider.getInstance() ) );
        }

        private class GuardTestServer extends CommercialNeoServer
        {
            GuardTestServer( Config config, ExternalDependencies dependencies )
            {
                super( config, new SimpleGraphFactory( databaseManagementService ), dependencies );
            }
        }
    }

    private class TerminationIdGeneratorFactory implements IdGeneratorFactory
    {
        private final IdGeneratorFactory delegate;

        TerminationIdGeneratorFactory( IdGeneratorFactory delegate )
        {
            this.delegate = delegate;
        }

        @Override
        public IdGenerator open( File filename, IdType idType, LongSupplier highIdSupplier, long maxId )
        {
            return delegate.open( filename, idType, highIdSupplier, maxId );
        }

        @Override
        public IdGenerator open( File filename, int grabSize, IdType idType, LongSupplier highIdSupplier, long maxId )
        {
            return new TerminationIdGenerator( delegate.open( filename, grabSize, idType, highIdSupplier, maxId ) );
        }

        @Override
        public void create( File filename, long highId, boolean throwIfFileExists )
        {
            delegate.create( filename, highId, throwIfFileExists );
        }

        @Override
        public IdGenerator get( IdType idType )
        {
            return delegate.get( idType );
        }
    }

    private final class TerminationIdGenerator implements IdGenerator
    {

        private final IdGenerator delegate;

        TerminationIdGenerator( IdGenerator delegate )
        {
            this.delegate = delegate;
        }

        @Override
        public IdRange nextIdBatch( int size )
        {
            return delegate.nextIdBatch( size );
        }

        @Override
        public void setHighId( long id )
        {
            delegate.setHighId( id );
        }

        @Override
        public long getHighId()
        {
            return delegate.getHighId();
        }

        @Override
        public long getHighestPossibleIdInUse()
        {
            return delegate.getHighestPossibleIdInUse();
        }

        @Override
        public void freeId( long id )
        {
            delegate.freeId( id );
        }

        @Override
        public void close()
        {
            delegate.close();
        }

        @Override
        public long getNumberOfIdsInUse()
        {
            return delegate.getNumberOfIdsInUse();
        }

        @Override
        public long getDefragCount()
        {
            return delegate.getDefragCount();
        }

        @Override
        public void delete()
        {
            delegate.delete();
        }

        @Override
        public long nextId()
        {
            getIdInjectionFunction.tickAndCheck();
            return delegate.nextId();
        }
    }
}
