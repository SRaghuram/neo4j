/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j;

import com.neo4j.configuration.OnlineBackupSettings;
import com.neo4j.test.TestEnterpriseDatabaseManagementServiceBuilder;
import org.eclipse.collections.api.set.ImmutableSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.PrintWriter;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.LongSupplier;
import java.util.function.Supplier;
import java.util.stream.Stream;

import org.neo4j.collection.Dependencies;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.configuration.SettingValueParsers;
import org.neo4j.configuration.connectors.BoltConnector;
import org.neo4j.configuration.connectors.ConnectorPortRegister;
import org.neo4j.configuration.connectors.HttpConnector;
import org.neo4j.configuration.helpers.SocketAddress;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.Logging;
import org.neo4j.driver.Session;
import org.neo4j.driver.exceptions.Neo4jException;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.TransactionTerminatedException;
import org.neo4j.graphdb.config.Setting;
import org.neo4j.graphdb.factory.module.id.IdContextFactory;
import org.neo4j.graphdb.factory.module.id.IdContextFactoryBuilder;
import org.neo4j.internal.helpers.collection.MapUtil;
import org.neo4j.internal.id.DefaultIdGeneratorFactory;
import org.neo4j.internal.id.IdGenerator;
import org.neo4j.internal.id.IdGeneratorFactory;
import org.neo4j.internal.id.IdType;
import org.neo4j.io.fs.EphemeralFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.FileUtils;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.impl.api.transaction.monitor.KernelTransactionMonitor;
import org.neo4j.kernel.impl.scheduler.JobSchedulerFactory;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.server.helpers.TestWebContainer;
import org.neo4j.server.web.HttpHeaderUtils;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.pagecache.EphemeralPageCacheExtension;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.test.server.HTTP;
import org.neo4j.time.Clocks;
import org.neo4j.time.FakeClock;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.configuration.GraphDatabaseSettings.transaction_timeout;
import static org.neo4j.index.internal.gbptree.RecoveryCleanupWorkCollector.immediate;
import static org.neo4j.kernel.api.exceptions.Status.Transaction.TransactionNotFound;
import static org.neo4j.test.server.HTTP.RawPayload.quotedJson;

@EphemeralPageCacheExtension
class TransactionGuardIT
{
    @Inject
    private TestDirectory testDirectory;
    @Inject
    private EphemeralFileSystemAbstraction fileSystem;
    @Inject
    private PageCache pageCache;

    private static final FakeClock fakeClock = Clocks.fakeClock();
    private static GraphDatabaseAPI databaseWithTimeout;
    private static GraphDatabaseAPI databaseWithoutTimeout;
    private static TestWebContainer testWebContainer;
    private static int boltPortDatabaseWithTimeout;
    private static final String DEFAULT_TIMEOUT = "2s";
    private static final KernelTransactionTimeoutMonitorSupplier monitorSupplier = new
            KernelTransactionTimeoutMonitorSupplier();
    private static final IdInjectionFunctionAction getIdInjectionFunction = new IdInjectionFunctionAction( monitorSupplier );
    private DatabaseManagementService customManagementService;

    @AfterEach
    void tearDown()
    {
        databaseWithTimeout = null;
        databaseWithoutTimeout = null;
        if ( testWebContainer != null )
        {
            testWebContainer.shutdown();
            testWebContainer = null;
        }
        customManagementService.shutdown();
        monitorSupplier.clear();
    }

    @Test
    void terminateLongRunningTransaction()
    {
        GraphDatabaseAPI database = startDatabaseWithTimeout();
        KernelTransactionMonitor timeoutMonitor =
                database.getDependencyResolver().resolveDependency( KernelTransactionMonitor.class );
        TransactionTerminatedException exception = assertThrows( TransactionTerminatedException.class, () ->
        {
            try ( Transaction transaction = database.beginTx() )
            {
                fakeClock.forward( 3, TimeUnit.SECONDS );
                timeoutMonitor.run();
                transaction.createNode();
            }
        } );

        assertThat( exception.getMessage() ).startsWith( "The transaction has been terminated." );
        assertEquals( Status.Transaction.TransactionTimedOut, exception.status() );

        assertDatabaseDoesNotHaveNodes( database );
    }

    @Test
    void terminateLongRunningTransactionWithPeriodicCommit()
    {
        GraphDatabaseAPI database = startDatabaseWithTimeout();
        KernelTransactionMonitor timeoutMonitor =
                database.getDependencyResolver().resolveDependency( KernelTransactionMonitor.class );
        monitorSupplier.setTransactionMonitor( timeoutMonitor );
        assertThrows( TransactionTerminatedException.class, () ->
        {
            URL url = prepareTestImportFile( 8 );
            database.executeTransactionally( "USING PERIODIC COMMIT 5 LOAD CSV FROM '" + url + "' AS line CREATE ();" );
        } );
        assertDatabaseDoesNotHaveNodes( database );
    }

    @Test
    void terminateTransactionWithCustomTimeoutWithoutConfiguredDefault()
    {
        GraphDatabaseAPI database = startDatabaseWithoutTimeout();
        KernelTransactionMonitor timeoutMonitor =
                database.getDependencyResolver().resolveDependency( KernelTransactionMonitor.class );
        try ( Transaction transaction = database.beginTx( 27, TimeUnit.SECONDS ) )
        {
            fakeClock.forward( 26, TimeUnit.SECONDS );
            timeoutMonitor.run();
            transaction.createNode();
            transaction.rollback();
        }

        TransactionTerminatedException exception = assertThrows( TransactionTerminatedException.class, () ->
        {
            try ( Transaction transaction = database.beginTx( 27, TimeUnit.SECONDS ) )
            {
                fakeClock.forward( 28, TimeUnit.SECONDS );
                timeoutMonitor.run();
                transaction.createNode();
            }
        } );
        assertThat( exception.getMessage() ).startsWith( "The transaction has been terminated." );

        assertDatabaseDoesNotHaveNodes( database );
    }

    @Test
    void terminateLongRunningQueryTransaction()
    {
        GraphDatabaseAPI database = startDatabaseWithTimeout();
        KernelTransactionMonitor timeoutMonitor =
                database.getDependencyResolver().resolveDependency( KernelTransactionMonitor.class );
        monitorSupplier.setTransactionMonitor( timeoutMonitor );

        TransactionTerminatedException exception = assertThrows( TransactionTerminatedException.class, () ->
        {
            try ( Transaction tx = database.beginTx() )
            {
                fakeClock.forward( 3, TimeUnit.SECONDS );
                timeoutMonitor.run();
                tx.execute( "create (n)" );
            }
        } );
        assertThat( exception.getMessage() ).startsWith( "The transaction has been terminated." );

        assertDatabaseDoesNotHaveNodes( database );
    }

    @Test
    void terminateLongRunningQueryWithCustomTimeoutWithoutConfiguredDefault()
    {
        GraphDatabaseAPI database = startDatabaseWithoutTimeout();
        KernelTransactionMonitor timeoutMonitor =
                database.getDependencyResolver().resolveDependency( KernelTransactionMonitor.class );
        try ( Transaction transaction = database.beginTx( 5, TimeUnit.SECONDS ) )
        {
            fakeClock.forward( 4, TimeUnit.SECONDS );
            timeoutMonitor.run();
            transaction.execute( "create (n)" );
            transaction.rollback();
        }

        TransactionTerminatedException exception = assertThrows( TransactionTerminatedException.class, () ->
        {
            try ( Transaction tx = database.beginTx( 6, TimeUnit.SECONDS ) )
            {
                fakeClock.forward( 7, TimeUnit.SECONDS );
                timeoutMonitor.run();
                tx.execute( "create (n)" );
            }
        } );
        assertThat( exception.getMessage() ).startsWith( "The transaction has been terminated." );

        assertDatabaseDoesNotHaveNodes( database );
    }

    @Test
    void terminateLongRunningRestTransactionalEndpointQuery() throws Exception
    {
        GraphDatabaseAPI database = startDatabaseWithTimeout();
        KernelTransactionMonitor timeoutMonitor =
                database.getDependencyResolver().resolveDependency( KernelTransactionMonitor.class );
        var webServerContainer = createWebContainer( customManagementService );
        String transactionEndPoint = HTTP.POST( transactionUri( webServerContainer ) ).location();

        fakeClock.forward( 3, TimeUnit.SECONDS );
        timeoutMonitor.run();

        HTTP.Response response =
                HTTP.POST( transactionEndPoint, quotedJson( "{ 'statements': [ { 'statement': 'CREATE (n)' } ] }" ) );
        assertEquals( 200, response.status(), "Response should be successful." );

        HTTP.Response commitResponse = HTTP.POST( transactionEndPoint + "/commit" );
        assertEquals( 404, commitResponse.status(), "Transaction should be already closed and not found." );

        assertEquals( TransactionNotFound.code().serialize(),
                commitResponse.get( "errors" ).findValue( "code" ).asText(), "Transaction should be forcefully closed." );
        assertDatabaseDoesNotHaveNodes( database );
    }

    @Test
    void terminateLongRunningRestTransactionalEndpointWithCustomTimeoutQuery() throws Exception
    {
        GraphDatabaseAPI database = startDatabaseWithTimeout();
        KernelTransactionMonitor timeoutMonitor =
                database.getDependencyResolver().resolveDependency( KernelTransactionMonitor.class );
        var neoServer = createWebContainer( customManagementService );
        long customTimeout = TimeUnit.SECONDS.toMillis( 10 );
        HTTP.Response beginResponse = HTTP
                .withHeaders( HttpHeaderUtils.MAX_EXECUTION_TIME_HEADER, String.valueOf( customTimeout ) )
                .POST( transactionUri( neoServer ),
                        quotedJson( "{ 'statements': [ { 'statement': 'CREATE (n)' } ] }" ) );
        assertEquals( 201, beginResponse.status(), "Response should be successful." );

        String transactionEndPoint = beginResponse.location();
        fakeClock.forward( 3, TimeUnit.SECONDS );

        HTTP.Response response =
                HTTP.POST( transactionEndPoint, quotedJson( "{ 'statements': [ { 'statement': 'CREATE (n)' } ] }" ) );
        assertEquals( 200, response.status(), "Response should be successful." );

        fakeClock.forward( 11, TimeUnit.SECONDS );
        timeoutMonitor.run();

        response =
                HTTP.POST( transactionEndPoint, quotedJson( "{ 'statements': [ { 'statement': 'CREATE (n)' } ] }" ) );
        assertEquals( 200, response.status(), "Response should be successful." );

        HTTP.Response commitResponse = HTTP.POST( transactionEndPoint + "/commit" );
        assertEquals( 404, commitResponse.status(), "Transaction should be already closed and not found." );

        assertEquals( TransactionNotFound.code().serialize(),
                commitResponse.get( "errors" ).findValue( "code" ).asText(), "Transaction should be forcefully closed." );
        assertDatabaseDoesNotHaveNodes( database );
    }

    @Test
    void terminateLongRunningDriverQuery() throws Exception
    {
        GraphDatabaseAPI database = startDatabaseWithTimeout();
        KernelTransactionMonitor timeoutMonitor =
                database.getDependencyResolver().resolveDependency( KernelTransactionMonitor.class );
        createWebContainer( customManagementService );

        org.neo4j.driver.Config driverConfig = getDriverConfig();

        try ( Driver driver = GraphDatabase.driver( "bolt://localhost:" + boltPortDatabaseWithTimeout, driverConfig );
                Session session = driver.session() )
        {
            org.neo4j.driver.Transaction transaction = session.beginTransaction();
            transaction.run( "create (n)" ).consume();
            fakeClock.forward( 3, TimeUnit.SECONDS );
            timeoutMonitor.run();
            Neo4jException e = assertThrows( Neo4jException.class, transaction.run( "create (n)" )::consume );
            assertEquals( Status.Transaction.TransactionTimedOut.code().serialize(), e.code() );
        }
        assertDatabaseDoesNotHaveNodes( database );
    }

    @Test
    void terminateLongRunningDriverPeriodicCommitQuery()
    {
        GraphDatabaseAPI database = startDatabaseWithTimeout();
        KernelTransactionMonitor timeoutMonitor =
                database.getDependencyResolver().resolveDependency( KernelTransactionMonitor.class );
        monitorSupplier.setTransactionMonitor( timeoutMonitor );
        createWebContainer( customManagementService );

        org.neo4j.driver.Config driverConfig = getDriverConfig();

        assertThrows( Exception.class, () ->
        {
            try ( Driver driver = GraphDatabase.driver( "bolt://localhost:" + boltPortDatabaseWithTimeout, driverConfig );
                  Session session = driver.session() )
            {
                URL url = prepareTestImportFile( 8 );
                session.run( "USING PERIODIC COMMIT 5 LOAD CSV FROM '" + url + "' AS line CREATE ();" ).consume();
            }
        } );
        assertDatabaseDoesNotHaveNodes( database );
    }

    @Test
    void changeTimeoutAtRuntime()
    {
        GraphDatabaseAPI database = startDatabaseWithTimeout();
        KernelTransactionMonitor timeoutMonitor =
                database.getDependencyResolver().resolveDependency( KernelTransactionMonitor.class );
        TransactionTerminatedException exception = assertThrows( TransactionTerminatedException.class, () ->
        {
            try ( Transaction tx = database.beginTx() )
            {
                fakeClock.forward( 3, TimeUnit.SECONDS );
                timeoutMonitor.run();
                tx.execute( "create (n)" );
            }
        } );
        assertThat( exception.getMessage() ).startsWith( "The transaction has been terminated." );

        assertDatabaseDoesNotHaveNodes( database );

        // Increase timeout
        try ( Transaction transaction = database.beginTx() )
        {
            transaction.execute( "CALL dbms.setConfigValue('" + transaction_timeout.name() + "', '5s')" );
            transaction.commit();
        }

        try ( Transaction transaction = database.beginTx() )
        {
            fakeClock.forward( 3, TimeUnit.SECONDS );
            timeoutMonitor.run();
            transaction.execute( "create (n)" );
            transaction.commit();
        }

        // Assert node successfully created
        try ( Transaction tx = database.beginTx() )
        {
            assertEquals( 1, tx.getAllNodes().stream().count() );
        }

        // Reset timeout and cleanup
        try ( Transaction transaction = database.beginTx() )
        {
            transaction.execute( "CALL dbms.setConfigValue('" + transaction_timeout.name() + "', '" + DEFAULT_TIMEOUT + "')" );
            try ( Stream<Node> stream = transaction.getAllNodes().stream() )
            {
                stream.findFirst().map( node ->
                {
                    node.delete();
                    return node;
                } );
            }
            transaction.commit();
        }
    }

    private GraphDatabaseAPI startDatabaseWithTimeout()
    {
        if ( databaseWithTimeout == null )
        {
            databaseWithTimeout = startCustomDatabase( testDirectory.directory( "dbWithTimeout" ), getSettingsWithTimeoutAndBolt() );
            boltPortDatabaseWithTimeout = getBoltConnectorPort( databaseWithTimeout );
        }
        return databaseWithTimeout;
    }

    private static int getBoltConnectorPort( GraphDatabaseAPI databaseAPI )
    {
        ConnectorPortRegister connectorPortRegister = databaseAPI.getDependencyResolver()
                .resolveDependency( ConnectorPortRegister.class );
        return connectorPortRegister.getLocalAddress( BoltConnector.NAME ).getPort();
    }

    private GraphDatabaseAPI startDatabaseWithoutTimeout()
    {
        if ( databaseWithoutTimeout == null )
        {
            databaseWithoutTimeout = startCustomDatabase( testDirectory.directory( "dbWithoutTimeout" ), getSettingsWithoutTransactionTimeout() );
        }
        return databaseWithoutTimeout;
    }

    private static org.neo4j.driver.Config getDriverConfig()
    {
        return org.neo4j.driver.Config.builder()
                .withoutEncryption()
                .withLogging( Logging.none() )
                .build();
    }

    private TestWebContainer createWebContainer( DatabaseManagementService databaseManagementService )
    {
        if ( testWebContainer == null )
        {
            testWebContainer = new TestWebContainer( databaseManagementService );
        }
        return testWebContainer;
    }

    private static Map<Setting<?>,Object> getSettingsWithTimeoutAndBolt()
    {
        return MapUtil.genericMap(
                transaction_timeout, SettingValueParsers.DURATION.parse( DEFAULT_TIMEOUT ),
                BoltConnector.enabled, true,
                BoltConnector.listen_address, new SocketAddress( "localhost", 0 ),
                BoltConnector.encryption_level, BoltConnector.EncryptionLevel.DISABLED,
                HttpConnector.enabled, true,
                HttpConnector.advertised_address, new SocketAddress( "localhost", 0 ),
                OnlineBackupSettings.online_backup_enabled, false,
                GraphDatabaseSettings.auth_enabled, false );
    }

    private static Map<Setting<?>,Object> getSettingsWithoutTransactionTimeout()
    {
        return MapUtil.genericMap();
    }

    private static String transactionUri( TestWebContainer testWebContainer )
    {
        return testWebContainer.getBaseUri().toString() + "db/neo4j/tx";
    }

    private static URL prepareTestImportFile( int lines ) throws IOException
    {
        Path tempFile = Files.createTempFile( "testImport", ".csv" );
        try ( PrintWriter writer = FileUtils.newFilePrintWriter( tempFile, StandardCharsets.UTF_8 ) )
        {
            for ( int i = 0; i < lines; i++ )
            {
                writer.println( "a,b,c" );
            }
        }
        return tempFile.toUri().toURL();
    }

    private static void assertDatabaseDoesNotHaveNodes( GraphDatabaseAPI database )
    {
        try ( Transaction transaction = database.beginTx() )
        {
            assertEquals( 0, transaction.getAllNodes().stream().count() );
        }
    }

    private GraphDatabaseAPI startCustomDatabase( Path storeDir, Map<Setting<?>,Object> configMap )
    {
        // Inject IdContextFactory
        Dependencies dependencies = new Dependencies();
        dependencies.satisfyDependencies( createIdContextFactory( fileSystem ) );

        var databaseBuilder = new TestEnterpriseDatabaseManagementServiceBuilder( storeDir )
                        .setClock( fakeClock )
                        .setExternalDependencies( dependencies )
                        .setFileSystem( fileSystem ).impermanent();
        databaseBuilder.setConfig( configMap );

        customManagementService = databaseBuilder.build();
        return (GraphDatabaseAPI) customManagementService.database( DEFAULT_DATABASE_NAME );
    }

    private IdContextFactory createIdContextFactory( FileSystemAbstraction fileSystem )
    {
        return IdContextFactoryBuilder.of( fileSystem, JobSchedulerFactory.createScheduler(), Config.defaults(), PageCacheTracer.NULL )
                .withIdGenerationFactoryProvider(
                        any -> new TerminationIdGeneratorFactory( new DefaultIdGeneratorFactory( fileSystem, immediate() ) ) )
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

        void clear()
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

    private static class TerminationIdGeneratorFactory implements IdGeneratorFactory
    {
        private final IdGeneratorFactory delegate;

        TerminationIdGeneratorFactory( IdGeneratorFactory delegate )
        {
            this.delegate = delegate;
        }

        @Override
        public IdGenerator open( PageCache pageCache, Path filename, IdType idType, LongSupplier highIdSupplier, long maxId, boolean readOnly, Config config,
                PageCursorTracer cursorTracer, ImmutableSet<OpenOption> openOptions ) throws IOException
        {
            return new TerminationIdGenerator(
                    delegate.open( pageCache, filename, idType, highIdSupplier, maxId, readOnly, config, cursorTracer, openOptions ) );
        }

        @Override
        public IdGenerator create( PageCache pageCache, Path filename, IdType idType, long highId, boolean throwIfFileExists, long maxId, boolean readOnly,
                Config config, PageCursorTracer cursorTracer, ImmutableSet<OpenOption> openOptions ) throws IOException
        {
            return new TerminationIdGenerator(
                    delegate.create( pageCache, filename, idType, highId, throwIfFileExists, maxId, readOnly, config, cursorTracer, openOptions ) );
        }

        @Override
        public IdGenerator get( IdType idType )
        {
            return delegate.get( idType );
        }

        @Override
        public void visit( Consumer<IdGenerator> visitor )
        {
            delegate.visit( visitor );
        }

        @Override
        public void clearCache( PageCursorTracer cursorTracer )
        {
            delegate.clearCache( cursorTracer );
        }

        @Override
        public Collection<Path> listIdFiles()
        {
            return delegate.listIdFiles();
        }
    }

    private static final class TerminationIdGenerator extends IdGenerator.Delegate
    {
        TerminationIdGenerator( IdGenerator delegate )
        {
            super( delegate );
        }

        @Override
        public long nextId( PageCursorTracer cursorTracer )
        {
            getIdInjectionFunction.tickAndCheck();
            return super.nextId( cursorTracer );
        }
    }
}
