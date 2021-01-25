/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.bolt;

import com.neo4j.bolt.txtracking.WaitTrackingMonitor;
import com.neo4j.configuration.OnlineBackupSettings;
import com.neo4j.enterprise.edition.EnterpriseEditionModule;
import com.neo4j.test.driver.DriverExtension;
import com.neo4j.test.driver.DriverFactory;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.parallel.ResourceLock;
import org.junit.jupiter.api.parallel.Resources;

import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Function;
import java.util.stream.Stream;

import org.neo4j.bolt.txtracking.TransactionIdTrackerMonitor;
import org.neo4j.bolt.v4.runtime.bookmarking.BookmarkWithDatabaseId;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.configuration.connectors.BoltConnector;
import org.neo4j.configuration.connectors.ConnectorPortRegister;
import org.neo4j.configuration.helpers.ReadOnlyDatabaseChecker;
import org.neo4j.configuration.helpers.SocketAddress;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.driver.Bookmark;
import org.neo4j.driver.Driver;
import org.neo4j.driver.Session;
import org.neo4j.driver.SessionConfig;
import org.neo4j.driver.Transaction;
import org.neo4j.driver.exceptions.TransientException;
import org.neo4j.driver.internal.InternalBookmark;
import org.neo4j.fabric.bolt.FabricBookmark;
import org.neo4j.fabric.bolt.FabricBookmarkParser;
import org.neo4j.graphdb.facade.DatabaseManagementServiceFactory;
import org.neo4j.graphdb.facade.GraphDatabaseDependencies;
import org.neo4j.graphdb.factory.module.GlobalModule;
import org.neo4j.graphdb.factory.module.edition.AbstractEditionModule;
import org.neo4j.internal.helpers.collection.Iterables;
import org.neo4j.internal.kernel.api.exceptions.TransactionFailureException;
import org.neo4j.io.IOUtils;
import org.neo4j.kernel.database.Database;
import org.neo4j.kernel.database.NamedDatabaseId;
import org.neo4j.kernel.impl.api.CommitProcessFactory;
import org.neo4j.kernel.impl.api.TransactionCommitProcess;
import org.neo4j.kernel.impl.api.InternalTransactionCommitProcess;
import org.neo4j.kernel.impl.api.TransactionToApply;
import org.neo4j.kernel.impl.transaction.log.TransactionAppender;
import org.neo4j.kernel.impl.transaction.tracing.CommitEvent;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.storageengine.api.StorageEngine;
import org.neo4j.storageengine.api.TransactionApplicationMode;
import org.neo4j.storageengine.api.TransactionIdStore;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.SuppressOutputExtension;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

import static java.util.concurrent.Executors.newSingleThreadExecutor;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.stream.Collectors.toSet;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.neo4j.configuration.GraphDatabaseSettings.SYSTEM_DATABASE_NAME;
import static org.neo4j.internal.helpers.NamedThreadFactory.daemon;
import static org.neo4j.kernel.api.exceptions.Status.Transaction.BookmarkTimeout;
import static org.neo4j.kernel.impl.factory.DbmsInfo.ENTERPRISE;
import static org.neo4j.test.assertion.Assert.assertEventually;
import static org.neo4j.test.conditions.Conditions.TRUE;
import static org.neo4j.test.conditions.Conditions.equalityCondition;

@TestDirectoryExtension
@DriverExtension
@ExtendWith( SuppressOutputExtension.class )
@ResourceLock( Resources.SYSTEM_OUT )
class BookmarkIT
{
    @Inject
    private TestDirectory directory;
    @Inject
    private DriverFactory driverFactory;

    private Driver driver;
    private GraphDatabaseAPI db;
    private DatabaseManagementService managementService;
    private ExecutorService executor;

    @AfterEach
    void afterEach()
    {
        IOUtils.closeAllSilently( driver );
        if ( executor != null )
        {
            executor.shutdownNow();
        }
        if ( managementService != null )
        {
            managementService.shutdown();
        }
    }

    @Test
    void shouldReturnUpToDateBookmarkWhenSomeTransactionIsCommitting() throws Exception
    {
        CommitBlocker commitBlocker = new CommitBlocker();
        db = createDbms( commitBlocker );
        driver = driverFactory.graphDatabaseDriver( boltAddress( db ) );

        Bookmark firstBookmark = createNode( driver );

        // make next transaction append to the log and then pause before applying to the store
        // this makes it allocate a transaction ID but wait before acknowledging the commit operation
        commitBlocker.blockNextTransaction();
        CompletableFuture<Bookmark> secondBookmarkFuture = CompletableFuture.supplyAsync( () -> createNode( driver ) );
        assertEventually( "Transaction did not block as expected", commitBlocker::hasBlockedTransaction, TRUE, 1, MINUTES );

        Set<Bookmark> otherBookmarks = Stream.generate( () -> createNode( driver ) ).limit( 10 ).collect( toSet() );

        commitBlocker.unblock();
        Bookmark lastBookmark = secondBookmarkFuture.get();

        // first and last bookmarks should not be null and should be different
        assertNotNull( firstBookmark );
        assertNotNull( lastBookmark );
        assertNotEquals( firstBookmark, lastBookmark );

        // all bookmarks received while a transaction was blocked committing should be unique
        assertThat( otherBookmarks ).hasSize( 10 );
    }

    @Test
    void shouldReturnBookmarkInNewFormat() throws Exception
    {
        db = createDbms();
        driver = driverFactory.graphDatabaseDriver( boltAddress( db ) );

        var bookmark = (InternalBookmark) createNode( driver );
        var bookmarkStr = Iterables.first( bookmark.values() );

        var split = bookmarkStr.split( ":" );
        assertThat( split ).hasSize( 2 );
    }

    @Test
    void shouldFailForUnreachableSystemDatabaseBookmark() throws IOException
    {
        db = createDbms();
        driver = driverFactory.graphDatabaseDriver( boltAddress( db ) );

        var unreachableSystemDbBookmark = bumpedSystemDbBookmark( 9999 );

        var error = assertThrows( TransientException.class,
                () -> createDatabase( "bar", unreachableSystemDbBookmark ) );

        assertEquals( BookmarkTimeout.code().serialize(), error.code() );
    }

    @Test
    void shouldWaitForSystemDatabaseBookmark( TestInfo testInfo ) throws Exception
    {
        var waitTrackingMonitor = new WaitTrackingMonitor();
        db = createDbms( waitTrackingMonitor );
        driver = driverFactory.graphDatabaseDriver( boltAddress( db ) );
        executor = newSingleThreadExecutor( daemon( "thread-" + testInfo.getDisplayName() ) );

        var txCount = 5;
        var systemDbBookmark = bumpedSystemDbBookmark( txCount );

        var future = executor.submit( () -> createDatabase( "foo", systemDbBookmark ) );

        waitTrackingMonitor.clearWaiting();
        assertFalse( future.isDone() );
        assertEventually( "Tracker did not begin waiting", waitTrackingMonitor::isWaiting, equalityCondition( true ), 1, MINUTES );

        createDatabase( "bar" );
        waitTrackingMonitor.clearWaiting();
        assertFalse( future.isDone() );
        assertEventually( "Tracker did not continue waiting", waitTrackingMonitor::isWaiting, equalityCondition( true ), 1, MINUTES );

        assertThat( managementService.listDatabases() ).as( "Dbms already has database foo" ).doesNotContain( "foo" );

        for ( var i = 0; i < txCount; i++ )
        {
            createDatabase( "baz" + i );
        }

        // Check that "CREATE DATABASE foo" has executed successfully
        assertEventually( future::get, equalityCondition( true ), 1, MINUTES );

        var databaseNames = managementService.listDatabases();
        assertThat( databaseNames ).contains( "foo" );
        assertThat( databaseNames ).contains( "bar" );
        for ( var i = 0; i < txCount; i++ )
        {
            assertThat( databaseNames ).contains( "baz" + i );
        }
    }

    private GraphDatabaseAPI createDbms( CommitBlocker commitBlocker )
    {
        return createDbms( globalModule -> new CustomEnterpriseEditionModule( globalModule, commitBlocker ) );
    }

    private GraphDatabaseAPI createDbms( TransactionIdTrackerMonitor monitor )
    {
        return createDbms( globalModule -> new EnterpriseEditionModuleWithMonitor( globalModule, monitor ) );
    }

    private GraphDatabaseAPI createDbms()
    {
        return createDbms( EnterpriseEditionModule::new );
    }

    private GraphDatabaseAPI createDbms( Function<GlobalModule,AbstractEditionModule> editionModuleFactory )
    {
        var factory = new DatabaseManagementServiceFactory( ENTERPRISE, editionModuleFactory );
        managementService = factory.build( configWithBoltEnabled(), GraphDatabaseDependencies.newDependencies() );
        return (GraphDatabaseAPI) managementService.database( GraphDatabaseSettings.DEFAULT_DATABASE_NAME );
    }

    private long lastCommittedSystemDatabaseTxId()
    {
        var db = (GraphDatabaseAPI) managementService.database( SYSTEM_DATABASE_NAME );
        return db.getDependencyResolver().resolveDependency( TransactionIdStore.class ).getLastCommittedTransactionId();
    }

    private Bookmark systemDatabaseBookmark( long txId )
    {
        var db = (GraphDatabaseAPI) managementService.database( SYSTEM_DATABASE_NAME );
        var databaseId = db.getDependencyResolver().resolveDependency( Database.class ).getNamedDatabaseId();
        return InternalBookmark.parse( new BookmarkWithDatabaseId( txId, databaseId ).toString() );
    }

    private Bookmark bumpedSystemDbBookmark( long txIdDelta )
    {
        try
        {
            return bumpedSystemDbBookmarkFabric( txIdDelta );
        }
        catch ( Exception e )
        {
            return systemDatabaseBookmark( lastCommittedSystemDatabaseTxId() + txIdDelta );
        }
    }

    private Bookmark bumpedSystemDbBookmarkFabric( long txIdDelta )
    {
        Bookmark bookmark = showDatabases( null );
        var parser = new FabricBookmarkParser();
        var fabricBookmarks = bookmark.values().stream()
                                      .map( parser::parse )
                                      .map( fabricBookmark ->
                                            {
                                                FabricBookmark.InternalGraphState systemDbState = fabricBookmark.getInternalGraphStates().get( 0 );
                                                FabricBookmark.InternalGraphState systemDbStateNew =
                                                        new FabricBookmark.InternalGraphState( systemDbState.getGraphUuid(),
                                                                                               systemDbState.getTransactionId() + txIdDelta );
                                                FabricBookmark fabricBookmarkNew =
                                                        new FabricBookmark( List.of( systemDbStateNew ), fabricBookmark.getExternalGraphStates() );
                                                return fabricBookmarkNew.serialize();
                                            } )
                                      .collect( toSet() );

        return InternalBookmark.parse( fabricBookmarks );
    }

    private void createDatabase( String databaseName )
    {
        createDatabase( databaseName, null );
    }

    private boolean createDatabase( String databaseName, Bookmark systemDatabaseBookmark )
    {
        var sessionConfig = SessionConfig.builder()
                .withDatabase( SYSTEM_DATABASE_NAME )
                .withBookmarks( systemDatabaseBookmark == null ? List.of() : List.of( systemDatabaseBookmark ) )
                .build();

        try ( var session = driver.session( sessionConfig ) )
        {
            session.run( "CREATE DATABASE " + databaseName ).consume();
        }
        return true;
    }

    private Bookmark showDatabases( Bookmark systemDatabaseBookmark )
    {
        var sessionConfig = SessionConfig.builder()
                                         .withDatabase( SYSTEM_DATABASE_NAME )
                                         .withBookmarks( systemDatabaseBookmark == null ? List.of() : List.of( systemDatabaseBookmark ) )
                                         .build();

        try ( var session = driver.session( sessionConfig ) )
        {
            session.run( "SHOW DATABASES" ).consume();
            return session.lastBookmark();
        }
    }

    private static Bookmark createNode( Driver driver )
    {
        try ( Session session = driver.session() )
        {
            try ( Transaction tx = session.beginTransaction() )
            {
                tx.run( "CREATE ()" );
                tx.commit();
            }
            return session.lastBookmark();
        }
    }

    private Config configWithBoltEnabled()
    {
        return Config.newBuilder()
                .set( BoltConnector.enabled, true )
                .set( OnlineBackupSettings.online_backup_enabled, false )
                .set( BoltConnector.listen_address, new SocketAddress( "localhost", 0 ) )
                .set( GraphDatabaseSettings.neo4j_home, directory.absolutePath() )
                .set( GraphDatabaseSettings.bookmark_ready_timeout, Duration.ofMinutes( 5 ) )
                .set( GraphDatabaseSettings.preallocate_logical_logs, false )
                .build();
    }

    private static String boltAddress( GraphDatabaseAPI db )
    {
        ConnectorPortRegister portRegister = db.getDependencyResolver().resolveDependency( ConnectorPortRegister.class );
        return "bolt://" + portRegister.getLocalAddress( "bolt" );
    }

    private static class EnterpriseEditionModuleWithMonitor extends EnterpriseEditionModule
    {
        EnterpriseEditionModuleWithMonitor( GlobalModule globalModule, TransactionIdTrackerMonitor monitor )
        {
            super( globalModule );
            globalModule.getGlobalMonitors().addMonitorListener( monitor );
        }
    }

    private static class CustomEnterpriseEditionModule extends EnterpriseEditionModule
    {
        CustomEnterpriseEditionModule( GlobalModule globalModule, CommitBlocker commitBlocker )
        {
            super( globalModule );
            commitProcessFactory = new CustomCommitProcessFactory( commitBlocker );
        }
    }

    private static class CustomCommitProcessFactory implements CommitProcessFactory
    {
        final CommitBlocker commitBlocker;

        private CustomCommitProcessFactory( CommitBlocker commitBlocker )
        {
            this.commitBlocker = commitBlocker;
        }

        @Override
        public TransactionCommitProcess create( TransactionAppender appender, StorageEngine storageEngine, NamedDatabaseId databaseId,
                                                ReadOnlyDatabaseChecker readOnlyDatabaseChecker )
        {
            return new CustomCommitProcessInternal( appender, storageEngine, commitBlocker );
        }
    }

    private static class CustomCommitProcessInternal extends InternalTransactionCommitProcess
    {
        final CommitBlocker commitBlocker;

        CustomCommitProcessInternal( TransactionAppender appender, StorageEngine storageEngine, CommitBlocker commitBlocker )
        {
            super( appender, storageEngine );
            this.commitBlocker = commitBlocker;
        }

        @Override
        protected void applyToStore( TransactionToApply batch, CommitEvent commitEvent, TransactionApplicationMode mode ) throws TransactionFailureException
        {
            commitBlocker.blockWhileWritingToStoreIfNeeded();
            super.applyToStore( batch, commitEvent, mode );
        }
    }

    private static class CommitBlocker
    {
        final ReentrantLock lock = new ReentrantLock();
        volatile boolean shouldBlock;

        void blockNextTransaction()
        {
            shouldBlock = true;
            lock.lock();
        }

        void blockWhileWritingToStoreIfNeeded()
        {
            if ( shouldBlock )
            {
                shouldBlock = false;
                lock.lock();
            }
        }

        void unblock()
        {
            lock.unlock();
        }

        boolean hasBlockedTransaction()
        {
            return lock.getQueueLength() == 1;
        }
    }
}
