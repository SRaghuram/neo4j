/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.bolt;

import org.junit.After;
import org.junit.Rule;
import org.junit.Test;

import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Function;
import java.util.stream.Stream;

import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.configuration.connectors.BoltConnector;
import org.neo4j.configuration.connectors.ConnectorPortRegister;
import org.neo4j.configuration.helpers.SocketAddress;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.Session;
import org.neo4j.driver.Transaction;
import org.neo4j.graphdb.facade.DatabaseManagementServiceFactory;
import org.neo4j.graphdb.facade.GraphDatabaseDependencies;
import org.neo4j.graphdb.factory.module.GlobalModule;
import org.neo4j.graphdb.factory.module.edition.AbstractEditionModule;
import org.neo4j.graphdb.factory.module.edition.CommunityEditionModule;
import org.neo4j.internal.kernel.api.exceptions.TransactionFailureException;
import org.neo4j.io.IOUtils;
import org.neo4j.kernel.impl.api.CommitProcessFactory;
import org.neo4j.kernel.impl.api.TransactionCommitProcess;
import org.neo4j.kernel.impl.api.TransactionRepresentationCommitProcess;
import org.neo4j.kernel.impl.api.TransactionToApply;
import org.neo4j.kernel.impl.factory.DatabaseInfo;
import org.neo4j.kernel.impl.transaction.log.TransactionAppender;
import org.neo4j.kernel.impl.transaction.tracing.CommitEvent;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.storageengine.api.StorageEngine;
import org.neo4j.storageengine.api.TransactionApplicationMode;
import org.neo4j.test.rule.TestDirectory;

import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.stream.Collectors.toSet;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.arrayWithSize;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.neo4j.test.assertion.Assert.assertEventually;

public class BookmarkIT
{
    @Rule
    public final TestDirectory directory = TestDirectory.testDirectory();

    private Driver driver;
    private GraphDatabaseAPI db;
    private DatabaseManagementService managementService;

    @After
    public void tearDown() throws Exception
    {
        IOUtils.closeAllSilently( driver );
        if ( managementService != null )
        {
            managementService.shutdown();
        }
    }

    @Test
    public void shouldReturnUpToDateBookmarkWhenSomeTransactionIsCommitting() throws Exception
    {
        CommitBlocker commitBlocker = new CommitBlocker();
        db = createDb( commitBlocker );
        driver = GraphDatabase.driver( boltAddress( db ) );

        String firstBookmark = createNode( driver );

        // make next transaction append to the log and then pause before applying to the store
        // this makes it allocate a transaction ID but wait before acknowledging the commit operation
        commitBlocker.blockNextTransaction();
        CompletableFuture<String> secondBookmarkFuture = CompletableFuture.supplyAsync( () -> createNode( driver ) );
        assertEventually( "Transaction did not block as expected", commitBlocker::hasBlockedTransaction, is( true ), 1, MINUTES );

        Set<String> otherBookmarks = Stream.generate( () -> createNode( driver ) ).limit( 10 ).collect( toSet() );

        commitBlocker.unblock();
        String lastBookmark = secondBookmarkFuture.get();

        // first and last bookmarks should not be null and should be different
        assertNotNull( firstBookmark );
        assertNotNull( lastBookmark );
        assertNotEquals( firstBookmark, lastBookmark );

        // all bookmarks received while a transaction was blocked committing should be unique
        assertThat( otherBookmarks, hasSize( 10 ) );
    }

    @Test
    public void shouldReturnBookmarkInNewFormat() throws Exception
    {
        db = createDb();
        driver = GraphDatabase.driver( boltAddress( db ) );

        var bookmark = createNode( driver );
        var split = bookmark.split( ":" );
        assertThat( split, arrayWithSize( 2 ) );
    }

    private GraphDatabaseAPI createDb( CommitBlocker commitBlocker )
    {
        return createDb( globalModule -> new CustomCommunityEditionModule( globalModule, commitBlocker ) );
    }

    private GraphDatabaseAPI createDb()
    {
        return createDb( CommunityEditionModule::new );
    }

    private GraphDatabaseAPI createDb( Function<GlobalModule,AbstractEditionModule> editionModuleFactory )
    {
        DatabaseManagementServiceFactory facadeFactory = new DatabaseManagementServiceFactory( DatabaseInfo.COMMUNITY, editionModuleFactory );
        managementService = facadeFactory.build( directory.storeDir(), configWithBoltEnabled(), GraphDatabaseDependencies.newDependencies() );
        return (GraphDatabaseAPI) managementService.database(
                GraphDatabaseSettings.DEFAULT_DATABASE_NAME );
    }

    private static String createNode( Driver driver )
    {
        try ( Session session = driver.session() )
        {
            try ( Transaction tx = session.beginTransaction() )
            {
                tx.run( "CREATE ()" );
                tx.success();
            }
            return session.lastBookmark();
        }
    }

    private Config configWithBoltEnabled()
    {
        return Config.newBuilder()
                .set( BoltConnector.enabled, true )
                .set( BoltConnector.listen_address, new SocketAddress( "localhost", 0 ) )
                .set( GraphDatabaseSettings.neo4j_home, directory.storeDir().toPath().toAbsolutePath() )
                .build();
    }

    private static String boltAddress( GraphDatabaseAPI db )
    {
        ConnectorPortRegister portRegister = db.getDependencyResolver().resolveDependency( ConnectorPortRegister.class );
        return "bolt://" + portRegister.getLocalAddress( "bolt" );
    }

    private static class CustomCommunityEditionModule extends CommunityEditionModule
    {
        CustomCommunityEditionModule( GlobalModule globalModule, CommitBlocker commitBlocker )
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
        public TransactionCommitProcess create( TransactionAppender appender, StorageEngine storageEngine, Config config )
        {
            return new CustomCommitProcess( appender, storageEngine, commitBlocker );
        }
    }

    private static class CustomCommitProcess extends TransactionRepresentationCommitProcess
    {
        final CommitBlocker commitBlocker;

        CustomCommitProcess( TransactionAppender appender, StorageEngine storageEngine, CommitBlocker commitBlocker )
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
