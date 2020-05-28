/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.bolt;

import com.neo4j.configuration.OnlineBackupSettings;
import com.neo4j.test.TestEnterpriseDatabaseManagementServiceBuilder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.function.Consumer;

import org.neo4j.bolt.runtime.BoltConnectionMetricsMonitor;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.configuration.connectors.BoltConnector;
import org.neo4j.configuration.helpers.SocketAddress;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.dbms.api.DatabaseManagementServiceBuilder;
import org.neo4j.driver.Driver;
import org.neo4j.driver.Session;
import org.neo4j.driver.Transaction;
import org.neo4j.driver.exceptions.ServiceUnavailableException;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.monitoring.Monitors;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.SkipThreadLeakageGuard;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

import static com.neo4j.bolt.BoltDriverHelper.graphDatabaseDriver;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.io.IOUtils.closeAllSilently;
import static org.neo4j.test.PortUtils.getBoltPort;

@TestDirectoryExtension
@SkipThreadLeakageGuard
class BoltFailuresIT
{
    @Inject
    private TestDirectory directory;

    private GraphDatabaseService db;
    private Driver driver;
    private DatabaseManagementService managementService;

    @AfterEach
    void shutdownDb()
    {
        closeAllSilently( driver );
        if ( managementService != null )
        {
            managementService.shutdown();
        }
    }

    @Test
    void throwsWhenMonitoredWorkerCreationFails()
    {
        ThrowingSessionMonitor sessionMonitor = new ThrowingSessionMonitor();
        sessionMonitor.throwInConnectionOpened();
        Monitors monitors = newMonitorsSpy( sessionMonitor );

        db = startDbWithBolt( new TestDatabaseManagementServiceBuilder( directory.homeDir() ).setMonitors( monitors ) );
        assertThrows( ServiceUnavailableException.class, () -> driver = createDriver( getBoltPort( db ) ) );
    }

    @Test
    void throwsWhenInitMessageReceiveFails()
    {
        throwsWhenInitMessageFails( ThrowingSessionMonitor::throwInMessageReceived, false );
    }

    @Test
    void throwsWhenInitMessageProcessingFailsToStart()
    {
        throwsWhenInitMessageFails( ThrowingSessionMonitor::throwInMessageProcessingStarted, false );
    }

    @Test
    void throwsWhenInitMessageProcessingFailsToComplete()
    {
        throwsWhenInitMessageFails( ThrowingSessionMonitor::throwInMessageProcessingCompleted, true );
    }

    @Test
    void throwsWhenRunMessageReceiveFails()
    {
        throwsWhenRunMessageFails( ThrowingSessionMonitor::throwInMessageReceived );
    }

    @Test
    void throwsWhenRunMessageProcessingFailsToStart()
    {
        throwsWhenRunMessageFails( ThrowingSessionMonitor::throwInMessageProcessingStarted );
    }

    @Test
    void throwsWhenRunMessageProcessingFailsToComplete()
    {
        throwsWhenRunMessageFails( ThrowingSessionMonitor::throwInMessageProcessingCompleted );
    }

    private void throwsWhenInitMessageFails( Consumer<ThrowingSessionMonitor> monitorSetup,
            boolean shouldBeAbleToBeginTransaction )
    {
        ThrowingSessionMonitor sessionMonitor = new ThrowingSessionMonitor();
        monitorSetup.accept( sessionMonitor );
        Monitors monitors = newMonitorsSpy( sessionMonitor );

        db = startTestDb( monitors );

        try
        {
            driver = graphDatabaseDriver( "bolt://localhost:" + getBoltPort( db ) );
            driver.verifyConnectivity();
            if ( shouldBeAbleToBeginTransaction )
            {
                try ( Session session = driver.session();
                      Transaction tx = session.beginTransaction() )
                {
                    tx.run( "CREATE ()" ).consume();
                }
            }
            else
            {
                fail( "Exception expected" );
            }
        }
        catch ( Exception e )
        {
            assertThat( e ).isInstanceOf( ServiceUnavailableException.class );
        }
    }

    private void throwsWhenRunMessageFails( Consumer<ThrowingSessionMonitor> monitorSetup )
    {
        ThrowingSessionMonitor sessionMonitor = new ThrowingSessionMonitor();
        Monitors monitors = newMonitorsSpy( sessionMonitor );

        db = startTestDb( monitors );
        driver = createDriver( getBoltPort( db ) );

        // open a session and start a transaction, this will force driver to obtain
        // a network connection and bind it to the transaction
        Session session = driver.session();
        Transaction tx = session.beginTransaction();

        // at this point driver holds a valid initialize connection
        // setup monitor to throw before running the query to make processing of the RUN message fail
        monitorSetup.accept( sessionMonitor );
        tx.run( "CREATE ()" );
        try
        {
            tx.close();
            session.close();
            fail( "Exception expected" );
        }
        catch ( Exception e )
        {
            assertThat( e ).isInstanceOf( ServiceUnavailableException.class );
        }
    }

    private GraphDatabaseService startTestDb( Monitors monitors )
    {
        return startDbWithBolt( newDbFactory( directory.homeDir() ).setMonitors( monitors ) );
    }

    private GraphDatabaseService startDbWithBolt( DatabaseManagementServiceBuilder dbFactory )
    {
        managementService = dbFactory
                .setConfig( BoltConnector.enabled, true )
                .setConfig( BoltConnector.listen_address, new SocketAddress( "localhost", 0 ) )
                .setConfig( GraphDatabaseSettings.auth_enabled, false )
                .setConfig( OnlineBackupSettings.online_backup_enabled, false ).build();
        return managementService.database( DEFAULT_DATABASE_NAME );
    }

    private static TestEnterpriseDatabaseManagementServiceBuilder newDbFactory( File databaseRootDir )
    {
        return new TestEnterpriseDatabaseManagementServiceBuilder( databaseRootDir );
    }

    private static Driver createDriver( int port )
    {
        Driver driver = graphDatabaseDriver( "bolt://localhost:" + port );
        driver.verifyConnectivity();
        return driver;
    }

    private static Monitors newMonitorsSpy( ThrowingSessionMonitor sessionMonitor )
    {
        Monitors monitors = spy( new Monitors() );
        // it is not allowed to throw exceptions from monitors
        // make the given sessionMonitor be returned as is, without any proxying
        when( monitors.newMonitor( BoltConnectionMetricsMonitor.class ) ).thenReturn( sessionMonitor );
        return monitors;
    }

    private static class ThrowingSessionMonitor implements BoltConnectionMetricsMonitor
    {
        volatile boolean throwInConnectionOpened;
        volatile boolean throwInMessageReceived;
        volatile boolean throwInMessageProcessingStarted;
        volatile boolean throwInMessageProcessingCompleted;

        @Override
        public void connectionOpened()
        {
            throwIfNeeded( throwInConnectionOpened );
        }

        @Override
        public void connectionActivated()
        {

        }

        @Override
        public void connectionWaiting()
        {

        }

        @Override
        public void messageReceived()
        {
            throwIfNeeded( throwInMessageReceived );
        }

        @Override
        public void messageProcessingStarted( long queueTime )
        {
            throwIfNeeded( throwInMessageProcessingStarted );
        }

        @Override
        public void messageProcessingCompleted( long processingTime )
        {
            throwIfNeeded( throwInMessageProcessingCompleted );
        }

        @Override
        public void messageProcessingFailed()
        {

        }

        @Override
        public void connectionClosed()
        {

        }

        void throwInConnectionOpened()
        {
            throwInConnectionOpened = true;
        }

        void throwInMessageReceived()
        {
            throwInMessageReceived = true;
        }

        void throwInMessageProcessingStarted()
        {
            throwInMessageProcessingStarted = true;
        }

        void throwInMessageProcessingCompleted()
        {
            throwInMessageProcessingCompleted = true;
        }

        void throwIfNeeded( boolean shouldThrow )
        {
            if ( shouldThrow )
            {
                throw new RuntimeException();
            }
        }
    }
}
