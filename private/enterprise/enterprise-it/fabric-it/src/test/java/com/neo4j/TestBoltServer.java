/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;

import org.neo4j.bolt.BoltServer;
import org.neo4j.bolt.dbapi.BoltGraphDatabaseManagementServiceSPI;
import org.neo4j.bolt.runtime.BoltConnectionMetricsMonitor;
import org.neo4j.collection.Dependencies;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.connectors.ConnectorPortRegister;
import org.neo4j.internal.kernel.api.security.AuthSubject;
import org.neo4j.internal.kernel.api.security.LoginContext;
import org.neo4j.kernel.api.net.NetworkConnectionTracker;
import org.neo4j.kernel.api.security.AuthManager;
import org.neo4j.kernel.database.DatabaseIdRepository;
import org.neo4j.kernel.impl.scheduler.JobSchedulerFactory;
import org.neo4j.logging.internal.NullLogService;
import org.neo4j.monitoring.Monitors;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.ssl.config.SslPolicyLoader;
import org.neo4j.time.SystemNanoClock;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.neo4j.internal.kernel.api.security.AuthenticationResult.SUCCESS;
import static org.neo4j.kernel.database.DatabaseIdRepository.NAMED_SYSTEM_DATABASE_ID;

public class TestBoltServer
{
    private final AtomicLong connectionIdCounter = new AtomicLong();
    private final JobScheduler jobScheduler = JobSchedulerFactory.createScheduler();
    private final Config config;
    private final Dependencies dependencies = new Dependencies();
    private BoltServer boltServer;

    BoltGraphDatabaseManagementServiceSPI boltGraphDatabaseManagementService = mock( BoltGraphDatabaseManagementServiceSPI.class );

    public TestBoltServer( int port )
    {
        var configProperties = Map.of(
                "fabric.driver.connection.encrypted", "false",
                "dbms.connector.bolt.listen_address", "0.0.0.0:" + port,
                "dbms.connector.bolt.enabled", "true"
        );

        config = Config.newBuilder()
                .setRaw( configProperties )
                .build();

        var sslPolicyLoader = mock( SslPolicyLoader.class);
        dependencies.satisfyDependency( sslPolicyLoader );
    }

    public void start()
    {
        var authManager = mock( AuthManager.class );
        try
        {
            var loginContext = mock( LoginContext.class );
            var authSubject = mock( AuthSubject.class );
            when(loginContext.subject()).thenReturn( authSubject );
            when( authSubject.getAuthenticationResult() ).thenReturn( SUCCESS );
            when( authManager.login( any() ) ).thenReturn( loginContext );
        }
        catch ( Exception e )
        {
            throw new IllegalStateException( e );
        }
        var networkConnectionTracker = mock( NetworkConnectionTracker.class );
        when( networkConnectionTracker.newConnectionId( any() ) ).thenAnswer( invocationOnMock -> "c-" + connectionIdCounter.incrementAndGet() );

        var metricsMonitor = mock(BoltConnectionMetricsMonitor.class);
        var monitors = mock(Monitors.class);
        when( monitors.newMonitor( any() ) ).thenReturn( metricsMonitor );

        var dbIdRepository = mock( DatabaseIdRepository.class );
        when( dbIdRepository.getById( any() ) ).thenReturn( Optional.of( NAMED_SYSTEM_DATABASE_ID ) );

        boltServer = new BoltServer( boltGraphDatabaseManagementService,
                jobScheduler,
                mock( ConnectorPortRegister.class ),
                networkConnectionTracker,
                dbIdRepository,
                config,
                mock( SystemNanoClock.class ),
                monitors,
                NullLogService.getInstance(),
                dependencies,
                authManager
        );

        try
        {
            boltServer.init();
            boltServer.start();
        }
        catch ( Exception e )
        {
            throw new IllegalStateException( "Failed to start test Bolt server", e );
        }
    }

    public void stop()
    {
        try
        {
            jobScheduler.shutdown();
            boltServer.shutdown();
        }
        catch ( Exception e )
        {
            throw new IllegalStateException( "Failed to stop test Bolt server", e );
        }
    }
}
