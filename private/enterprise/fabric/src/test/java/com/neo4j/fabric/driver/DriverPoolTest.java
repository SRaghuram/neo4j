/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.fabric.driver;

import com.neo4j.fabric.auth.CredentialsProvider;
import com.neo4j.fabric.config.FabricConfig;
import com.neo4j.fabric.config.FabricSettings;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import java.time.Clock;
import java.time.Duration;

import org.neo4j.configuration.Config;
import org.neo4j.configuration.helpers.NormalizedGraphName;
import org.neo4j.driver.AuthToken;
import org.neo4j.driver.AuthTokens;
import org.neo4j.harness.Neo4j;
import org.neo4j.harness.Neo4jBuilders;
import org.neo4j.internal.kernel.api.security.AuthSubject;
import org.neo4j.logging.Level;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.ssl.config.SslPolicyLoader;

import static java.time.Duration.ofMinutes;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class DriverPoolTest
{
    private static Neo4j shard0;
    private static Neo4j shard1;

    private final JobScheduler jobScheduler = mock( JobScheduler.class );
    private final FabricConfig fabricConfig = mock( FabricConfig.class );
    private final Config config = mock( Config.class );

    private final FabricConfig.Graph s1 =
            new FabricConfig.Graph( 1, FabricConfig.RemoteUri.create( shard0.boltURI() ), "db1", new NormalizedGraphName( "shard-0" ), null );
    private final FabricConfig.Graph s2 =
            new FabricConfig.Graph( 2, FabricConfig.RemoteUri.create( shard1.boltURI() ), "db1", new NormalizedGraphName( "shard-1" ), null );

    private final CredentialsProvider credentialsProvider = Mockito.mock( CredentialsProvider.class );
    private final AuthSubject as1 = mock( AuthSubject.class );
    private final AuthSubject as2 = mock( AuthSubject.class );

    private final AuthToken at1 = AuthTokens.basic( "u1", "p" );
    private final AuthToken at2 = AuthTokens.basic( "u2", "p" );

    private final Duration idleTimeout = ofMinutes( 1 );

    private DriverPool driverPool;

    @BeforeAll
    static void beforeAll()
    {
        shard0 = Neo4jBuilders.newInProcessBuilder().build();
        shard1 = Neo4jBuilders.newInProcessBuilder().build();
    }

    @AfterAll
    static void afterAll()
    {
        shard0.close();
        shard1.close();
    }

    @BeforeEach
    void beforeEach()
    {
        var driverConfig = mock( FabricConfig.DriverConfig.class );
        when( driverConfig.getMaxConnectionPoolSize() ).thenReturn( 10 );
        when( driverConfig.getLoggingLevel() ).thenReturn( Level.INFO );
        when( driverConfig.getDriverApi() ).thenReturn( FabricSettings.DriverApi.RX );

        var remoteGraphDriver = new FabricConfig.GlobalDriverConfig( idleTimeout, ofMinutes( 1 ), 1, driverConfig );
        when( fabricConfig.getGlobalDriverConfig() ).thenReturn( remoteGraphDriver );
        when( credentialsProvider.credentialsFor( as1 ) ).thenReturn( at1 );
        when( credentialsProvider.credentialsFor( as2 ) ).thenReturn( at2 );

        driverPool = new DriverPool( jobScheduler, fabricConfig, config, Clock.systemUTC(), credentialsProvider, mock(SslPolicyLoader.class ) );
    }

    @Test
    void testCreate()
    {
        driverPool.start();

        PooledDriver d1 = driverPool.getDriver( s1, as1 );
        PooledDriver d2 = driverPool.getDriver( s1, as1 );
        PooledDriver d3 = driverPool.getDriver( s2, as1 );
        PooledDriver d4 = driverPool.getDriver( s2, as2 );

        assertSame( d1, d2 );
        assertNotSame( d2, d3 );
        assertNotSame( d3, d4 );

        driverPool.stop();
    }

    @Test
    void lifecycleTest()
    {
        driverPool.start();

        ArgumentCaptor<Runnable> argumentCaptor = ArgumentCaptor.forClass( Runnable.class );
        verify( jobScheduler ).schedule( any(), argumentCaptor.capture(), anyLong(), any() );

        Runnable idleDriverCheck = argumentCaptor.getValue();

        PooledDriver d1 = driverPool.getDriver( s1, as1 );
        driverPool.getDriver( s1, as1 );
        driverPool.getDriver( s1, as1 );
        assertEquals( 3, d1.getReferenceCounter().get() );

        d1.release();
        assertEquals( 2, d1.getReferenceCounter().get() );

        d1.release();
        d1.release();

        d1.setLastUsedTimestamp( Clock.systemUTC().instant().minus( idleTimeout.plusMinutes( 1 ) ) );
        idleDriverCheck.run();

        PooledDriver d2 = driverPool.getDriver( s1, as1 );

        assertNotSame( d1, d2 );
        assertEquals( 0, d1.getReferenceCounter().get() );
        assertEquals( 1, d2.getReferenceCounter().get() );

        driverPool.stop();
    }
}
