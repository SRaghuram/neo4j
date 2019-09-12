/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.fabric.driver;

import com.neo4j.fabric.config.FabricConfig;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.time.Clock;
import java.time.Duration;

import org.neo4j.driver.AuthToken;
import org.neo4j.driver.AuthTokens;
import org.neo4j.harness.internal.InProcessNeo4j;
import org.neo4j.harness.internal.TestNeo4jBuilders;
import org.neo4j.scheduler.JobScheduler;

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
    private static InProcessNeo4j shard0;
    private static InProcessNeo4j shard1;

    private final JobScheduler jobScheduler = mock( JobScheduler.class );
    private final FabricConfig fabricConfig = mock( FabricConfig.class );

    private final FabricConfig.Graph s1 = new FabricConfig.Graph( 1, shard0.boltURI(), "db1", "shard-0" );
    private final FabricConfig.Graph s2 = new FabricConfig.Graph( 2, shard1.boltURI(), "db1", "shard-1" );

    private final AuthToken at1 = AuthTokens.basic( "u1", "p" );
    private final AuthToken at2 = AuthTokens.basic( "u2", "p" );

    private final Duration idleTimeout = Duration.ofMinutes( 1 );

    private DriverPool driverPool = new DriverPool( jobScheduler, fabricConfig, Clock.systemUTC() );

    @BeforeAll
    static void beforeAll()
    {
        shard0 = TestNeo4jBuilders.newInProcessBuilder().build();
        shard1 = TestNeo4jBuilders.newInProcessBuilder().build();
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
        driverPool = new DriverPool( jobScheduler, fabricConfig, Clock.systemUTC() );
        FabricConfig.RemoteGraphDriver remoteGraphDriver = new FabricConfig.RemoteGraphDriver( idleTimeout, Duration.ofMinutes( 1 ) );
        when( fabricConfig.getRemoteGraphDriver() ).thenReturn( remoteGraphDriver );
    }

    @Test
    void testCreate()
    {
        driverPool.start();

        PooledDriver d1 = driverPool.getDriver( s1, at1 );
        PooledDriver d2 = driverPool.getDriver( s1, at1 );
        PooledDriver d3 = driverPool.getDriver( s2, at1 );
        PooledDriver d4 = driverPool.getDriver( s2, at2 );

        assertSame( d1.getDriver(), d2.getDriver() );
        assertNotSame( d2.getDriver(), d3.getDriver() );
        assertNotSame( d3.getDriver(), d4.getDriver() );

        driverPool.stop();
    }

    @Test
    void lifecycleTest()
    {
        driverPool.start();

        ArgumentCaptor<Runnable> argumentCaptor = ArgumentCaptor.forClass( Runnable.class );
        verify( jobScheduler).schedule( any(), argumentCaptor.capture(), anyLong(), any() );

        Runnable idleDriverCheck = argumentCaptor.getValue();

        PooledDriver d1 = driverPool.getDriver( s1, at1 );
        driverPool.getDriver( s1, at1 );
        driverPool.getDriver( s1, at1 );
        assertEquals( 3, d1.getReferenceCounter().get() );

        d1.release();
        assertEquals( 2, d1.getReferenceCounter().get() );

        d1.release();
        d1.release();

        d1.setLastUsedTimestamp( Clock.systemUTC().instant().minus( idleTimeout.plusMinutes( 1 ) ) );
        idleDriverCheck.run();

        PooledDriver d2 = driverPool.getDriver( s1, at1 );

        assertNotSame( d1, d2 );
        assertEquals( 0, d1.getReferenceCounter().get() );
        assertEquals( 1, d2.getReferenceCounter().get() );

        driverPool.stop();
    }
}
