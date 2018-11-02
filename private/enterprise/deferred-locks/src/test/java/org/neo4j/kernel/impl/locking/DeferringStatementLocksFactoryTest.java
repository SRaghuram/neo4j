/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.kernel.impl.locking;

import org.junit.Test;

import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.configuration.Settings;

import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.neo4j.kernel.impl.locking.DeferringStatementLocksFactory.deferred_locks_enabled;

public class DeferringStatementLocksFactoryTest
{
    @Test
    public void initializeThrowsForNullLocks()
    {
        DeferringStatementLocksFactory factory = new DeferringStatementLocksFactory();
        try
        {
            factory.initialize( null, Config.defaults() );
            fail( "Exception expected" );
        }
        catch ( Exception e )
        {
            assertThat( e, instanceOf( NullPointerException.class ) );
        }
    }

    @Test
    public void initializeThrowsForNullConfig()
    {
        DeferringStatementLocksFactory factory = new DeferringStatementLocksFactory();
        try
        {
            factory.initialize( mock( Locks.class ), null );
            fail( "Exception expected" );
        }
        catch ( Exception e )
        {
            assertThat( e, instanceOf( NullPointerException.class ) );
        }
    }

    @Test
    public void newInstanceThrowsWhenNotInitialized()
    {
        DeferringStatementLocksFactory factory = new DeferringStatementLocksFactory();
        try
        {
            factory.newInstance();
            fail( "Exception expected" );
        }
        catch ( Exception e )
        {
            assertThat( e, instanceOf( IllegalStateException.class ) );
        }
    }

    @Test
    public void newInstanceCreatesSimpleLocksWhenConfigNotSet()
    {
        Locks locks = mock( Locks.class );
        Locks.Client client = mock( Locks.Client.class );
        when( locks.newClient() ).thenReturn( client );

        Config config = Config.defaults( deferred_locks_enabled, Settings.FALSE );

        DeferringStatementLocksFactory factory = new DeferringStatementLocksFactory();
        factory.initialize( locks, config );

        StatementLocks statementLocks = factory.newInstance();

        assertThat( statementLocks, instanceOf( SimpleStatementLocks.class ) );
        assertSame( client, statementLocks.optimistic() );
        assertSame( client, statementLocks.pessimistic() );
    }

    @Test
    public void newInstanceCreatesDeferredLocksWhenConfigSet()
    {
        Locks locks = mock( Locks.class );
        Locks.Client client = mock( Locks.Client.class );
        when( locks.newClient() ).thenReturn( client );

        Config config = Config.defaults( deferred_locks_enabled, Settings.TRUE );

        DeferringStatementLocksFactory factory = new DeferringStatementLocksFactory();
        factory.initialize( locks, config );

        StatementLocks statementLocks = factory.newInstance();

        assertThat( statementLocks, instanceOf( DeferringStatementLocks.class ) );
        assertThat( statementLocks.optimistic(), instanceOf( DeferringLockClient.class ) );
        assertSame( client, statementLocks.pessimistic() );
    }
}
