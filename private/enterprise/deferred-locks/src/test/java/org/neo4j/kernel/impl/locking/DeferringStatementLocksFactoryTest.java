/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.kernel.impl.locking;

import org.junit.jupiter.api.Test;

import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.configuration.Settings;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.neo4j.kernel.impl.locking.DeferringStatementLocksFactory.deferred_locks_enabled;

class DeferringStatementLocksFactoryTest
{
    @Test
    void initializeThrowsForNullLocks()
    {
        DeferringStatementLocksFactory factory = new DeferringStatementLocksFactory();
        assertThrows( NullPointerException.class, () -> factory.initialize( null, Config.defaults() ) );
    }

    @Test
    void initializeThrowsForNullConfig()
    {
        DeferringStatementLocksFactory factory = new DeferringStatementLocksFactory();
        assertThrows( NullPointerException.class, () -> factory.initialize( mock( Locks.class ), null ) );
    }

    @Test
    void newInstanceThrowsWhenNotInitialized()
    {
        DeferringStatementLocksFactory factory = new DeferringStatementLocksFactory();
        assertThrows( IllegalStateException.class, factory::newInstance );
    }

    @Test
    void newInstanceCreatesSimpleLocksWhenConfigNotSet()
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
    void newInstanceCreatesDeferredLocksWhenConfigSet()
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
