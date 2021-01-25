/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.kernel.impl.locking;

import org.junit.jupiter.api.Test;

import org.neo4j.configuration.Config;
import org.neo4j.kernel.impl.locking.Locks;
import org.neo4j.kernel.impl.locking.SimpleStatementLocks;
import org.neo4j.kernel.impl.locking.StatementLocks;

import static com.neo4j.kernel.impl.locking.DeferringStatementLocksFactory.Configuration.deferred_locks_enabled;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

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

        Config config = Config.defaults( deferred_locks_enabled, false );

        DeferringStatementLocksFactory factory = new DeferringStatementLocksFactory();
        factory.initialize( locks, config );

        StatementLocks statementLocks = factory.newInstance();

        assertThat( statementLocks ).isInstanceOf( SimpleStatementLocks.class );
        assertSame( client, statementLocks.optimistic() );
        assertSame( client, statementLocks.pessimistic() );
    }

    @Test
    void newInstanceCreatesDeferredLocksWhenConfigSet()
    {
        Locks locks = mock( Locks.class );
        Locks.Client client = mock( Locks.Client.class );
        when( locks.newClient() ).thenReturn( client );

        Config config = Config.defaults( deferred_locks_enabled, true );

        DeferringStatementLocksFactory factory = new DeferringStatementLocksFactory();
        factory.initialize( locks, config );

        StatementLocks statementLocks = factory.newInstance();

        assertThat( statementLocks ).isInstanceOf( DeferringStatementLocks.class );
        assertThat( statementLocks.optimistic() ).isInstanceOf( DeferringLockClient.class );
        assertSame( client, statementLocks.pessimistic() );
    }
}
