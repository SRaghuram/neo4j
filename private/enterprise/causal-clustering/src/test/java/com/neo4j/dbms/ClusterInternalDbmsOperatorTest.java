/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.dbms;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Collections;

import org.neo4j.kernel.database.DatabaseIdRepository;
import org.neo4j.kernel.database.TestDatabaseIdRepository;

import static com.neo4j.dbms.OperatorState.STORE_COPYING;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class ClusterInternalDbmsOperatorTest
{
    private OperatorConnector connector = mock( OperatorConnector.class );
    private ClusterInternalDbmsOperator operator = new ClusterInternalDbmsOperator();
    private DatabaseIdRepository databaseIdRepository = new TestDatabaseIdRepository();

    @BeforeEach
    void setup()
    {
        when( connector.trigger() ).thenReturn( Reconciliation.EMPTY );
        operator.connect( connector );
    }

    @Test
    void shouldNotDesireAnythingInitially()
    {
        assertEquals( operator.getDesired(), Collections.emptyMap() );
    }

    @Test
    void shouldTriggerOnStartAndStop()
    {
        // given
        var someDb = databaseIdRepository.get( "some" );

        // when
        var stoppedDatabase = operator.stopForStoreCopy( someDb );

        // then
        verify( connector, times( 1 ) ).trigger();

        // when
        stoppedDatabase.restart();

        // then
        verify( connector, times( 2 ) ).trigger();
    }

    @Test
    void shouldReturnIndependentContexts()
    {
        // given
        var databaseA = databaseIdRepository.get( "A" );
        var databaseB = databaseIdRepository.get( "B" );

        // when
        var stopA1 = operator.stopForStoreCopy( databaseA );

        // then
        assertEquals( STORE_COPYING, operator.getDesired().get( databaseA ) );
        assertNull( operator.getDesired().get( databaseB ) );

        // when
        var stopA2 = operator.stopForStoreCopy( databaseA );

        // then
        assertEquals( STORE_COPYING, operator.getDesired().get( databaseA ) );
        assertNull( operator.getDesired().get( databaseB ) );

        // when
        var stopB1 = operator.stopForStoreCopy( databaseB );

        // then
        assertEquals( STORE_COPYING, operator.getDesired().get( databaseA ) );
        assertEquals( STORE_COPYING, operator.getDesired().get( databaseB ) );

        // when
        var stopB2 = operator.stopForStoreCopy( databaseB );

        // then
        assertEquals( STORE_COPYING, operator.getDesired().get( databaseA ) );
        assertEquals( STORE_COPYING, operator.getDesired().get( databaseB ) );

        // when
        stopA1.restart();

        // then
        assertEquals( STORE_COPYING, operator.getDesired().get( databaseA ) );
        assertEquals( STORE_COPYING, operator.getDesired().get( databaseB ) );

        // when
        stopA2.restart();

        // then
        assertNull( operator.getDesired().get( databaseA ) );
        assertEquals( STORE_COPYING, operator.getDesired().get( databaseB ) );

        // when
        stopB1.restart();

        // then
        assertNull( operator.getDesired().get( databaseA ) );
        assertEquals( STORE_COPYING, operator.getDesired().get( databaseB ) );

        // when
        stopB2.restart();

        // then
        assertEquals( operator.getDesired(), Collections.emptyMap() );
    }

    @Test
    void shouldComplainWhenStartingTwice()
    {
        // given
        var someDb = databaseIdRepository.get( "some" );
        var stoppedDatabase = operator.stopForStoreCopy( someDb );
        stoppedDatabase.restart();

        // when/then
        assertThrows( IllegalStateException.class, stoppedDatabase::restart );
    }

    @Test
    void shouldNotDesireStoreCopyingWhileBootstrapping()
    {
        // given
        var someDb = databaseIdRepository.get( "some" );
        var bootstrapping = operator.bootstrap( someDb );

        // when
        var storeCopying = operator.stopForStoreCopy( someDb );

        // then
        assertNotEquals( operator.getDesired().get( someDb ), STORE_COPYING );

        // when
        bootstrapping.bootstrapped();

        // then
        assertEquals( operator.getDesired().get( someDb ), STORE_COPYING );

        // when
        storeCopying.restart();

        // then
        assertNull( operator.getDesired().get( someDb ) );
    }
}
