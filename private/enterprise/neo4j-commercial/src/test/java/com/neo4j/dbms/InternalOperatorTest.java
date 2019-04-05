/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.dbms;

import org.junit.jupiter.api.Test;

import java.util.Collections;

import org.neo4j.kernel.database.DatabaseId;

import static com.neo4j.dbms.OperatorState.STOPPED;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

class InternalOperatorTest
{
    private OperatorConnector connector = mock( OperatorConnector.class );
    private InternalOperator operator = new InternalOperator( connector );

    @Test
    void shouldNotDesireAnythingInitially()
    {
        assertEquals( operator.getDesired(), Collections.emptyMap() );
    }

    @Test
    void shouldTriggerOnStartAndStop()
    {
        // given
        DatabaseId someDb = new DatabaseId( "some.db" );

        // when
        Startable stoppedDatabase = operator.stopDatabase( someDb );

        // then
        verify( connector, times( 1 ) ).trigger();

        // when
        stoppedDatabase.start();

        // then
        verify( connector, times( 2 ) ).trigger();
    }

    @Test
    void shouldReturnIndependentContexts()
    {
        // given
        DatabaseId databaseA = new DatabaseId( "A" );
        DatabaseId databaseB = new DatabaseId( "B" );

        // when
        Startable stopA1 = operator.stopDatabase( databaseA );

        // then
        assertEquals( STOPPED, operator.getDesired().get( databaseA ) );
        assertNull( operator.getDesired().get( databaseB ) );

        // when
        Startable stopA2 = operator.stopDatabase( databaseA );

        // then
        assertEquals( STOPPED, operator.getDesired().get( databaseA ) );
        assertNull( operator.getDesired().get( databaseB ) );

        // when
        Startable stopB1 = operator.stopDatabase( databaseB );

        // then
        assertEquals( STOPPED, operator.getDesired().get( databaseA ) );
        assertEquals( STOPPED, operator.getDesired().get( databaseB ) );

        // when
        Startable stopB2 = operator.stopDatabase( databaseB );

        // then
        assertEquals( STOPPED, operator.getDesired().get( databaseA ) );
        assertEquals( STOPPED, operator.getDesired().get( databaseB ) );

        // when
        stopA1.start();

        // then
        assertEquals( STOPPED, operator.getDesired().get( databaseA ) );
        assertEquals( STOPPED, operator.getDesired().get( databaseB ) );

        // when
        stopA2.start();

        // then
        assertNull( operator.getDesired().get( databaseA ) );
        assertEquals( STOPPED, operator.getDesired().get( databaseB ) );

        // when
        stopB1.start();

        // then
        assertNull( operator.getDesired().get( databaseA ) );
        assertEquals( STOPPED, operator.getDesired().get( databaseB ) );

        // when
        stopB2.start();

        // then
        assertEquals( operator.getDesired(), Collections.emptyMap() );
    }

    @Test
    void shouldComplainWhenStartingTwice()
    {
        // given
        DatabaseId someDb = new DatabaseId( "some.db" );
        Startable stoppedDatabase = operator.stopDatabase( someDb );
        stoppedDatabase.start();

        // when/then
        assertThrows( IllegalStateException.class, stoppedDatabase::start );
    }
}
