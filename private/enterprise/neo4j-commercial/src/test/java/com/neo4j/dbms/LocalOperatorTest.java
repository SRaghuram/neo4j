/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.dbms;

import org.junit.jupiter.api.Test;

import org.neo4j.kernel.database.DatabaseId;

import static com.neo4j.dbms.OperatorState.DROPPED;
import static com.neo4j.dbms.OperatorState.STARTED;
import static com.neo4j.dbms.OperatorState.STOPPED;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

class LocalOperatorTest
{
    private OperatorConnector connector = mock( OperatorConnector.class );
    private LocalOperator operator = new LocalOperator( connector );

    private String databaseName = "my.db";

    @Test
    void shouldBeAbleToCreateDatabase()
    {
        operator.createDatabase( databaseName );
        verify( connector, times( 1 ) ).trigger();

        assertEquals( STOPPED, operator.getDesired().get( new DatabaseId( databaseName ) ) );
    }

    @Test
    void shouldBeAbleToDropDatabase()
    {
        operator.dropDatabase( databaseName );
        verify( connector, times( 1 ) ).trigger();

        assertEquals( DROPPED, operator.getDesired().get( new DatabaseId( databaseName ) ) );
    }

    @Test
    void shouldBeAbleToStartDatabase()
    {
        operator.startDatabase( databaseName );
        verify( connector, times( 1 ) ).trigger();

        assertEquals( STARTED, operator.getDesired().get( new DatabaseId( databaseName ) ) );
    }

    @Test
    void shouldBeAbleToStopDatabase()
    {
        operator.stopDatabase( databaseName );
        verify( connector, times( 1 ) ).trigger();

        assertEquals( STOPPED, operator.getDesired().get( new DatabaseId( databaseName ) ) );
    }
}
