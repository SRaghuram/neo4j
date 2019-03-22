/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.dbms.database;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.InOrder;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.neo4j.dbms.database.DatabaseContext;
import org.neo4j.kernel.database.DatabaseId;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.inOrder;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.configuration.GraphDatabaseSettings.SYSTEM_DATABASE_NAME;

class MultiDatabaseManagerTest
{
    private static String CUSTOM_DATABASE_NAME = "custom";

    private MultiDatabaseManager<DatabaseContext> databaseManager;
    private DatabaseContext sys;
    private DatabaseContext neo;
    private DatabaseContext custom;

    private void initDatabaseManager( Comparator<DatabaseId> databasesOrdering ) throws Exception
    {
        databaseManager = new StubMultiDatabaseManager( databasesOrdering );
        sys = databaseManager.createDatabase( new DatabaseId( SYSTEM_DATABASE_NAME ) );
        neo = databaseManager.createDatabase( new DatabaseId( DEFAULT_DATABASE_NAME ) );
        custom = databaseManager.createDatabase( new DatabaseId( CUSTOM_DATABASE_NAME ) );
        databaseManager.start();
    }

    private static Stream<Object[]> databaseOrderings()
    {
        return Stream.of(
                new Object[] { "Natural order", DatabaseId.comparator },
                new Object[] { "Reverse order", DatabaseId.comparator.reversed() }
        );
    }

    @Test
    void startsSystemDatabaseFirst() throws Exception
    {
        // given
        initDatabaseManager( null );

        // then
        InOrder inOrder = inOrder( sys.database(), custom.database(), neo.database() );

        inOrder.verify( sys.database() ).start();
        inOrder.verify( custom.database() ).start();
        inOrder.verify( neo.database() ).start();
    }

    @Test
    void stopsSystemDatabaseLast() throws Exception
    {
        // given
        initDatabaseManager( null );

        // when
        databaseManager.stop();

        // then
        InOrder inOrder = inOrder( sys.database(), custom.database(), neo.database() );

        inOrder.verify( neo.database() ).stop();
        inOrder.verify( custom.database() ).stop();
        inOrder.verify( sys.database() ).stop();
    }

    @ParameterizedTest( name = "{0}" )
    @MethodSource( "databaseOrderings" )
    void ignoresCustomComparatorWhenOperatingOnSystemDatabase( String ignored,  Comparator<DatabaseId> databasesOrdering ) throws Exception
    {
        // given
        initDatabaseManager( databasesOrdering );

        // then
        InOrder inOrder = inOrder( sys.database(), custom.database(), neo.database() );

        inOrder.verify( sys.database() ).start();
    }

    @ParameterizedTest( name = "{0}" )
    @MethodSource( "databaseOrderings" )
    void returnsDatabasesInCorrectOrder( String ignored, Comparator<DatabaseId> databasesOrdering ) throws Exception
    {
        // given
        initDatabaseManager( databasesOrdering );
        List<DatabaseId> expectedDatabaseIds = Stream.of( DEFAULT_DATABASE_NAME, CUSTOM_DATABASE_NAME )
                .map( DatabaseId::new )
                .sorted( databasesOrdering )
                .collect( Collectors.toList() );
        expectedDatabaseIds.add( 0, new DatabaseId( SYSTEM_DATABASE_NAME ) );

        // when
        ArrayList<DatabaseId> actualDatabaseIds = new ArrayList<>( databaseManager.registeredDatabases().keySet() );

        // then
        assertEquals( expectedDatabaseIds, actualDatabaseIds );
    }

}
