/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.dbms.database;

import org.junit.jupiter.api.Test;
import org.mockito.InOrder;

import java.util.List;
import java.util.stream.Collectors;

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

    private void initDatabaseManager() throws Exception
    {
        databaseManager = new StubMultiDatabaseManager();
        sys = databaseManager.createDatabase( new DatabaseId( SYSTEM_DATABASE_NAME ) );
        neo = databaseManager.createDatabase( new DatabaseId( DEFAULT_DATABASE_NAME ) );
        custom = databaseManager.createDatabase( new DatabaseId( CUSTOM_DATABASE_NAME ) );
        databaseManager.start();
    }

    @Test
    void startsSystemDatabaseFirst() throws Exception
    {
        // given
        initDatabaseManager();

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
        initDatabaseManager();

        // when
        databaseManager.stop();

        // then
        InOrder inOrder = inOrder( sys.database(), custom.database(), neo.database() );

        inOrder.verify( neo.database() ).stop();
        inOrder.verify( custom.database() ).stop();
        inOrder.verify( sys.database() ).stop();
    }

    @Test
    void ignoresCustomComparatorWhenOperatingOnSystemDatabase() throws Exception
    {
        // given
        initDatabaseManager();

        // then
        InOrder inOrder = inOrder( sys.database(), custom.database(), neo.database() );

        inOrder.verify( sys.database() ).start();
    }

    @Test
    void returnsDatabasesInCorrectOrder() throws Exception
    {
        // given
        initDatabaseManager();
        List<String> expectedNames = List.of( SYSTEM_DATABASE_NAME, CUSTOM_DATABASE_NAME, DEFAULT_DATABASE_NAME );

        // when
        List<String> actualNames = databaseManager.registeredDatabases().keySet().stream().map( DatabaseId::name ).collect( Collectors.toList() );

        // then
        assertEquals( expectedNames, actualNames );
    }

}
