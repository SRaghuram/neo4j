/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.dbms.database;

import org.junit.jupiter.api.Test;
import org.mockito.InOrder;

import java.util.Arrays;
import java.util.List;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import org.neo4j.dbms.database.DatabaseContext;
import org.neo4j.kernel.database.NamedDatabaseId;
import org.neo4j.kernel.database.TestDatabaseIdRepository;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.configuration.GraphDatabaseSettings.SYSTEM_DATABASE_NAME;
import static org.neo4j.kernel.database.DatabaseIdRepository.NAMED_SYSTEM_DATABASE_ID;

class MultiDatabaseManagerTest
{
    private static final String CUSTOM_DATABASE_NAME = "custom";
    private static final TestDatabaseIdRepository databaseIdRepository = new TestDatabaseIdRepository();
    private static final NamedDatabaseId sysId = NAMED_SYSTEM_DATABASE_ID;
    private static final NamedDatabaseId neoId = databaseIdRepository.defaultDatabase();
    private static final NamedDatabaseId customId = databaseIdRepository.getRaw( CUSTOM_DATABASE_NAME );

    private MultiDatabaseManager<DatabaseContext> databaseManager;
    private DatabaseContext sys;
    private DatabaseContext neo;
    private DatabaseContext custom;

    private void initDatabaseManager() throws Exception
    {
        databaseManager = new StubMultiDatabaseManager();
        sys = databaseManager.createDatabase( sysId );
        neo = databaseManager.createDatabase( neoId );
        custom = databaseManager.createDatabase( customId );
        databaseManager.start();
    }

    @Test
    void crudOperationsFailWithStoppedManager() throws Exception
    {
        // given
        initDatabaseManager();
        List<Consumer<NamedDatabaseId>> crudOps = Arrays.asList( databaseManager::startDatabase, databaseManager::stopDatabase, databaseManager::dropDatabase );
        for ( var op : crudOps )
        {
            op.accept( customId );
        }

        // when
        databaseManager.stop();

        // then
        for ( var op : crudOps )
        {
            try
            {
                op.accept( neoId );
                fail( "Database start, stop and drop operations should fail against stopped database managers!" );
            }
            catch ( IllegalStateException e )
            {
                //expected
            }
        }
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
    void returnsDatabasesInCorrectOrder() throws Exception
    {
        // given
        initDatabaseManager();
        List<String> expectedNames = List.of( SYSTEM_DATABASE_NAME, CUSTOM_DATABASE_NAME, DEFAULT_DATABASE_NAME );

        // when
        List<String> actualNames = databaseManager.registeredDatabases().keySet().stream().map( NamedDatabaseId::name ).collect( Collectors.toList() );

        // then
        assertEquals( expectedNames, actualNames );
    }

    @Test
    void shouldNotDropOnDatabaseIfKeepData() throws Exception
    {
        // given
        initDatabaseManager();

        // when
        databaseManager.dropDatabase( neoId, false );
        databaseManager.dropDatabase( customId, true );

        // then
        verify( neo.database() ).drop();
        verify( custom.database(), never() ).drop();
    }
}
