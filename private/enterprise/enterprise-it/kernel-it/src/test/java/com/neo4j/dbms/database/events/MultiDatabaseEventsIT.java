/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.dbms.database.events;

import com.neo4j.test.extension.EnterpriseDbmsExtension;
import org.apache.commons.lang3.mutable.MutableInt;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.event.DatabaseEventContext;
import org.neo4j.graphdb.event.DatabaseEventListener;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.monitoring.DatabaseHealth;
import org.neo4j.test.extension.Inject;

import static org.junit.jupiter.api.Assertions.assertEquals;

@EnterpriseDbmsExtension
class MultiDatabaseEventsIT
{
    private static final MutableInt ZERO = new MutableInt( 0 );
    @Inject
    private DatabaseManagementService managementService;

    @Test
    void databaseStartNotification()
    {
        MultiDatabaseEventListener eventListener = new MultiDatabaseEventListener();
        managementService.registerDatabaseEventListener( eventListener );

        String firstDatabaseName = "firstDatabaseA";
        String secondDatabaseName = "secondDatabaseA";
        String thirdDatabaseName = "thirdDatabaseA";

        managementService.createDatabase( firstDatabaseName );

        assertEquals( 1, getStartedEvents( eventListener, firstDatabaseName ) );
        assertEquals( 0, getStartedEvents( eventListener, secondDatabaseName ) );
        assertEquals( 0, getStartedEvents( eventListener, thirdDatabaseName ) );

        managementService.createDatabase( secondDatabaseName );
        managementService.createDatabase( thirdDatabaseName );

        assertEquals( 1, getStartedEvents( eventListener, firstDatabaseName ) );
        assertEquals( 1, getStartedEvents( eventListener, secondDatabaseName ) );
        assertEquals( 1, getStartedEvents( eventListener, thirdDatabaseName ) );
    }

    @Test
    void databaseStopNotification()
    {
        MultiDatabaseEventListener eventListener = new MultiDatabaseEventListener();
        managementService.registerDatabaseEventListener( eventListener );

        String firstDatabaseName = "firstDatabaseB";
        String secondDatabaseName = "secondDatabaseB";
        String thirdDatabaseName = "thirdDatabaseB";

        managementService.createDatabase( firstDatabaseName );
        managementService.createDatabase( secondDatabaseName );
        managementService.createDatabase( thirdDatabaseName );

        managementService.shutdownDatabase( firstDatabaseName );
        assertEquals( 1, getShutdownEvents( eventListener, firstDatabaseName ) );
        assertEquals( 0, getShutdownEvents( eventListener, secondDatabaseName ) );
        assertEquals( 0, getShutdownEvents( eventListener, thirdDatabaseName ) );

        managementService.shutdownDatabase( thirdDatabaseName );
        managementService.shutdownDatabase( secondDatabaseName );
        assertEquals( 1, getShutdownEvents( eventListener, firstDatabaseName ) );
        assertEquals( 1, getShutdownEvents( eventListener, secondDatabaseName ) );
        assertEquals( 1, getShutdownEvents( eventListener, thirdDatabaseName ) );
    }

    @Test
    void databasePanicNotification()
    {
        MultiDatabaseEventListener eventListener = new MultiDatabaseEventListener();
        managementService.registerDatabaseEventListener( eventListener );

        String firstDatabaseName = "firstDatabaseC";
        String secondDatabaseName = "secondDatabaseC";

        managementService.createDatabase( firstDatabaseName );
        managementService.createDatabase( secondDatabaseName );

        assertEquals( 0, getPanicEvents( eventListener, firstDatabaseName ) );
        assertEquals( 0, getPanicEvents( eventListener, secondDatabaseName ) );

        panicDatabase( managementService.database( firstDatabaseName ) );
        assertEquals( 1, getPanicEvents( eventListener, firstDatabaseName ) );
        assertEquals( 0, getPanicEvents( eventListener, secondDatabaseName ) );

        panicDatabase( managementService.database( secondDatabaseName ) );
        assertEquals( 1, getPanicEvents( eventListener, firstDatabaseName ) );
        assertEquals( 1, getPanicEvents( eventListener, secondDatabaseName ) );
    }

    private static void panicDatabase( GraphDatabaseService database )
    {
        ((GraphDatabaseAPI) database).getDependencyResolver().resolveDependency( DatabaseHealth.class ).panic( new RuntimeException() );
    }

    private static int getStartedEvents( MultiDatabaseEventListener eventListener, String dbName )
    {
        return eventListener.getStartEvents().getOrDefault( dbName.toLowerCase(), ZERO ).intValue();
    }

    private static int getShutdownEvents( MultiDatabaseEventListener eventListener, String dbName )
    {
        return eventListener.getShutdownEvents().getOrDefault( dbName.toLowerCase(), ZERO ).intValue();
    }

    private static int getPanicEvents( MultiDatabaseEventListener eventListener, String dbName )
    {
        return eventListener.getPanicEvents().getOrDefault( dbName.toLowerCase(), ZERO ).intValue();
    }

    private static class MultiDatabaseEventListener implements DatabaseEventListener
    {
        private final Map<String,MutableInt> startEvents = new HashMap<>();
        private final Map<String,MutableInt> shutdownEvents = new HashMap<>();
        private final Map<String,MutableInt> panicEvents = new HashMap<>();

        @Override
        public void databaseStart( DatabaseEventContext eventContext )
        {
            consumeEvent( eventContext, startEvents );
        }

        @Override
        public void databaseShutdown( DatabaseEventContext eventContext )
        {
            consumeEvent( eventContext, shutdownEvents );
        }

        @Override
        public void databasePanic( DatabaseEventContext eventContext )
        {
            consumeEvent( eventContext, panicEvents );
        }

        private static void consumeEvent( DatabaseEventContext eventContext, Map<String,MutableInt> trackingMap )
        {
            trackingMap.computeIfAbsent( eventContext.getDatabaseName(), s -> new MutableInt( 0 ) ).increment();
        }

        Map<String,MutableInt> getStartEvents()
        {
            return startEvents;
        }

        Map<String,MutableInt> getShutdownEvents()
        {
            return shutdownEvents;
        }

        Map<String,MutableInt> getPanicEvents()
        {
            return panicEvents;
        }
    }
}
