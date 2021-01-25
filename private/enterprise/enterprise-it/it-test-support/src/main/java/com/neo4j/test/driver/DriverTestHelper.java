/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.test.driver;

import org.neo4j.driver.Bookmark;
import org.neo4j.driver.Driver;
import org.neo4j.driver.Session;
import org.neo4j.driver.SessionConfig;
import org.neo4j.kernel.database.SystemDbDatabaseIdRepository;

import static java.lang.String.format;

public class DriverTestHelper
{
    private DriverTestHelper()
    {
    }

    public static Bookmark writeData( Driver driver )
    {
        return writeData( driver, SessionConfig.builder().build() );
    }

    public static Bookmark writeData( Driver driver, String databaseName )
    {
        return writeData( driver, SessionConfig.builder().withDatabase( databaseName ).build() );
    }

    private static Bookmark writeData( Driver driver, SessionConfig sessionConfig )
    {
        try ( Session session = driver.session( sessionConfig ) )
        {
            session.writeTransaction( tx -> tx.run( "CREATE (:foo {bar: 'baz'})" ) ).consume();
            return session.lastBookmark();
        }
    }

    public static void readData( Driver driver, Bookmark bookmark )
    {
        try ( Session session = driver.session( SessionConfig.builder().withBookmarks( bookmark ).build() ) )
        {
            session.readTransaction( tx -> tx.run( "MATCH (n) RETURN count(n)" ) ).consume();
        }
    }

    public static void createDatabaseNoWait( Driver driver, String databaseName )
    {
        asyncDatabaseOperation( driver, "CREATE", databaseName );
    }

    public static void stopDatabaseNoWait( Driver driver, String databaseName )
    {
        asyncDatabaseOperation( driver, "STOP", databaseName );
    }

    public static void startDatabaseNoWait( Driver driver, String databaseName )
    {
        asyncDatabaseOperation( driver, "START", databaseName );
    }

    public static void dropDatabaseNoWait( Driver driver, String databaseName )
    {
        asyncDatabaseOperation( driver, "DROP", databaseName );
    }

    public static WaitResponses createDatabaseWait( Driver driver, String databaseName )
    {
        return blockingDatabaseOperation( driver, "CREATE", databaseName );
    }

    public static WaitResponses stopDatabaseWait( Driver driver, String databaseName )
    {
        return blockingDatabaseOperation( driver, "STOP", databaseName );
    }

    public static WaitResponses startDatabaseWait( Driver driver, String databaseName )
    {
        return blockingDatabaseOperation( driver, "START", databaseName );
    }

    public static WaitResponses dropDatabaseWait( Driver driver, String databaseName )
    {
        return blockingDatabaseOperation( driver, "DROP", databaseName );
    }

    private static void asyncDatabaseOperation( Driver driver, String operation, String databaseName )
    {
        try ( Session session = driver.session( SessionConfig.builder().withDatabase( SystemDbDatabaseIdRepository.NAMED_SYSTEM_DATABASE_ID.name() ).build() ) )
        {
            session.writeTransaction( tx -> tx.run( format( "%s DATABASE %s %s", operation, databaseName, "NOWAIT" ) ) ).consume();
        }
    }

    private static WaitResponses blockingDatabaseOperation( Driver driver, String operation, String databaseName )
    {
        try ( Session session = driver.session( SessionConfig.builder().withDatabase( SystemDbDatabaseIdRepository.NAMED_SYSTEM_DATABASE_ID.name() ).build() ) )
        {
            return session.writeTransaction( tx -> WaitResponses.create( tx.run( format( "%s DATABASE %s %s", operation, databaseName, "WAIT" ) ) ) );
        }
    }
}
