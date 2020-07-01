/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.test.driver;

import org.neo4j.driver.Bookmark;
import org.neo4j.driver.Driver;
import org.neo4j.driver.Session;
import org.neo4j.driver.SessionConfig;

public class DriverTestHelper
{
    private DriverTestHelper()
    {
    }

    public static Bookmark writeData( Driver driver )
    {
        try ( Session session = driver.session() )
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
}
