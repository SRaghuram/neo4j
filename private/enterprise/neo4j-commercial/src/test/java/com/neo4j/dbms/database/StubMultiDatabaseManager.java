/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.dbms.database;

import org.neo4j.dbms.database.DatabaseContext;
import org.neo4j.dbms.database.StandaloneDatabaseContext;
import org.neo4j.kernel.database.Database;
import org.neo4j.kernel.database.DatabaseId;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;
import org.neo4j.logging.NullLog;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

public class StubMultiDatabaseManager extends MultiDatabaseManager<DatabaseContext>
{
    StubMultiDatabaseManager()
    {
        super( null, null, NullLog.getInstance(), null );
    }

    @Override
    protected DatabaseContext createNewDatabaseContext( DatabaseId databaseId )
    {
        Database db = mock( Database.class );
        when( db.getDatabaseId() ).thenReturn( databaseId );
        return databaseContextFactory( db, mock( GraphDatabaseFacade.class ) );
    }

    @Override
    protected DatabaseContext databaseContextFactory( Database database, GraphDatabaseFacade facade )
    {
        return spy( new StandaloneDatabaseContext( database, facade ) );
    }
}
