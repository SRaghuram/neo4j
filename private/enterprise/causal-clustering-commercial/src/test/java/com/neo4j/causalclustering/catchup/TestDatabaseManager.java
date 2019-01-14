/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.catchup;

import java.util.Optional;

import org.neo4j.dbms.database.DatabaseContext;
import org.neo4j.dbms.database.DatabaseManager;
import org.neo4j.kernel.database.Database;
import org.neo4j.storageengine.api.StoreId;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class TestDatabaseManager
{
    static DatabaseManager newDbManager( String existingDb )
    {
        DatabaseManager dbManager = mock( DatabaseManager.class );
        DatabaseContext dbContext = mock( DatabaseContext.class );
        Database db = mock( Database.class );
        when( db.getDatabaseName() ).thenReturn( existingDb );
        when( db.getStoreId() ).thenReturn( StoreId.DEFAULT );
        when( dbContext.getDatabase() ).thenReturn( db );
        when( dbManager.getDatabaseContext( existingDb ) ).thenReturn( Optional.of( dbContext ) );
        return dbManager;
    }
}
