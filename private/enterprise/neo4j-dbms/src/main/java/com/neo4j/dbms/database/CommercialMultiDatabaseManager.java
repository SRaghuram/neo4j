/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.dbms.database;

import org.neo4j.dbms.database.StandaloneDatabaseContext;
import org.neo4j.graphdb.factory.module.GlobalModule;
import org.neo4j.graphdb.factory.module.edition.AbstractEditionModule;
import org.neo4j.kernel.database.Database;
import org.neo4j.kernel.database.DatabaseId;
import org.neo4j.logging.Log;

public class CommercialMultiDatabaseManager extends MultiDatabaseManager<StandaloneDatabaseContext>
{
    public CommercialMultiDatabaseManager( GlobalModule globalModule, AbstractEditionModule edition )
    {
        super( globalModule, edition );
    }

    @Override
    protected StandaloneDatabaseContext createDatabaseContext( DatabaseId databaseId )
    {
        var databaseCreationContext = newDatabaseCreationContext( databaseId, globalModule.getGlobalDependencies(), globalModule.getGlobalMonitors() );
        var kernelDatabase = new Database( databaseCreationContext );
        return new StandaloneDatabaseContext( kernelDatabase );
    }
}
