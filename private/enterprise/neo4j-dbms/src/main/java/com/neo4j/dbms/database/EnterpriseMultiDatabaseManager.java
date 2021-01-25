/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.dbms.database;

import org.neo4j.dbms.database.StandaloneDatabaseContext;
import org.neo4j.graphdb.factory.module.GlobalModule;
import org.neo4j.graphdb.factory.module.edition.AbstractEditionModule;
import org.neo4j.kernel.database.Database;
import org.neo4j.kernel.database.NamedDatabaseId;

public class EnterpriseMultiDatabaseManager extends MultiDatabaseManager<StandaloneDatabaseContext>
{
    public EnterpriseMultiDatabaseManager( GlobalModule globalModule, AbstractEditionModule edition )
    {
        super( globalModule, edition );
    }

    @Override
    protected StandaloneDatabaseContext createDatabaseContext( NamedDatabaseId namedDatabaseId )
    {
        var databaseCreationContext = newDatabaseCreationContext( namedDatabaseId, globalModule.getGlobalDependencies(), globalModule.getGlobalMonitors() );
        var kernelDatabase = new Database( databaseCreationContext );
        return new StandaloneDatabaseContext( kernelDatabase );
    }
}
