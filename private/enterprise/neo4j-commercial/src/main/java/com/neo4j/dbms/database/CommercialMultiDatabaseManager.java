/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.dbms.database;

import org.neo4j.collection.Dependencies;
import org.neo4j.dbms.database.StandaloneDatabaseContext;
import org.neo4j.graphdb.factory.module.GlobalModule;
import org.neo4j.graphdb.factory.module.ModularDatabaseCreationContext;
import org.neo4j.graphdb.factory.module.edition.AbstractEditionModule;
import org.neo4j.graphdb.factory.module.edition.context.EditionDatabaseComponents;
import org.neo4j.kernel.api.procedure.GlobalProcedures;
import org.neo4j.kernel.database.Database;
import org.neo4j.kernel.database.DatabaseCreationContext;
import org.neo4j.kernel.database.DatabaseId;
import org.neo4j.logging.Log;
import org.neo4j.monitoring.Monitors;

public class CommercialMultiDatabaseManager extends MultiDatabaseManager<StandaloneDatabaseContext>
{
    public CommercialMultiDatabaseManager( GlobalModule globalModule, AbstractEditionModule edition, Log log )
    {
        super( globalModule, edition, log );
    }

    @Override
    protected StandaloneDatabaseContext createDatabaseContext( DatabaseId databaseId )
    {
        Database kernelDatabase = createKernelDatabase( databaseId, globalModule.getGlobalDependencies(), globalModule.getGlobalMonitors() );
        return new StandaloneDatabaseContext( kernelDatabase );
    }

    private Database createKernelDatabase( DatabaseId databaseId, Dependencies parentDependencies, Monitors parentMonitors )
    {
        log.info( "Creating '%s' database.", databaseId.name() );
        DatabaseCreationContext databaseCreationContext = newDatabaseCreationContext( databaseId, parentDependencies, parentMonitors );
        return new Database( databaseCreationContext );
    }

    private DatabaseCreationContext newDatabaseCreationContext( DatabaseId databaseId, Dependencies parentDependencies, Monitors parentMonitors )
    {
        EditionDatabaseComponents editionDatabaseComponents = edition.createDatabaseComponents( databaseId );
        GlobalProcedures globalProcedures = edition.getGlobalProcedures();
        return new ModularDatabaseCreationContext( databaseId, globalModule, parentDependencies, parentMonitors, editionDatabaseComponents, globalProcedures );
    }
}
