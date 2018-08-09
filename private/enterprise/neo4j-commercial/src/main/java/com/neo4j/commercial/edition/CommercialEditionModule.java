/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.commercial.edition;

import com.neo4j.dbms.database.MultiDatabaseManager;

import org.neo4j.dbms.database.DatabaseManager;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.graphdb.factory.module.EditionModule;
import org.neo4j.graphdb.factory.module.PlatformModule;
import org.neo4j.kernel.api.security.SecurityModule;
import org.neo4j.kernel.api.security.UserManagerSupplier;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.enterprise.api.security.EnterpriseAuthManager;
import org.neo4j.kernel.impl.enterprise.EnterpriseEditionModule;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;
import org.neo4j.kernel.impl.proc.Procedures;
import org.neo4j.logging.Logger;

class CommercialEditionModule extends EnterpriseEditionModule
{
    CommercialEditionModule( PlatformModule platformModule )
    {
        super( platformModule );
    }

    @Override
    public DatabaseManager createDatabaseManager( GraphDatabaseFacade graphDatabaseFacade, PlatformModule platform, EditionModule edition,
            Procedures procedures, Logger msgLog )
    {
        return new MultiDatabaseManager( platform, edition, procedures, msgLog, graphDatabaseFacade );
    }

    @Override
    public void createDatabases( DatabaseManager databaseManager, Config config )
    {
        GraphDatabaseFacade systemFacade = null; //databaseManager.createDatabase( MultiDatabaseManager.SYSTEM_DB_NAME );
        createConfiguredDatabases( databaseManager, systemFacade, config );
    }

    private static void createConfiguredDatabases( DatabaseManager databaseManager, GraphDatabaseFacade systemFacade, Config config )
    {
        databaseManager.createDatabase( config.get( GraphDatabaseSettings.active_database ) );
    }

    @Override
    public void createSecurityModule( PlatformModule platformModule, Procedures procedures )
    {
        CommercialEditionModule.createCommercialSecurityModule( this, platformModule, procedures );
    }

    private static void createCommercialSecurityModule( EditionModule editionModule, PlatformModule platformModule, Procedures procedures )
    {
        if ( platformModule.config.get( GraphDatabaseSettings.auth_enabled ) )
        {
            SecurityModule securityModule = setupSecurityModule( platformModule,
                    platformModule.logging.getUserLog( EnterpriseEditionModule.class ),
                    procedures, "commercial-security-module" );
            editionModule.authManager = securityModule.authManager();
            editionModule.userManagerSupplier = securityModule.userManagerSupplier();
            platformModule.life.add( securityModule );
        }
        else
        {
            editionModule.authManager = EnterpriseAuthManager.NO_AUTH;
            editionModule.userManagerSupplier = UserManagerSupplier.NO_AUTH;
            platformModule.life.add( platformModule.dependencies.satisfyDependency( editionModule.authManager ) );
            platformModule.life.add( platformModule.dependencies.satisfyDependency( editionModule.userManagerSupplier ) );
        }
    }
}
