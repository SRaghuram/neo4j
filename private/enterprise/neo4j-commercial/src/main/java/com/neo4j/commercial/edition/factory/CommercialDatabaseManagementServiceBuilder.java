/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.commercial.edition.factory;

import com.neo4j.commercial.edition.CommercialEditionModule;

import java.io.File;

import org.neo4j.common.Edition;
import org.neo4j.configuration.Config;
import org.neo4j.dbms.database.DatabaseManagementService;
import org.neo4j.graphdb.facade.DatabaseManagementServiceFactory;
import org.neo4j.graphdb.factory.DatabaseManagementServiceBuilder;
import org.neo4j.graphdb.factory.DatabaseManagementServiceInternalBuilder;
import org.neo4j.graphdb.factory.GraphDatabaseFactoryState;
import org.neo4j.kernel.impl.factory.DatabaseInfo;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;

public class CommercialDatabaseManagementServiceBuilder extends DatabaseManagementServiceBuilder
{
    @Override
    protected DatabaseManagementServiceInternalBuilder.DatabaseCreator createDatabaseCreator( File storeDir, GraphDatabaseFactoryState state )
    {
        return new CommercialDatabaseCreator( storeDir, state );
    }

    @Override
    public String getEdition()
    {
        return Edition.COMMERCIAL.toString();
    }

    private static class CommercialDatabaseCreator implements DatabaseManagementServiceInternalBuilder.DatabaseCreator
    {
        private final File storeDir;
        private final GraphDatabaseFactoryState state;

        CommercialDatabaseCreator( File storeDir, GraphDatabaseFactoryState state )
        {
            this.storeDir = storeDir;
            this.state = state;
        }

        @Override
        public DatabaseManagementService newDatabase( Config config )
        {
            return new DatabaseManagementServiceFactory( DatabaseInfo.COMMERCIAL, CommercialEditionModule::new )
                    .initFacade( storeDir, config, state.databaseDependencies(), new GraphDatabaseFacade() );
        }
    }
}
