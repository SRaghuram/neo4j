/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.dbms.api;

import com.neo4j.enterprise.edition.EnterpriseEditionModule;
import com.neo4j.kernel.impl.enterprise.configuration.EnterpriseEditionSettings;

import java.io.File;
import java.util.function.Function;

import org.neo4j.annotations.api.PublicApi;
import org.neo4j.common.Edition;
import org.neo4j.configuration.Config;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.dbms.api.DatabaseManagementServiceBuilder;
import org.neo4j.graphdb.facade.ExternalDependencies;
import org.neo4j.graphdb.factory.module.GlobalModule;
import org.neo4j.graphdb.factory.module.edition.AbstractEditionModule;
import org.neo4j.kernel.impl.factory.DatabaseInfo;

import static org.neo4j.graphdb.facade.GraphDatabaseDependencies.newDependencies;

/**
 * Creates a {@link DatabaseManagementService} with Enterprise Edition features.
 */
@PublicApi
public class EnterpriseDatabaseManagementServiceBuilder extends DatabaseManagementServiceBuilder
{
    public EnterpriseDatabaseManagementServiceBuilder( File homeDirectory )
    {
        super( homeDirectory );
    }

    @Override
    public String getEdition()
    {
        return Edition.ENTERPRISE.toString();
    }

    @Override
    protected ExternalDependencies databaseDependencies()
    {
        return newDependencies()
                .monitors( monitors )
                .userLogProvider( userLogProvider )
                .dependencies( dependencies )
                .urlAccessRules( urlAccessRules )
                .extensions( extensions )
                .databaseEventListeners( databaseEventListeners );
    }

    @Override
    protected DatabaseInfo getDatabaseInfo( Config config )
    {
        EnterpriseEditionSettings.Mode mode = config.get( EnterpriseEditionSettings.mode );
        if ( mode == EnterpriseEditionSettings.Mode.SINGLE )
        {
            return DatabaseInfo.ENTERPRISE;
        }
        throw new IllegalArgumentException( "Unsupported mode: " + mode );
    }

    @Override
    protected Function<GlobalModule,AbstractEditionModule> getEditionFactory( Config config )
    {
        EnterpriseEditionSettings.Mode mode = config.get( EnterpriseEditionSettings.mode );
        if ( mode == EnterpriseEditionSettings.Mode.SINGLE )
        {
            return EnterpriseEditionModule::new;
        }
        throw new IllegalArgumentException( "Unsupported mode: " + mode );
    }
}
