/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.commercial.edition.factory;

import com.neo4j.commercial.edition.CommercialEditionModule;
import com.neo4j.kernel.impl.enterprise.lock.forseti.ForsetiLocksFactory;

import java.io.File;
import java.util.Collections;
import java.util.function.Function;

import org.neo4j.common.Edition;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.cypher.internal.javacompat.CommunityCypherEngineProvider;
import org.neo4j.cypher.internal.javacompat.EnterpriseCypherEngineProvider;
import org.neo4j.dbms.api.DatabaseManagementServiceBuilder;
import org.neo4j.graphdb.facade.ExternalDependencies;
import org.neo4j.graphdb.factory.module.GlobalModule;
import org.neo4j.graphdb.factory.module.edition.AbstractEditionModule;
import org.neo4j.kernel.impl.factory.DatabaseInfo;

import static org.neo4j.graphdb.facade.GraphDatabaseDependencies.newDependencies;

public class CommercialDatabaseManagementServiceBuilder extends DatabaseManagementServiceBuilder
{
    public CommercialDatabaseManagementServiceBuilder( File databaseRootDir )
    {
        super( databaseRootDir );
    }

    @Override
    public String getEdition()
    {
        return Edition.COMMERCIAL.toString();
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
                .databaseEventListeners( databaseEventListeners )
                .queryEngineProviders( Collections.singletonList( new EnterpriseCypherEngineProvider() ) );
    }

    @Override
    protected DatabaseInfo getDatabaseInfo()
    {
        return DatabaseInfo.COMMERCIAL;
    }

    @Override
    protected Function<GlobalModule,AbstractEditionModule> getEditionFactory()
    {
        return CommercialEditionModule::new;
    }

    @Override
    protected Config augmentConfig( Config config )
    {
        config = super.augmentConfig( config );
        config.setIfNotSet( GraphDatabaseSettings.lock_manager, ForsetiLocksFactory.KEY );
        return config;
    }
}
