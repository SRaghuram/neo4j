/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.test;

import com.neo4j.commercial.edition.CommercialEditionModule;
import com.neo4j.kernel.impl.enterprise.configuration.OnlineBackupSettings;

import java.io.File;
import java.util.function.Function;

import org.neo4j.common.Edition;
import org.neo4j.configuration.Config;
import org.neo4j.graphdb.factory.module.GlobalModule;
import org.neo4j.graphdb.factory.module.edition.AbstractEditionModule;
import org.neo4j.kernel.impl.factory.DatabaseInfo;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;

import static org.neo4j.configuration.Settings.FALSE;

public class TestCommercialDatabaseManagementServiceBuilder extends TestDatabaseManagementServiceBuilder
{
    public TestCommercialDatabaseManagementServiceBuilder()
    {
        super();
    }

    public TestCommercialDatabaseManagementServiceBuilder( File databaseRootDir )
    {
        super( databaseRootDir );
    }

    @Override
    protected Config augmentConfig( Config config )
    {
        config = super.augmentConfig( config );
        config.augmentDefaults( OnlineBackupSettings.online_backup_listen_address, "127.0.0.1:0" );
        config.augmentDefaults( OnlineBackupSettings.online_backup_enabled, FALSE );
        return config;
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
    public String getEdition()
    {
        return Edition.COMMERCIAL.toString();
    }
}

