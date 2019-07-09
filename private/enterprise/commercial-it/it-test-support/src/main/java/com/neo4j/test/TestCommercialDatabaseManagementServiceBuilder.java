/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.test;

import com.neo4j.commercial.edition.CommercialEditionModule;
import com.neo4j.kernel.impl.enterprise.configuration.OnlineBackupSettings;

import java.io.File;
import java.util.Map;
import java.util.function.Function;

import org.neo4j.common.DependencyResolver;
import org.neo4j.common.Edition;
import org.neo4j.configuration.Config;
import org.neo4j.graphdb.config.Setting;
import org.neo4j.graphdb.factory.module.GlobalModule;
import org.neo4j.graphdb.factory.module.edition.AbstractEditionModule;
import org.neo4j.graphdb.security.URLAccessRule;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.impl.factory.DatabaseInfo;
import org.neo4j.logging.LogProvider;
import org.neo4j.monitoring.Monitors;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.time.SystemNanoClock;

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

    // Override to allow chaining

    @Override
    public TestCommercialDatabaseManagementServiceBuilder impermanent()
    {
        return (TestCommercialDatabaseManagementServiceBuilder) super.impermanent();
    }

    @Override
    public TestCommercialDatabaseManagementServiceBuilder setFileSystem( FileSystemAbstraction fileSystem )
    {
        return (TestCommercialDatabaseManagementServiceBuilder) super.setFileSystem( fileSystem );
    }

    @Override
    public TestCommercialDatabaseManagementServiceBuilder setDatabaseRootDirectory( File storeDir )
    {
        return (TestCommercialDatabaseManagementServiceBuilder) super.setDatabaseRootDirectory( storeDir );
    }

    @Override
    public TestCommercialDatabaseManagementServiceBuilder setInternalLogProvider( LogProvider internalLogProvider )
    {
        return (TestCommercialDatabaseManagementServiceBuilder) super.setInternalLogProvider( internalLogProvider );
    }

    @Override
    public TestCommercialDatabaseManagementServiceBuilder setClock( SystemNanoClock clock )
    {
        return (TestCommercialDatabaseManagementServiceBuilder) super.setClock( clock );
    }

    @Override
    public TestCommercialDatabaseManagementServiceBuilder setExternalDependencies( DependencyResolver dependencies )
    {
        return (TestCommercialDatabaseManagementServiceBuilder) super.setExternalDependencies( dependencies );
    }

    @Override
    public TestCommercialDatabaseManagementServiceBuilder setMonitors( Monitors monitors )
    {
        return (TestCommercialDatabaseManagementServiceBuilder) super.setMonitors( monitors );
    }

    @Override
    public TestCommercialDatabaseManagementServiceBuilder setUserLogProvider( LogProvider logProvider )
    {
        return (TestCommercialDatabaseManagementServiceBuilder) super.setUserLogProvider( logProvider );
    }

    @Override
    public TestCommercialDatabaseManagementServiceBuilder addURLAccessRule( String protocol, URLAccessRule rule )
    {
        return (TestCommercialDatabaseManagementServiceBuilder) super.addURLAccessRule( protocol, rule );
    }

    @Override
    public TestCommercialDatabaseManagementServiceBuilder setConfig( String name, String value )
    {
        return (TestCommercialDatabaseManagementServiceBuilder) super.setConfig( name, value );
    }

    @Override
    public TestCommercialDatabaseManagementServiceBuilder setConfig( Setting<?> setting, String value )
    {
        return (TestCommercialDatabaseManagementServiceBuilder) super.setConfig( setting, value );
    }

    @Override
    public TestCommercialDatabaseManagementServiceBuilder setConfig( Map<Setting<?>,String> config )
    {
        return (TestCommercialDatabaseManagementServiceBuilder) super.setConfig( config );
    }

    @Override
    public TestCommercialDatabaseManagementServiceBuilder setConfigRaw( Map<String,String> config )
    {
        return (TestCommercialDatabaseManagementServiceBuilder) super.setConfigRaw( config );
    }
}

