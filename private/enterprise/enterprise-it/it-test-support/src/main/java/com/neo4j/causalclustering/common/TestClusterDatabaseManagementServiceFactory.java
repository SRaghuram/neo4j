/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.common;

import java.util.function.Function;

import org.neo4j.configuration.Config;
import org.neo4j.graphdb.facade.DatabaseManagementServiceFactory;
import org.neo4j.graphdb.facade.ExternalDependencies;
import org.neo4j.graphdb.factory.module.GlobalModule;
import org.neo4j.graphdb.factory.module.edition.AbstractEditionModule;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.impl.factory.DbmsInfo;
import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.logging.LogProvider;
import org.neo4j.logging.internal.LogService;
import org.neo4j.logging.internal.SimpleLogService;

public class TestClusterDatabaseManagementServiceFactory extends DatabaseManagementServiceFactory
{
    public TestClusterDatabaseManagementServiceFactory( DbmsInfo dbmsInfo, Function<GlobalModule,AbstractEditionModule> editionFactory )
    {
        super( dbmsInfo, editionFactory );
    }

    @Override
    protected GlobalModule createGlobalModule( Config config, ExternalDependencies dependencies )
    {
        return new TestClusterGlobalModule( config, dependencies, this.dbmsInfo );
    }

    static class TestClusterGlobalModule extends GlobalModule
    {
        TestClusterGlobalModule( Config config, ExternalDependencies dependencies, DbmsInfo dbmsInfo )
        {
            super( config, dbmsInfo, dependencies );
        }

        @Override
        protected LogService createLogService( LogProvider userLogProvider )
        {
            var standardLogService = super.createLogService( userLogProvider );

            // For tests, if the provided log is assertable log use it for both user and debug logs.
            // this allows us to assert on the contents of the debug log
            if ( userLogProvider instanceof AssertableLogProvider )
            {
                var newLogService = new SimpleLogService( standardLogService.getUserLogProvider() );
                return getGlobalLife().add( newLogService );
            }

            return standardLogService;
        }

        @Override
        protected FileSystemAbstraction createFileSystemAbstraction()
        {
            return new TestClusterFileSystem();
        }
    }
}
