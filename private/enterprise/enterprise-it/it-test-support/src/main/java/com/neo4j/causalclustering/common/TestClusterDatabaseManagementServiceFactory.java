/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
        protected FileSystemAbstraction createFileSystemAbstraction()
        {
            return new TestClusterFileSystem();
        }
    }
}
