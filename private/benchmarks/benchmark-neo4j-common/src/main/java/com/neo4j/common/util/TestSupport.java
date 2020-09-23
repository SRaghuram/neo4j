/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.common.util;

import com.neo4j.bench.common.database.Neo4jStore;
import com.neo4j.bench.common.database.Store;
import com.neo4j.bench.model.model.Neo4jConfig;
import com.neo4j.dbms.api.EnterpriseDatabaseManagementServiceBuilder;

import java.nio.file.Path;

import org.neo4j.dbms.api.DatabaseManagementService;

public class TestSupport
{
    public static Store createEmptyStore( Path homeDir, Path neo4jConfigFile )
    {
        DatabaseManagementService managementService =
                new EnterpriseDatabaseManagementServiceBuilder( homeDir )
                        .loadPropertiesFromFile( neo4jConfigFile )
                        .build();
        managementService.shutdown();
        return Neo4jStore.createFrom( homeDir );
    }

    public static Store createEmptyStore( Path homeDir, Neo4jConfig neo4jConfig )
    {
        DatabaseManagementService managementService =
                new EnterpriseDatabaseManagementServiceBuilder( homeDir )
                        .setConfigRaw( neo4jConfig.toMap() )
                        .build();
        managementService.shutdown();
        return Neo4jStore.createFrom( homeDir );
    }

    public static Store createTemporaryEmptyStore( Path homeDir, Path neo4jConfigFile )
    {
        DatabaseManagementService managementService =
                new EnterpriseDatabaseManagementServiceBuilder( homeDir )
                        .loadPropertiesFromFile( neo4jConfigFile )
                        .build();
        managementService.shutdown();
        return Neo4jStore.createTemporaryFrom( homeDir );
    }

}
