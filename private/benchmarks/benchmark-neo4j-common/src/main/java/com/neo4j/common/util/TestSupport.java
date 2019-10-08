/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.common.util;

import com.neo4j.bench.common.database.Store;
import com.neo4j.dbms.api.EnterpriseDatabaseManagementServiceBuilder;

import java.nio.file.Path;

import org.neo4j.dbms.api.DatabaseManagementService;

public class TestSupport
{
    public static Store createEmptyStore( Path homeDir, Path neo4jConfigFile )
    {
        DatabaseManagementService managementService =
                new EnterpriseDatabaseManagementServiceBuilder( homeDir.toFile() )
                        .loadPropertiesFromFile( neo4jConfigFile.toFile().getAbsolutePath() )
                        .build();
        managementService.shutdown();
        return Store.createFrom( homeDir );
    }
}
