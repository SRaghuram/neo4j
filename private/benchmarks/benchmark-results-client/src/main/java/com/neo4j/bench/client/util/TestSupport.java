/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.client.util;

import com.neo4j.bench.client.database.Store;
import com.neo4j.commercial.edition.factory.CommercialDatabaseManagementServiceBuilder;

import java.nio.file.Path;

import org.neo4j.dbms.database.DatabaseManagementService;

public class TestSupport
{
    public static Store createEmptyStore( Path storeDir )
    {
        DatabaseManagementService managementService = new CommercialDatabaseManagementServiceBuilder( storeDir.toFile() )
                .build();
        managementService.shutdown();
        return Store.createFrom( storeDir );
    }
}
