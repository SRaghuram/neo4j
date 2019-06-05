/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.client.util;

import com.neo4j.bench.client.database.Store;
import com.neo4j.commercial.edition.factory.CommercialDatabaseManagementServiceBuilder;

import java.nio.file.Path;

import org.neo4j.dbms.api.DatabaseManagementService;

import static com.neo4j.kernel.impl.enterprise.configuration.OnlineBackupSettings.online_backup_enabled;

public class TestSupport
{
    public static Store createEmptyStore( Path storeDir )
    {
        DatabaseManagementService managementService = new CommercialDatabaseManagementServiceBuilder( storeDir.toFile() )
                .setConfig( online_backup_enabled,"false" )
                .build();
        managementService.shutdown();
        return Store.createFrom( storeDir );
    }
}
