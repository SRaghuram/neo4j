/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.kernel;

import com.neo4j.test.extension.EnterpriseDbmsExtension;
import org.junit.jupiter.api.Test;

import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.io.pagecache.monitoring.PageCacheCounters;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.extension.Inject;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThan;

@EnterpriseDbmsExtension
class DatabaseStopPageCacheMetricsIT
{
    @Inject
    private DatabaseManagementService managementService;

    @Test
    void reportPageCacheMetricsOnDatabaseStop()
    {
        var databaseName = "foo";
        managementService.createDatabase( databaseName );

        var databaseAPI = (GraphDatabaseAPI) managementService.database( databaseName );
        var cacheCounters = databaseAPI.getDependencyResolver().resolveDependency( PageCacheCounters.class );

        long pinsBeforeShutdown = cacheCounters.pins();
        managementService.shutdownDatabase( databaseName );
        assertThat( cacheCounters.pins(), greaterThan( pinsBeforeShutdown ) ) ;
    }
}
