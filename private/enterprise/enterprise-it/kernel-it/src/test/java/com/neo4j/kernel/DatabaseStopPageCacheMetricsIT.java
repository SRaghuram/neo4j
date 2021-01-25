/*
 * Copyright (c) "Neo4j"
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

import static org.assertj.core.api.Assertions.assertThat;

@EnterpriseDbmsExtension
class DatabaseStopPageCacheMetricsIT
{
    @Inject
    private DatabaseManagementService managementService;
    @Inject
    private PageCacheCounters pageCacheCounters;

    @Test
    void reportPageCacheMetricsOnDatabaseStop()
    {
        var databaseName = "foo";
        managementService.createDatabase( databaseName );

        long pinsBeforeShutdown = pageCacheCounters.pins();
        managementService.shutdownDatabase( databaseName );
        assertThat( pageCacheCounters.pins() ).isGreaterThan( pinsBeforeShutdown );
    }
}
