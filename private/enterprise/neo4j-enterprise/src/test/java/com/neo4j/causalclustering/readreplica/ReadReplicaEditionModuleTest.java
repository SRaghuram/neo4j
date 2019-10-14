/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.readreplica;

import com.neo4j.kernel.impl.pagecache.PageCacheWarmer;
import org.junit.jupiter.api.Test;

import java.util.function.Predicate;

import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.kernel.impl.transaction.log.files.TransactionLogFilesHelper;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.Neo4jLayoutExtension;
import org.neo4j.test.extension.SkipThreadLeakageGuard;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

@SkipThreadLeakageGuard
@Neo4jLayoutExtension
class ReadReplicaEditionModuleTest
{
    @Inject
    private DatabaseLayout databaseLayout;

    @Test
    void fileWatcherFileNameFilter()
    {

        Predicate<String> filter = ReadReplicaEditionModule.fileWatcherFileNameFilter();
        String metadataStoreName = databaseLayout.metadataStore().getName();

        assertFalse( filter.test( metadataStoreName ) );
        assertFalse( filter.test( databaseLayout.nodeStore().getName() ) );
        assertTrue( filter.test( TransactionLogFilesHelper.DEFAULT_NAME + ".1" ) );
        assertTrue( filter.test( metadataStoreName + PageCacheWarmer.SUFFIX_CACHEPROF ) );
    }
}
