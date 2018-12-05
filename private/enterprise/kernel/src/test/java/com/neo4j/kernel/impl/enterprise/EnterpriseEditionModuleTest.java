/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.kernel.impl.enterprise;

import com.neo4j.kernel.impl.pagecache.PageCacheWarmer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.function.Predicate;

import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.kernel.impl.transaction.log.files.TransactionLogFiles;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

@ExtendWith( TestDirectoryExtension.class )
class EnterpriseEditionModuleTest
{
    @Inject
    private TestDirectory testDirectory;

    @Test
    void fileWatcherFileNameFilter()
    {
        DatabaseLayout layout = testDirectory.databaseLayout();
        Predicate<String> filter = EnterpriseEditionModule.enterpriseNonClusterFileWatcherFileNameFilter();
        String metadataStoreName = layout.metadataStore().getName();

        assertFalse( filter.test( metadataStoreName ) );
        assertFalse( filter.test( layout.nodeStore().getName() ) );
        assertTrue( filter.test( TransactionLogFiles.DEFAULT_NAME + ".1" ) );
        assertTrue( filter.test( metadataStoreName + PageCacheWarmer.SUFFIX_CACHEPROF ) );
    }
}
