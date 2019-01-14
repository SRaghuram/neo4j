/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.kernel.ha.factory;

import org.junit.Rule;
import org.junit.Test;

import java.util.function.Predicate;

import org.neo4j.com.storecopy.StoreUtil;
import org.neo4j.graphdb.DependencyResolver;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.kernel.impl.ha.ClusterManager;
import org.neo4j.kernel.impl.index.IndexConfigStore;
import org.neo4j.kernel.impl.pagecache.PageCacheWarmer;
import org.neo4j.kernel.impl.storageengine.impl.recordstorage.id.BufferedIdController;
import org.neo4j.kernel.impl.storageengine.impl.recordstorage.id.IdController;
import org.neo4j.kernel.impl.store.id.BufferingIdGeneratorFactory;
import org.neo4j.kernel.impl.store.id.IdGeneratorFactory;
import org.neo4j.kernel.impl.transaction.log.files.TransactionLogFiles;
import org.neo4j.test.ha.ClusterRule;

import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

public class HighlyAvailableEditionModuleIT
{
    @Rule
    public final ClusterRule clusterRule = new ClusterRule();

    @Test
    public void createBufferedIdComponentsByDefault()
    {
        ClusterManager.ManagedCluster managedCluster = clusterRule.startCluster();
        DependencyResolver dependencyResolver = managedCluster.getMaster().getDependencyResolver();

        IdController idController = dependencyResolver.resolveDependency( IdController.class );
        IdGeneratorFactory idGeneratorFactory = dependencyResolver.resolveDependency( IdGeneratorFactory.class );

        assertThat( idController, instanceOf( BufferedIdController.class ) );
        assertThat( idGeneratorFactory, instanceOf( BufferingIdGeneratorFactory.class ) );
    }

    @Test
    public void fileWatcherFileNameFilter()
    {
        DatabaseLayout databaseLayout = clusterRule.getTestDirectory().databaseLayout();
        Predicate<String> filter = HighlyAvailableEditionModule.fileWatcherFileNameFilter();
        String metadataStoreName = databaseLayout.metadataStore().getName();

        assertFalse( filter.test( metadataStoreName ) );
        assertFalse( filter.test( databaseLayout.nodeStore().getName() ) );
        assertTrue( filter.test( TransactionLogFiles.DEFAULT_NAME + ".1" ) );
        assertTrue( filter.test( IndexConfigStore.INDEX_DB_FILE_NAME + ".any" ) );
        assertTrue( filter.test( StoreUtil.BRANCH_SUBDIRECTORY ) );
        assertTrue( filter.test( StoreUtil.TEMP_COPY_DIRECTORY_NAME ) );
        assertTrue( filter.test( metadataStoreName + PageCacheWarmer.SUFFIX_CACHEPROF ) );
    }
}
