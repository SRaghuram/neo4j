/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core;

import com.neo4j.causalclustering.common.Cluster;
import com.neo4j.kernel.impl.pagecache.PageCacheWarmer;
import com.neo4j.test.causalclustering.ClusterExtension;
import com.neo4j.test.causalclustering.ClusterFactory;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.function.Predicate;

import org.neo4j.common.DependencyResolver;
import org.neo4j.internal.id.BufferedIdController;
import org.neo4j.internal.id.BufferingIdGeneratorFactory;
import org.neo4j.internal.id.IdController;
import org.neo4j.internal.id.IdGeneratorFactory;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.kernel.impl.transaction.log.files.TransactionLogFilesHelper;
import org.neo4j.test.extension.Inject;

import static com.neo4j.test.causalclustering.ClusterConfig.clusterConfig;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

@ClusterExtension
class CoreEditionModuleIT
{
    @Inject
    private ClusterFactory clusterFactory;

    private Cluster cluster;

    @BeforeAll
    void beforeAll() throws Exception
    {
        cluster = clusterFactory.createCluster( clusterConfig().withNumberOfCoreMembers( 3 ).withNumberOfReadReplicas( 0 ) );
        cluster.start();
    }

    @Test
    void createBufferedIdComponentsByDefault() throws Exception
    {
        CoreClusterMember leader = cluster.awaitLeader();
        DependencyResolver dependencyResolver = leader.defaultDatabase().getDependencyResolver();

        IdController idController = dependencyResolver.resolveDependency( IdController.class );
        IdGeneratorFactory idGeneratorFactory = dependencyResolver.resolveDependency( IdGeneratorFactory.class );

        assertThat( idController, instanceOf( BufferedIdController.class ) );
        assertThat( idGeneratorFactory, instanceOf( BufferingIdGeneratorFactory.class ) );
    }

    @Test
    void fileWatcherFileNameFilter() throws Exception
    {
        DatabaseLayout layout = cluster.awaitLeader().databaseLayout();
        Predicate<String> filter = CoreEditionModule.fileWatcherFileNameFilter();
        String metadataStoreName = layout.metadataStore().getFileName().toString();
        assertFalse( filter.test( metadataStoreName ) );
        assertFalse( filter.test( layout.nodeStore().getFileName().toString() ) );
        assertTrue( filter.test( TransactionLogFilesHelper.DEFAULT_NAME + ".1" ) );
        assertTrue( filter.test( metadataStoreName + PageCacheWarmer.SUFFIX_CACHEPROF ) );
    }
}
