/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.io.pagecache.impl.muninn;

import com.neo4j.causalclustering.common.Cluster;
import com.neo4j.causalclustering.core.CoreClusterMember;
import com.neo4j.test.causalclustering.ClusterExtension;
import com.neo4j.test.causalclustering.ClusterFactory;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.Callable;

import org.neo4j.common.DependencyResolver;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.internal.recordstorage.RecordStorageEngine;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.PagedFile;
import org.neo4j.io.pagecache.impl.muninn.MuninnPageCursor;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.storageengine.api.TransactionIdStore;
import org.neo4j.test.extension.Inject;

import static com.neo4j.test.causalclustering.ClusterConfig.clusterConfig;
import static java.util.concurrent.TimeUnit.MINUTES;
import static org.neo4j.configuration.SettingValueParsers.TRUE;
import static org.neo4j.test.assertion.Assert.assertEventually;

@ClusterExtension
class VersionContextTrackingIT
{
    private static final int NUMBER_OF_TRANSACTIONS = 3;

    @Inject
    private ClusterFactory clusterFactory;

    private Cluster cluster;

    @BeforeAll
    void beforeAll() throws Exception
    {
        var clusterConfig = clusterConfig()
                .withSharedCoreParam( GraphDatabaseSettings.snapshot_query, TRUE )
                .withSharedReadReplicaParam( GraphDatabaseSettings.snapshot_query, TRUE );

        cluster = clusterFactory.createCluster( clusterConfig );
        cluster.start();
    }

    @Test
    void coreMemberTransactionIdPageTracking() throws Exception
    {
        long baseTxId = getBaseTransactionId();
        for ( int i = 1; i < 4; i++ )
        {
            generateData();
            long expectedLatestPageVersion = getExpectedLatestPageVersion( baseTxId, i );
            Callable<Long> anyCoreSupplier = () -> getLatestPageVersion( getAnyCore() );
            assertEventually( "Any core page version should match to expected page version.", anyCoreSupplier,
                    value -> value >= expectedLatestPageVersion, 2, MINUTES );
        }
    }

    @Test
    void readReplicatesTransactionIdPageTracking() throws Exception
    {
        long baseTxId = getBaseTransactionId();
        for ( int i = 1; i < 4; i++ )
        {
            generateData();
            long expectedLatestPageVersion = getExpectedLatestPageVersion( baseTxId, i );
            Callable<Long> replicateVersionSupplier = () -> getLatestPageVersion( getAnyReadReplica() );
            assertEventually( "Read replica page version should match to core page version.", replicateVersionSupplier,
                    value -> value >= expectedLatestPageVersion, 2, MINUTES );
        }
    }

    private static long getExpectedLatestPageVersion( long baseTxId, int round )
    {
        return baseTxId + round * NUMBER_OF_TRANSACTIONS;
    }

    private long getBaseTransactionId()
    {
        DependencyResolver dependencyResolver = getAnyCore().getDependencyResolver();
        TransactionIdStore transactionIdStore = dependencyResolver.resolveDependency( TransactionIdStore.class );
        return transactionIdStore.getLastClosedTransactionId();
    }

    private CoreClusterMember anyCoreClusterMember()
    {
        return cluster.coreMembers().iterator().next();
    }

    private GraphDatabaseFacade getAnyCore()
    {
        return anyCoreClusterMember().defaultDatabase();
    }

    private GraphDatabaseFacade getAnyReadReplica()
    {
        return cluster.findAnyReadReplica().defaultDatabase();
    }

    private static long getLatestPageVersion( GraphDatabaseAPI databaseFacade ) throws IOException
    {
        DependencyResolver dependencyResolver = databaseFacade.getDependencyResolver();
        PageCache pageCache = dependencyResolver.resolveDependency( PageCache.class );
        NeoStores neoStores = dependencyResolver.resolveDependency( RecordStorageEngine.class ).testAccessNeoStores();
        File storeFile = neoStores.getNodeStore().getStorageFile();
        long maxTransactionId = Long.MIN_VALUE;
        PagedFile pageFile = pageCache.getExistingMapping( storeFile ).get();
        long lastPageId = pageFile.getLastPageId();
        for ( int i = 0; i <= lastPageId; i++ )
        {
            try ( MuninnPageCursor pageCursor = (MuninnPageCursor) pageFile.io( i, PagedFile.PF_SHARED_READ_LOCK, PageCursorTracer.NULL )  )
            {
                if ( pageCursor.next() )
                {
                    maxTransactionId = Math.max( maxTransactionId, pageCursor.lastTxModifierId() );
                }
            }
        }
        return maxTransactionId;
    }

    private void generateData() throws Exception
    {
        for ( int i = 0; i < NUMBER_OF_TRANSACTIONS; i++ )
        {
            cluster.coreTx( ( coreGraphDatabase, transaction ) ->
            {
                transaction.createNode();
                transaction.commit();
            } );
        }
    }
}
