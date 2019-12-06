/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.kernel.impl;

import org.apache.commons.io.IOUtils;
import com.neo4j.test.TestEnterpriseDatabaseManagementServiceBuilder;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.neo4j.collection.Dependencies;
import org.neo4j.common.DependencyResolver;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Resource;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.index.label.LabelScanStore;
import org.neo4j.internal.recordstorage.RecordStorageEngine;
import org.neo4j.io.fs.EphemeralFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.EphemeralPageSwapperFactory;
import org.neo4j.io.pagecache.ExternallyManagedPageCache;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.PageEvictionCallback;
import org.neo4j.io.pagecache.PageSwapper;
import org.neo4j.io.pagecache.impl.SingleFilePageSwapperFactory;
import org.neo4j.io.pagecache.impl.muninn.StandalonePageCacheFactory;
import org.neo4j.kernel.impl.scheduler.JobSchedulerFactory;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.store.NodeStore;
import org.neo4j.kernel.impl.store.PropertyStore;
import org.neo4j.kernel.impl.store.RelationshipStore;
import org.neo4j.kernel.impl.transaction.log.checkpoint.CheckPointerImpl;
import org.neo4j.kernel.impl.transaction.log.checkpoint.SimpleTriggerInfo;
import org.neo4j.kernel.impl.transaction.log.checkpoint.StoreCopyCheckPointMutex;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.EphemeralTestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;

@EphemeralTestDirectoryExtension
class ModifyingFixedFormatRecordsTest
{
    @Inject
    TestDirectory dir;
    @Inject
    EphemeralFileSystemAbstraction fs;

    @Test
    void beforeImagesOfRecordsMustRetainFixedFormatBit() throws Exception
    {
        long nodeId = 2042845695L;
        long propId = 20131230644L;
        long relId1 = 2223419287L;
        long relId2 = 8889490582L;

        FileSystemAbstraction crashFs;
        File labelScanStore;

        try ( AlmostEphemeralPageSwapperFactory swapperFactory = new AlmostEphemeralPageSwapperFactory();
              JobScheduler scheduler = JobSchedulerFactory.createInitialisedScheduler();
              PageCache pageCache = StandalonePageCacheFactory.createPageCache( swapperFactory, scheduler ) )
        {
            swapperFactory.open( fs );
            Dependencies deps = new Dependencies();
            deps.satisfyDependencies( new ExternallyManagedPageCache( pageCache ), scheduler );
            TestEnterpriseDatabaseManagementServiceBuilder serviceBuilder = new TestEnterpriseDatabaseManagementServiceBuilder();
            serviceBuilder.impermanent();
            serviceBuilder.setFileSystem( fs );
            serviceBuilder.setExternalDependencies( deps );
            serviceBuilder.setConfig( GraphDatabaseSettings.record_format, "high_limit" );
            DatabaseManagementService dbms = serviceBuilder.build();

            GraphDatabaseService db = dbms.database( DEFAULT_DATABASE_NAME );
            try
            {
                GraphDatabaseAPI api = (GraphDatabaseAPI) db;
                DependencyResolver resolver = api.getDependencyResolver();
                CheckPointerImpl checkPointer = resolver.resolveTypeDependencies( CheckPointerImpl.class ).iterator().next();
                StoreCopyCheckPointMutex mutex = resolver.resolveTypeDependencies( StoreCopyCheckPointMutex.class ).iterator().next();
                RecordStorageEngine engine = resolver.resolveTypeDependencies( RecordStorageEngine.class ).iterator().next();
                NeoStores neoStores = engine.testAccessNeoStores();

                NodeStore nodeStore = neoStores.getNodeStore();
                RelationshipStore relationshipStore = neoStores.getRelationshipStore();
                PropertyStore propertyStore = neoStores.getPropertyStore();

                // Prepare the ids of the records we wish to create.
                nodeStore.setHighId( nodeId );
                relationshipStore.setHighId( relId1 );
                propertyStore.setHighId( propId );
                try ( Transaction tx = db.beginTx() )
                {
                    Node node = tx.createNode( Label.label( "a" ) );
                    node.setProperty( "a", 1 );
                    assertEquals( nodeId, node.getId() );
                    Relationship rel = node.createRelationshipTo( node, RelationshipType.withName( "a" ) );
                    assertEquals( relId1, rel.getId() );
                    tx.commit();
                }

                // Place a check point for recovery.
                checkPointer.checkPointIfNeeded( new SimpleTriggerInfo( "Recovery point for the text." ) );

                // Prevent the transaction we wish to crash from being check-pointed.
                try ( Resource ignore = mutex.checkPoint() )
                {
                    // Add another relationship to that node.
                    // In this transaction the node will have both a before and an after image.
                    relationshipStore.setHighId( relId2 );
                    try ( Transaction tx = api.beginTx() )
                    {
                        Node node = tx.getNodeById( nodeId );
                        node.createRelationshipTo( node, RelationshipType.withName( "a" ) );
                        tx.commit();
                    }
                    // Simulate a crash and force recovery next time we start.
                    crashFs = fs.snapshot();
                    LabelScanStore lss = resolver.resolveTypeDependencies( LabelScanStore.class ).iterator().next();
                    labelScanStore = lss.getLabelScanStoreFile();
                }
            }
            finally
            {
                dbms.shutdown();
            }

            copyToCrash( crashFs, labelScanStore ); // Preserve the label scan store in the crash, so we don't have to do a node store scan to rebuild it.
            serviceBuilder.setFileSystem( crashFs );
            swapperFactory.open( crashFs );

            // Now recovery must not throw.
            dbms = serviceBuilder.build();
            dbms.database( DEFAULT_DATABASE_NAME );
            dbms.shutdown();
        }
    }

    private void copyToCrash( FileSystemAbstraction crashFs, File file ) throws IOException
    {
        try ( OutputStream out = crashFs.openAsOutputStream( file, false );
              InputStream in = fs.openAsInputStream( file ) )
        {
            IOUtils.copy( in, out );
        }
    }

    private static class AlmostEphemeralPageSwapperFactory extends EphemeralPageSwapperFactory
    {
        private SingleFilePageSwapperFactory alternativeFactory;

        @Override
        public PageSwapper createPageSwapper( File file, int filePageSize, PageEvictionCallback onEviction, boolean createIfNotExist,
                boolean noChannelStriping, boolean useDirectIO ) throws IOException
        {
            if ( file.getName().endsWith( "counts.db.a" ) || file.getName().endsWith( "counts.db.b" ) )
            {
                return alternativeFactory.createPageSwapper( file, filePageSize, onEviction, createIfNotExist, noChannelStriping, useDirectIO );
            }
            return super.createPageSwapper( file, filePageSize, onEviction, createIfNotExist, noChannelStriping, useDirectIO );
        }

        public void open( FileSystemAbstraction fs )
        {
            alternativeFactory = new SingleFilePageSwapperFactory( fs );
        }
    }
}
