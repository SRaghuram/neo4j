/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.jmx.impl;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

import org.neo4j.graphdb.mockfs.EphemeralFileSystemAbstraction;
import org.neo4j.internal.kernel.api.schema.IndexProviderDescriptor;
import org.neo4j.io.fs.StoreChannel;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.jmx.StoreSize;
import org.neo4j.kernel.NeoStoreDataSource;
import org.neo4j.kernel.api.index.IndexDirectoryStructure;
import org.neo4j.kernel.api.index.IndexProvider;
import org.neo4j.kernel.api.labelscan.LabelScanStore;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.api.ExplicitIndexProvider;
import org.neo4j.kernel.impl.transaction.log.files.LogFiles;
import org.neo4j.kernel.impl.transaction.log.files.LogFilesBuilder;
import org.neo4j.kernel.impl.transaction.state.DataSourceManager;
import org.neo4j.kernel.impl.transaction.state.DefaultIndexProviderMap;
import org.neo4j.kernel.impl.util.Dependencies;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.kernel.internal.KernelData;
import org.neo4j.kernel.spi.explicitindex.IndexImplementation;
import org.neo4j.test.extension.EphemeralFileSystemExtension;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.neo4j.helpers.collection.Iterables.iterable;
import static org.neo4j.kernel.impl.store.StoreFile.COUNTS_STORE_LEFT;
import static org.neo4j.kernel.impl.store.StoreFile.COUNTS_STORE_RIGHT;
import static org.neo4j.kernel.impl.store.StoreFile.LABEL_TOKEN_NAMES_STORE;
import static org.neo4j.kernel.impl.store.StoreFile.LABEL_TOKEN_STORE;
import static org.neo4j.kernel.impl.store.StoreFile.NODE_LABEL_STORE;
import static org.neo4j.kernel.impl.store.StoreFile.NODE_STORE;
import static org.neo4j.kernel.impl.store.StoreFile.PROPERTY_ARRAY_STORE;
import static org.neo4j.kernel.impl.store.StoreFile.PROPERTY_KEY_TOKEN_NAMES_STORE;
import static org.neo4j.kernel.impl.store.StoreFile.PROPERTY_KEY_TOKEN_STORE;
import static org.neo4j.kernel.impl.store.StoreFile.PROPERTY_STORE;
import static org.neo4j.kernel.impl.store.StoreFile.PROPERTY_STRING_STORE;
import static org.neo4j.kernel.impl.store.StoreFile.RELATIONSHIP_GROUP_STORE;
import static org.neo4j.kernel.impl.store.StoreFile.RELATIONSHIP_STORE;
import static org.neo4j.kernel.impl.store.StoreFile.RELATIONSHIP_TYPE_TOKEN_NAMES_STORE;
import static org.neo4j.kernel.impl.store.StoreFile.RELATIONSHIP_TYPE_TOKEN_STORE;
import static org.neo4j.kernel.impl.store.StoreFile.SCHEMA_STORE;
import static org.neo4j.kernel.impl.storemigration.StoreFileType.ID;
import static org.neo4j.kernel.impl.storemigration.StoreFileType.STORE;

@ExtendWith( {EphemeralFileSystemExtension.class, TestDirectoryExtension.class} )
class StoreSizeBeanTest
{
    @Inject
    private EphemeralFileSystemAbstraction fs;
    @Inject
    private TestDirectory testDirectory;
    private final ExplicitIndexProvider explicitIndexProviderLookup = mock( ExplicitIndexProvider.class );
    private final IndexProvider indexProvider = mockedIndexProvider( "provider1" );
    private final IndexProvider indexProvider2 = mockedIndexProvider( "provider2" );
    private final LabelScanStore labelScanStore = mock( LabelScanStore.class );
    private StoreSize storeSizeBean;
    private LogFiles logFiles;

    @BeforeEach
    void setUp() throws IOException
    {
        logFiles = LogFilesBuilder.logFilesBasedOnlyBuilder( testDirectory.databaseDir(), fs ).build();

        Dependencies dependencies = new Dependencies();
        DataSourceManager dataSourceManager = new DataSourceManager();
        GraphDatabaseAPI db = mock( GraphDatabaseAPI.class );
        NeoStoreDataSource dataSource = mock( NeoStoreDataSource.class );

        dependencies.satisfyDependency( indexProvider );
        dependencies.satisfyDependency( indexProvider2 );

        DefaultIndexProviderMap indexProviderMap = new DefaultIndexProviderMap( dependencies );
        indexProviderMap.init();

        // Setup all dependencies
        dependencies.satisfyDependency( fs );
        dependencies.satisfyDependencies( dataSourceManager );
        dependencies.satisfyDependency( logFiles );
        dependencies.satisfyDependency( explicitIndexProviderLookup );
        dependencies.satisfyDependency( indexProviderMap );
        dependencies.satisfyDependency( labelScanStore );
        when( db.getDependencyResolver() ).thenReturn( dependencies );
        when( dataSource.getDependencyResolver() ).thenReturn( dependencies );
        when( dataSource.getDatabaseLayout() ).thenReturn( testDirectory.databaseLayout() );

        // Start DataSourceManager
        dataSourceManager.register( dataSource );
        dataSourceManager.start();

        // Create bean
        KernelData kernelData = new KernelData( fs, mock( PageCache.class ), testDirectory.databaseDir(), Config.defaults(), dataSourceManager );
        ManagementData data = new ManagementData( new StoreSizeBean(), kernelData, ManagementSupport.load() );
        storeSizeBean = (StoreSize) new StoreSizeBean().createMBean( data );
    }

    private static IndexProvider mockedIndexProvider( String name )
    {
        IndexProvider provider = mock( IndexProvider.class );
        when( provider.getProviderDescriptor() ).thenReturn( new IndexProviderDescriptor( name, "1" ) );
        return provider;
    }

    private void createFakeStoreDirectory() throws IOException
    {
        Map<String,Integer> dummyStore = new HashMap<>();
        dummyStore.put( NODE_STORE.fileName( STORE ), 1 );
        dummyStore.put( NODE_STORE.fileName( ID ), 2 );
        dummyStore.put( NODE_LABEL_STORE.fileName( STORE ), 3 );
        dummyStore.put( NODE_LABEL_STORE.fileName( ID ), 4 );
        dummyStore.put( PROPERTY_STORE.fileName( STORE ), 5 );
        dummyStore.put( PROPERTY_STORE.fileName( ID ), 6 );
        dummyStore.put( PROPERTY_KEY_TOKEN_STORE.fileName( STORE ), 7 );
        dummyStore.put( PROPERTY_KEY_TOKEN_STORE.fileName( ID ), 8 );
        dummyStore.put( PROPERTY_KEY_TOKEN_NAMES_STORE.fileName( STORE ), 9 );
        dummyStore.put( PROPERTY_KEY_TOKEN_NAMES_STORE.fileName( ID ), 10 );
        dummyStore.put( PROPERTY_STRING_STORE.fileName( STORE ), 11 );
        dummyStore.put( PROPERTY_STRING_STORE.fileName( ID ), 12 );
        dummyStore.put( PROPERTY_ARRAY_STORE.fileName( STORE ), 13 );
        dummyStore.put( PROPERTY_ARRAY_STORE.fileName( ID ), 14 );
        dummyStore.put( RELATIONSHIP_STORE.fileName( STORE ), 15 );
        dummyStore.put( RELATIONSHIP_STORE.fileName( ID ), 16 );
        dummyStore.put( RELATIONSHIP_GROUP_STORE.fileName( STORE ), 17 );
        dummyStore.put( RELATIONSHIP_GROUP_STORE.fileName( ID ), 18 );
        dummyStore.put( RELATIONSHIP_TYPE_TOKEN_STORE.fileName( STORE ), 19 );
        dummyStore.put( RELATIONSHIP_TYPE_TOKEN_STORE.fileName( ID ), 20 );
        dummyStore.put( RELATIONSHIP_TYPE_TOKEN_NAMES_STORE.fileName( STORE ), 21 );
        dummyStore.put( RELATIONSHIP_TYPE_TOKEN_NAMES_STORE.fileName( ID ), 22 );
        dummyStore.put( LABEL_TOKEN_STORE.fileName( STORE ), 23 );
        dummyStore.put( LABEL_TOKEN_STORE.fileName( ID ), 24 );
        dummyStore.put( LABEL_TOKEN_NAMES_STORE.fileName( STORE ), 25 );
        dummyStore.put( LABEL_TOKEN_NAMES_STORE.fileName( ID ), 26 );
        dummyStore.put( SCHEMA_STORE.fileName( STORE ), 27 );
        dummyStore.put( SCHEMA_STORE.fileName( ID ), 28 );
        dummyStore.put( COUNTS_STORE_LEFT.fileName( STORE ), 29 );
        // COUNTS_STORE_RIGHT is created in the test

        DatabaseLayout databaseLayout = testDirectory.databaseLayout();
        for ( Map.Entry<String,Integer> dummyFile : dummyStore.entrySet() )
        {
            createFileOfSize( databaseLayout.file( dummyFile.getKey() ), dummyFile.getValue() );
        }
    }

    @Test
    void verifyGroupingOfNodeRelatedFiles() throws Exception
    {
        createFakeStoreDirectory();
        assertEquals( getExpected(1, 4 ), storeSizeBean.getNodeStoreSize() );
    }

    @Test
    void verifyGroupingOfPropertyRelatedFiles() throws Exception
    {
        createFakeStoreDirectory();
        assertEquals( getExpected( 5, 10 ), storeSizeBean.getPropertyStoreSize() );
    }

    @Test
    void verifyGroupingOfStringRelatedFiles() throws Exception
    {
        createFakeStoreDirectory();
        assertEquals( getExpected(11, 12 ), storeSizeBean.getStringStoreSize() );
    }

    @Test
    void verifyGroupingOfArrayRelatedFiles() throws Exception
    {
        createFakeStoreDirectory();
        assertEquals( getExpected(13, 14 ), storeSizeBean.getArrayStoreSize() );
    }

    @Test
    void verifyGroupingOfRelationshipRelatedFiles() throws Exception
    {
        createFakeStoreDirectory();
        assertEquals( getExpected( 15, 22 ), storeSizeBean.getRelationshipStoreSize() );
    }

    @Test
    void verifyGroupingOfLabelRelatedFiles() throws Exception
    {
        createFakeStoreDirectory();
        assertEquals( getExpected( 23, 26 ), storeSizeBean.getLabelStoreSize() );
    }

    @Test
    void verifyGroupingOfCountStoreRelatedFiles() throws Exception
    {
        createFakeStoreDirectory();
        assertEquals( getExpected( 29, 29), storeSizeBean.getCountStoreSize() );
        createFileOfSize( testDirectory.databaseLayout().file( COUNTS_STORE_RIGHT.fileName( STORE ) ), 30 );
        assertEquals( getExpected( 29, 30), storeSizeBean.getCountStoreSize() );
    }

    @Test
    void verifyGroupingOfSchemaRelatedFiles() throws Exception
    {
        createFakeStoreDirectory();
        assertEquals( getExpected( 27, 28 ), storeSizeBean.getSchemaStoreSize() );
    }

    @Test
    void sumAllFiles() throws Exception
    {
        createFakeStoreDirectory();
        assertEquals( getExpected( 0, 29 ), storeSizeBean.getTotalStoreSize() );
    }

    @Test
    void shouldCountAllLogFiles() throws Throwable
    {
        createFileOfSize( logFiles.getLogFileForVersion( 0 ), 1 );
        createFileOfSize( logFiles.getLogFileForVersion( 1 ), 2 );

        assertEquals( 3L, storeSizeBean.getTransactionLogsSize() );
    }

    @Test
    void shouldCountAllIndexFiles() throws Exception
    {
        // Explicit index file
        File explicitIndex = testDirectory.databaseLayout().file( "explicitIndex" );
        createFileOfSize( explicitIndex, 1 );

        IndexImplementation indexImplementation = mock( IndexImplementation.class );
        when( indexImplementation.getIndexImplementationDirectory( any() ) ).thenReturn( explicitIndex );
        when( explicitIndexProviderLookup.allIndexProviders() ).thenReturn( iterable( indexImplementation ) );

        // Schema index files
        File schemaIndex = testDirectory.databaseLayout().file("schemaIndex" );
        createFileOfSize( schemaIndex, 2 );
        IndexDirectoryStructure directoryStructure = mock( IndexDirectoryStructure.class );
        when( directoryStructure.rootDirectory() ).thenReturn( schemaIndex );
        when( indexProvider.directoryStructure() ).thenReturn( directoryStructure );

        File schemaIndex2 = testDirectory.databaseLayout().file("schemaIndex2" );
        createFileOfSize( schemaIndex2, 3 );
        IndexDirectoryStructure directoryStructure2 = mock( IndexDirectoryStructure.class );
        when( directoryStructure2.rootDirectory() ).thenReturn( schemaIndex2 );
        when( indexProvider2.directoryStructure() ).thenReturn( directoryStructure2 );

        // Label scan store
        File labelScan = testDirectory.databaseLayout().file("labelScanStore" );
        createFileOfSize( labelScan, 4 );
        when( labelScanStore.getLabelScanStoreFile() ).thenReturn( labelScan );

        // Count all files
        assertEquals( 10, storeSizeBean.getIndexStoreSize() );
    }

    private void createFileOfSize( File file, int size ) throws IOException
    {
        try ( StoreChannel storeChannel = fs.create( file ) )
        {
            byte[] bytes = new byte[size];
            ByteBuffer buffer = ByteBuffer.wrap( bytes );
            storeChannel.writeAll( buffer );
        }
    }

    private static long getExpected( int lower, int upper )
    {
        long expected = 0;
        for ( int i = lower; i <= upper; i++ )
        {
            expected += i;
        }
        return expected;
    }
}
