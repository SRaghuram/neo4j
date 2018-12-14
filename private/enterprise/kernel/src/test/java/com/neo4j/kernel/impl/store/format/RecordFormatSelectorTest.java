/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.kernel.impl.store.format;

import com.neo4j.kernel.impl.store.format.highlimit.HighLimit;
import com.neo4j.kernel.impl.store.format.highlimit.v340.HighLimitV3_4_0;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;

import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.graphdb.mockfs.EphemeralFileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.store.MetaDataStore;
import org.neo4j.kernel.impl.store.format.RecordFormats;
import org.neo4j.kernel.impl.store.format.standard.Standard;
import org.neo4j.kernel.impl.store.format.standard.StandardV3_4;
import org.neo4j.kernel.impl.store.format.standard.StandardV4_0;
import org.neo4j.logging.LogProvider;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.pagecache.EphemeralPageCacheExtension;
import org.neo4j.test.rule.TestDirectory;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.kernel.impl.store.MetaDataStore.Position.STORE_VERSION;
import static org.neo4j.kernel.impl.store.format.RecordFormatSelector.defaultFormat;
import static org.neo4j.kernel.impl.store.format.RecordFormatSelector.findSuccessor;
import static org.neo4j.kernel.impl.store.format.RecordFormatSelector.selectForConfig;
import static org.neo4j.kernel.impl.store.format.RecordFormatSelector.selectForStore;
import static org.neo4j.kernel.impl.store.format.RecordFormatSelector.selectForStoreOrConfig;
import static org.neo4j.kernel.impl.store.format.RecordFormatSelector.selectForVersion;
import static org.neo4j.kernel.impl.store.format.RecordFormatSelector.selectNewestFormat;

@EphemeralPageCacheExtension
class RecordFormatSelectorTest
{
    private static final LogProvider LOG = NullLogProvider.getInstance();

    @Inject
    private EphemeralFileSystemAbstraction fs;
    @Inject
    private TestDirectory testDirectory;
    @Inject
    private PageCache pageCache;

    @Test
    void defaultFormatTest()
    {
        assertSame( Standard.LATEST_RECORD_FORMATS, defaultFormat() );
    }

    @Test
    void selectForVersionTest()
    {
        assertSame( StandardV3_4.RECORD_FORMATS, selectForVersion( StandardV3_4.STORE_VERSION ) );
        assertSame( StandardV4_0.RECORD_FORMATS, selectForVersion( StandardV4_0.STORE_VERSION ) );
         assertSame( HighLimitV3_4_0.RECORD_FORMATS, selectForVersion( HighLimitV3_4_0.STORE_VERSION ) );
        assertSame( HighLimit.RECORD_FORMATS, selectForVersion( HighLimit.STORE_VERSION ) );
    }

    @Test
    void selectForWrongVersionTest()
    {
        assertThrows( IllegalArgumentException.class, () -> selectForVersion( "vA.B.9" ) );
    }

    @Test
    void selectForConfigWithRecordFormatParameter()
    {
        assertSame( Standard.LATEST_RECORD_FORMATS, selectForConfig( config( Standard.LATEST_NAME ), LOG ) );
        assertSame( HighLimit.RECORD_FORMATS, selectForConfig( config( HighLimit.NAME ), LOG ) );
    }

    @Test
    void selectForConfigWithoutRecordFormatParameter()
    {
        assertSame( defaultFormat(), selectForConfig( Config.defaults(), LOG ) );
    }

    @Test
    void selectForConfigWithWrongRecordFormatParameter()
    {
        assertThrows( IllegalArgumentException.class, () -> selectForConfig( config( "unknown_format" ), LOG ) );
    }

    @Test
    void selectForStoreWithValidStore() throws IOException
    {
        verifySelectForStore( pageCache, StandardV3_4.RECORD_FORMATS );
        verifySelectForStore( pageCache, StandardV4_0.RECORD_FORMATS );
        verifySelectForStore( pageCache, HighLimitV3_4_0.RECORD_FORMATS );
        verifySelectForStore( pageCache, HighLimit.RECORD_FORMATS );
    }

    @Test
    void selectForStoreWithNoStore()
    {
        assertNull( selectForStore( testDirectory.databaseLayout(), fs, pageCache, LOG ) );
    }

    @Test
    void selectForStoreWithThrowingPageCache() throws IOException
    {
        createNeoStoreFile();
        PageCache pageCache = mock( PageCache.class );
        when( pageCache.pageSize() ).thenReturn( PageCache.PAGE_SIZE );
        when( pageCache.map( any(), anyInt(), any() ) ).thenThrow( new IOException( "No reading..." ) );
        assertNull( selectForStore( testDirectory.databaseLayout(), fs, pageCache, LOG ) );
        verify( pageCache ).map( any(), anyInt(), any() );
    }

    @Test
    void selectForStoreWithInvalidStoreVersion() throws IOException
    {
        prepareNeoStoreFile( "v9.Z.9", pageCache );
        assertNull( selectForStore( testDirectory.databaseLayout(), fs, this.pageCache, LOG ) );
    }

    @Test
    void selectForStoreOrConfigWithSameStandardConfiguredAndStoredFormat() throws IOException
    {
        prepareNeoStoreFile( Standard.LATEST_STORE_VERSION, pageCache );

        Config config = config( Standard.LATEST_NAME );

        assertSame( Standard.LATEST_RECORD_FORMATS, selectForStoreOrConfig( config, testDirectory.databaseLayout(), fs, pageCache, LOG ) );
    }

    @Test
    void selectForStoreOrConfigWithSameHighLimitConfiguredAndStoredFormat() throws IOException
    {
        prepareNeoStoreFile( HighLimit.STORE_VERSION, pageCache );

        Config config = config( HighLimit.NAME );

        assertSame( HighLimit.RECORD_FORMATS, selectForStoreOrConfig( config, testDirectory.databaseLayout(), fs, pageCache, LOG ) );
    }

    @Test
    void selectForStoreOrConfigWithDifferentlyConfiguredAndStoredFormat() throws IOException
    {
        prepareNeoStoreFile( Standard.LATEST_STORE_VERSION, pageCache );

        Config config = config( HighLimit.NAME );

        assertThrows( IllegalArgumentException.class, () -> selectForStoreOrConfig( config, testDirectory.databaseLayout(), fs, pageCache, LOG ) );
    }

    @Test
    void selectForStoreOrConfigWithOnlyStandardStoredFormat() throws IOException
    {
        prepareNeoStoreFile( Standard.LATEST_STORE_VERSION, pageCache );

        Config config = Config.defaults();

        assertSame( Standard.LATEST_RECORD_FORMATS, selectForStoreOrConfig( config, testDirectory.databaseLayout(), fs, pageCache, LOG ) );
    }

    @Test
    void selectForStoreOrConfigWithOnlyHighLimitStoredFormat() throws IOException
    {
        prepareNeoStoreFile( HighLimit.STORE_VERSION, pageCache );

        Config config = Config.defaults();

        assertSame( HighLimit.RECORD_FORMATS, selectForStoreOrConfig( config, testDirectory.databaseLayout(), fs, pageCache, LOG ) );
    }

    @Test
    void selectForStoreOrConfigWithOnlyStandardConfiguredFormat()
    {
        Config config = config( Standard.LATEST_NAME );

        assertSame( Standard.LATEST_RECORD_FORMATS, selectForStoreOrConfig( config, testDirectory.databaseLayout(), fs, pageCache, LOG ) );
    }

    @Test
    void selectForStoreOrConfigWithOnlyHighLimitConfiguredFormat()
    {
        Config config = config( HighLimit.NAME );

        assertSame( HighLimit.RECORD_FORMATS, selectForStoreOrConfig( config, testDirectory.databaseLayout(), fs, pageCache, LOG ) );
    }

    @Test
    void selectForStoreOrConfigWithWrongConfiguredFormat()
    {
        Config config = config( "unknown_format" );

        assertEquals( defaultFormat(), selectForStoreOrConfig( config, testDirectory.databaseLayout(), fs, pageCache, LOG ) );
    }

    @Test
    void selectForStoreOrConfigWithoutConfiguredAndStoredFormats()
    {
        assertSame( defaultFormat(), selectForStoreOrConfig( Config.defaults(), testDirectory.databaseLayout(), fs, pageCache, LOG ) );
    }

    @Test
    void selectNewestFormatWithConfiguredStandardFormat()
    {
        assertSame( Standard.LATEST_RECORD_FORMATS,
                selectNewestFormat( config( Standard.LATEST_NAME ), testDirectory.databaseLayout(), fs, pageCache, LOG ) );
    }

    @Test
    void selectNewestFormatWithConfiguredHighLimitFormat()
    {
        assertSame( HighLimit.RECORD_FORMATS,
                selectNewestFormat( config( HighLimit.NAME ), testDirectory.databaseLayout(), fs, pageCache, LOG ) );
    }

    @Test
    void selectNewestFormatWithWrongConfiguredFormat()
    {
        assertThrows( IllegalArgumentException.class, () ->
                selectNewestFormat( config( "unknown_format" ), testDirectory.databaseLayout(), fs, pageCache, LOG ) );
    }

    @Test
    void selectNewestFormatWithoutConfigAndStore()
    {
        assertSame( defaultFormat(), selectNewestFormat( Config.defaults(), testDirectory.databaseLayout(), fs, pageCache, LOG ) );
    }

    @Test
    void selectNewestFormatForExistingStandardStore() throws IOException
    {
        prepareNeoStoreFile( Standard.LATEST_STORE_VERSION, pageCache );

        Config config = Config.defaults();

        assertSame( Standard.LATEST_RECORD_FORMATS, selectNewestFormat( config, testDirectory.databaseLayout(), fs, this.pageCache, LOG ) );
    }

    @Test
    void selectNewestFormatForExistingHighLimitStore() throws IOException
    {
        prepareNeoStoreFile( HighLimit.STORE_VERSION, pageCache );

        Config config = Config.defaults();

        assertSame( HighLimit.RECORD_FORMATS, selectNewestFormat( config, testDirectory.databaseLayout(), fs, this.pageCache, LOG ) );
    }

    @Test
    void selectNewestFormatForExistingStoreWithLegacyFormat() throws IOException
    {
        prepareNeoStoreFile( StandardV3_4.STORE_VERSION, pageCache );

        Config config = Config.defaults();

        assertSame( defaultFormat(), selectNewestFormat( config, testDirectory.databaseLayout(), fs, this.pageCache, LOG ) );
    }

    @Test
    void findSuccessorLatestVersion()
    {
        assertFalse( findSuccessor( defaultFormat() ).isPresent() );
    }

    @Test
    void findSuccessorToOlderVersion()
    {
        assertEquals( StandardV4_0.RECORD_FORMATS, findSuccessor( StandardV3_4.RECORD_FORMATS ).get() );

        assertEquals( HighLimit.RECORD_FORMATS, findSuccessor( HighLimitV3_4_0.RECORD_FORMATS ).get() );
    }

    private void verifySelectForStore( PageCache pageCache, RecordFormats format ) throws IOException
    {
        prepareNeoStoreFile( format.storeVersion(), pageCache );
        assertSame( format, selectForStore( testDirectory.databaseLayout(), fs, pageCache, LOG ) );
    }

    private void prepareNeoStoreFile( String storeVersion, PageCache pageCache ) throws IOException
    {
        File neoStoreFile = createNeoStoreFile();
        long value = MetaDataStore.versionStringToLong( storeVersion );
        MetaDataStore.setRecord( pageCache, neoStoreFile, STORE_VERSION, value );
    }

    private File createNeoStoreFile() throws IOException
    {
        File neoStoreFile = testDirectory.databaseLayout().metadataStore();
        fs.create( neoStoreFile ).close();
        return neoStoreFile;
    }

    private static Config config( String recordFormatName )
    {
        return Config.defaults( GraphDatabaseSettings.record_format, recordFormatName );
    }
}
