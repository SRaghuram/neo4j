/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.kernel.impl.store.format;

import com.neo4j.kernel.impl.store.format.highlimit.HighLimit;
import com.neo4j.kernel.impl.store.format.highlimit.v300.HighLimitV3_0_0;
import com.neo4j.kernel.impl.store.format.highlimit.v306.HighLimitV3_0_6;
import com.neo4j.kernel.impl.store.format.highlimit.v310.HighLimitV3_1_0;
import com.neo4j.kernel.impl.store.format.highlimit.v320.HighLimitV3_2_0;
import com.neo4j.kernel.impl.store.format.highlimit.v340.HighLimitV3_4_0;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.io.File;
import java.io.IOException;

import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.io.fs.EphemeralFileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.tracing.DefaultPageCacheTracer;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer;
import org.neo4j.kernel.impl.store.MetaDataStore;
import org.neo4j.kernel.impl.store.format.RecordFormatSelector;
import org.neo4j.kernel.impl.store.format.RecordFormats;
import org.neo4j.kernel.impl.store.format.StoreVersion;
import org.neo4j.kernel.impl.store.format.standard.Standard;
import org.neo4j.kernel.impl.store.format.standard.StandardV3_4;
import org.neo4j.kernel.impl.store.format.standard.StandardV4_0;
import org.neo4j.logging.LogProvider;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.test.extension.EphemeralNeo4jLayoutExtension;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.pagecache.EphemeralPageCacheExtension;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.params.provider.EnumSource.Mode.MATCH_ANY;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.io.pagecache.tracing.PageCacheTracer.NULL;
import static org.neo4j.kernel.impl.store.MetaDataStoreInterface.Position.STORE_VERSION;
import static org.neo4j.kernel.impl.store.format.RecordFormatSelector.defaultFormat;
import static org.neo4j.kernel.impl.store.format.RecordFormatSelector.findSuccessor;
import static org.neo4j.kernel.impl.store.format.RecordFormatSelector.selectForConfig;
import static org.neo4j.kernel.impl.store.format.RecordFormatSelector.selectForStore;
import static org.neo4j.kernel.impl.store.format.RecordFormatSelector.selectForStoreOrConfig;
import static org.neo4j.kernel.impl.store.format.RecordFormatSelector.selectForVersion;
import static org.neo4j.kernel.impl.store.format.RecordFormatSelector.selectNewestFormat;

@EphemeralPageCacheExtension
@EphemeralNeo4jLayoutExtension
class RecordFormatSelectorTest
{
    private static final LogProvider LOG = NullLogProvider.getInstance();

    @Inject
    private EphemeralFileSystemAbstraction fs;
    @Inject
    private PageCache pageCache;
    @Inject
    private DatabaseLayout databaseLayout;

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
        assertSame( HighLimitV3_0_0.RECORD_FORMATS, selectForVersion( HighLimitV3_0_0.STORE_VERSION ) );
        assertSame( HighLimitV3_1_0.RECORD_FORMATS, selectForVersion( HighLimitV3_1_0.STORE_VERSION ) );
        assertSame( HighLimitV3_2_0.RECORD_FORMATS, selectForVersion( HighLimitV3_2_0.STORE_VERSION ) );
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
    void tracePageCacheAccessOnStoreSelection() throws IOException
    {
        prepareNeoStoreFile( Standard.LATEST_RECORD_FORMATS.storeVersion(), pageCache );
        var pageCacheTracer = new DefaultPageCacheTracer();
        selectForStore( databaseLayout, fs, pageCache, LOG, pageCacheTracer );

        assertThat( pageCacheTracer.pins() ).isEqualTo( 1 );
        assertThat( pageCacheTracer.unpins() ).isEqualTo( 1 );
        assertThat( pageCacheTracer.faults() ).isEqualTo( 1 );
    }

    @Test
    void tracePageCacheAccessOnStoreSelectionOrConfig() throws IOException
    {
        prepareNeoStoreFile( Standard.LATEST_RECORD_FORMATS.storeVersion(), pageCache );
        var pageCacheTracer = new DefaultPageCacheTracer();
        selectForStoreOrConfig( Config.defaults(), databaseLayout, fs, pageCache, LOG, pageCacheTracer );

        assertThat( pageCacheTracer.pins() ).isEqualTo( 1 );
        assertThat( pageCacheTracer.unpins() ).isEqualTo( 1 );
        assertThat( pageCacheTracer.faults() ).isEqualTo( 1 );
    }

    @Test
    void tracePageCacheAccessOnNewestStoreSelection() throws IOException
    {
        prepareNeoStoreFile( Standard.LATEST_RECORD_FORMATS.storeVersion(), pageCache );
        var pageCacheTracer = new DefaultPageCacheTracer();
        selectNewestFormat( Config.defaults(), databaseLayout, fs, pageCache, LOG, pageCacheTracer );

        assertThat( pageCacheTracer.pins() ).isEqualTo( 1 );
        assertThat( pageCacheTracer.unpins() ).isEqualTo( 1 );
        assertThat( pageCacheTracer.faults() ).isEqualTo( 1 );
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
        verifySelectForStore( pageCache, HighLimitV3_0_0.RECORD_FORMATS );
        verifySelectForStore( pageCache, HighLimitV3_0_6.RECORD_FORMATS );
        verifySelectForStore( pageCache, HighLimitV3_1_0.RECORD_FORMATS );
        verifySelectForStore( pageCache, HighLimitV3_2_0.RECORD_FORMATS );
        verifySelectForStore( pageCache, HighLimitV3_4_0.RECORD_FORMATS );
        verifySelectForStore( pageCache, HighLimit.RECORD_FORMATS );
    }

    @Test
    void selectForStoreWithNoStore()
    {
        assertNull( selectForStore( databaseLayout, fs, pageCache, LOG, NULL ) );
    }

    @Test
    void selectForStoreWithThrowingPageCache() throws IOException
    {
        createNeoStoreFile( databaseLayout );
        PageCache pageCache = mock( PageCache.class );
        when( pageCache.pageSize() ).thenReturn( PageCache.PAGE_SIZE );
        when( pageCache.map( any(), any(), anyInt(), any() ) ).thenThrow( new IOException( "No reading..." ) );
        assertNull( selectForStore( databaseLayout, fs, pageCache, LOG, NULL ) );
        verify( pageCache ).map( any(), any(), anyInt(), any() );
    }

    @Test
    void selectForStoreWithInvalidStoreVersion() throws IOException
    {
        prepareNeoStoreFile( "v9.Z.9", pageCache );
        assertNull( selectForStore( databaseLayout, fs, this.pageCache, LOG, NULL ) );
    }

    @Test
    void selectForStoreOrConfigWithSameStandardConfiguredAndStoredFormat() throws IOException
    {
        prepareNeoStoreFile( Standard.LATEST_STORE_VERSION, pageCache );

        Config config = config( Standard.LATEST_NAME );

        assertSame( Standard.LATEST_RECORD_FORMATS, selectForStoreOrConfig( config, databaseLayout, fs, pageCache, LOG, NULL ) );
    }

    @Test
    void selectForStoreOrConfigWithSameHighLimitConfiguredAndStoredFormat() throws IOException
    {
        prepareNeoStoreFile( HighLimit.STORE_VERSION, pageCache );

        Config config = config( HighLimit.NAME );

        assertSame( HighLimit.RECORD_FORMATS, selectForStoreOrConfig( config, databaseLayout, fs, pageCache, LOG, NULL ) );
    }

    @Test
    void selectForStoreOrConfigWithDifferentlyConfiguredAndStoredFormat() throws IOException
    {
        prepareNeoStoreFile( Standard.LATEST_STORE_VERSION, pageCache );

        Config config = config( HighLimit.NAME );

        assertThrows( IllegalArgumentException.class, () -> selectForStoreOrConfig( config, databaseLayout, fs, pageCache, LOG, NULL ) );
    }

    @Test
    void selectForStoreOrConfigWithOnlyStandardStoredFormat() throws IOException
    {
        prepareNeoStoreFile( Standard.LATEST_STORE_VERSION, pageCache );

        Config config = Config.defaults();

        assertSame( Standard.LATEST_RECORD_FORMATS, selectForStoreOrConfig( config, databaseLayout, fs, pageCache, LOG, NULL ) );
    }

    @Test
    void selectForStoreOrConfigWithOnlyHighLimitStoredFormat() throws IOException
    {
        prepareNeoStoreFile( HighLimit.STORE_VERSION, pageCache );

        Config config = Config.defaults();

        assertSame( HighLimit.RECORD_FORMATS, selectForStoreOrConfig( config, databaseLayout, fs, pageCache, LOG, NULL ) );
    }

    @Test
    void selectForStoreOrConfigWithOnlyStandardConfiguredFormat()
    {
        Config config = config( Standard.LATEST_NAME );

        assertSame( Standard.LATEST_RECORD_FORMATS, selectForStoreOrConfig( config, databaseLayout, fs, pageCache, LOG, NULL ) );
    }

    @Test
    void selectForStoreOrConfigWithOnlyHighLimitConfiguredFormat()
    {
        Config config = config( HighLimit.NAME );

        assertSame( HighLimit.RECORD_FORMATS, selectForStoreOrConfig( config, databaseLayout, fs, pageCache, LOG, NULL ) );
    }

    @Test
    void selectForStoreOrConfigWithWrongConfiguredFormat()
    {
        Config config = config( "unknown_format" );

        assertEquals( defaultFormat(), selectForStoreOrConfig( config, databaseLayout, fs, pageCache, LOG, NULL ) );
    }

    @Test
    void selectForStoreOrConfigWithoutConfiguredAndStoredFormats()
    {
        assertSame( defaultFormat(), selectForStoreOrConfig( Config.defaults(), databaseLayout, fs, pageCache, LOG, NULL ) );
    }

    @Test
    void selectNewestFormatWithConfiguredStandardFormat()
    {
        assertSame( Standard.LATEST_RECORD_FORMATS,
                selectNewestFormat( config( Standard.LATEST_NAME ), databaseLayout, fs, pageCache, LOG, NULL ) );
    }

    @Test
    void selectNewestFormatWithConfiguredHighLimitFormat()
    {
        assertSame( HighLimit.RECORD_FORMATS,
                selectNewestFormat( config( HighLimit.NAME ), databaseLayout, fs, pageCache, LOG, NULL ) );
    }

    @Test
    void selectNewestFormatWithWrongConfiguredFormat()
    {
        assertThrows( IllegalArgumentException.class, () -> selectNewestFormat( config( "unknown_format" ), databaseLayout, fs, pageCache, LOG, NULL ) );
    }

    @Test
    void selectNewestFormatWithoutConfigAndStore()
    {
        assertSame( defaultFormat(), selectNewestFormat( Config.defaults(), databaseLayout, fs, pageCache, LOG, NULL ) );
    }

    @Test
    void selectNewestFormatForExistingStandardStore() throws IOException
    {
        prepareNeoStoreFile( Standard.LATEST_STORE_VERSION, pageCache );

        Config config = Config.defaults();

        assertSame( Standard.LATEST_RECORD_FORMATS, selectNewestFormat( config, databaseLayout, fs, this.pageCache, LOG, NULL ) );
    }

    @ParameterizedTest
    @EnumSource( value = StoreVersion.class, mode = MATCH_ANY, names = "HIGH_LIMIT.+" )
    void selectNewestFormatForExistingHighLimitStore( StoreVersion storeVersion ) throws IOException
    {
        prepareNeoStoreFile( storeVersion.versionString(), pageCache );
        Config config = Config.defaults();
        assertSame( HighLimit.RECORD_FORMATS, selectNewestFormat( config, databaseLayout, fs, this.pageCache, LOG, NULL ) );
    }

    @Test
    void selectNewestFormatForExistingStoreWithLegacyFormat() throws IOException
    {
        prepareNeoStoreFile( StandardV3_4.STORE_VERSION, pageCache );

        Config config = Config.defaults();

        assertSame( defaultFormat(), selectNewestFormat( config, databaseLayout, fs, this.pageCache, LOG, NULL ) );
    }

    @Test
    void selectNewestFormatForExistingHighLimitWithLegacyFormat() throws IOException
    {
        prepareNeoStoreFile( HighLimitV3_4_0.STORE_VERSION, pageCache );

        Config config = Config.defaults();

        assertSame( HighLimit.RECORD_FORMATS, selectNewestFormat( config, databaseLayout, fs, this.pageCache, LOG, NULL ) );
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

        assertEquals( HighLimitV3_0_6.RECORD_FORMATS, findSuccessor( HighLimitV3_0_0.RECORD_FORMATS ).get() );
        assertEquals( HighLimitV3_1_0.RECORD_FORMATS, findSuccessor( HighLimitV3_0_6.RECORD_FORMATS ).get() );
        assertEquals( HighLimitV3_2_0.RECORD_FORMATS, findSuccessor( HighLimitV3_1_0.RECORD_FORMATS ).get() );
        assertEquals( HighLimitV3_4_0.RECORD_FORMATS, findSuccessor( HighLimitV3_2_0.RECORD_FORMATS ).get() );
        assertEquals( HighLimit.RECORD_FORMATS, findSuccessor( HighLimitV3_4_0.RECORD_FORMATS ).get() );
    }

    private void verifySelectForStore( PageCache pageCache, RecordFormats format ) throws IOException
    {
        prepareNeoStoreFile( format.storeVersion(), pageCache );
        assertSame( format, selectForStore( databaseLayout, fs, pageCache, LOG, NULL ) );
    }

    private void prepareNeoStoreFile( String storeVersion, PageCache pageCache ) throws IOException
    {
        prepareNeoStoreFile( storeVersion, pageCache, databaseLayout );
    }

    private void prepareNeoStoreFile( String storeVersion, PageCache pageCache, DatabaseLayout databaseLayout ) throws IOException
    {
        File neoStoreFile = createNeoStoreFile( databaseLayout );
        long value = MetaDataStore.versionStringToLong( storeVersion );
        MetaDataStore.setRecord( pageCache, neoStoreFile, STORE_VERSION, value, PageCursorTracer.NULL );
    }

    private File createNeoStoreFile( DatabaseLayout databaseLayout ) throws IOException
    {
        File neoStoreFile = databaseLayout.metadataStore();
        fs.write( neoStoreFile ).close();
        return neoStoreFile;
    }

    private static Config config( String recordFormatName )
    {
        return Config.defaults( GraphDatabaseSettings.record_format, recordFormatName );
    }
}
