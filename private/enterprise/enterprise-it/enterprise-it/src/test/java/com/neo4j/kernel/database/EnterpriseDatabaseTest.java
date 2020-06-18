/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.kernel.database;

import com.neo4j.kernel.impl.store.format.highlimit.HighLimit;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

import org.neo4j.collection.Dependencies;
import org.neo4j.configuration.Config;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.database.Database;
import org.neo4j.kernel.impl.store.MetaDataStore;
import org.neo4j.kernel.impl.store.format.standard.Standard;
import org.neo4j.test.rule.DatabaseRule;
import org.neo4j.test.rule.PageCacheRule;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.test.rule.fs.DefaultFileSystemRule;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.configuration.GraphDatabaseSettings.neo4j_home;
import static org.neo4j.configuration.GraphDatabaseSettings.record_format;
import static org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer.NULL;

/**
 * Based on {@code DatabaseTest} in kernel-it.
 * This test is only placed in the enterprise code base so we can test upgrading using the
 * standard format to high_limit upgrade path.
 */
public class EnterpriseDatabaseTest
{
    @Rule
    public DefaultFileSystemRule fs = new DefaultFileSystemRule();
    @Rule
    public TestDirectory directory = TestDirectory.testDirectory( fs.get() );
    @Rule
    public DatabaseRule databaseRule = new DatabaseRule();
    @Rule
    public PageCacheRule pageCacheRule = new PageCacheRule();

    private DatabaseLayout databaseLayout;
    private Config cfg;
    private Dependencies deps;
    private PageCache pageCache;

    @Before
    public void setUp()
    {
        databaseLayout = DatabaseLayout.ofFlat( directory.directory( DEFAULT_DATABASE_NAME ) );
        cfg = Config.newBuilder()
                    .set( neo4j_home, directory.homeDir().toPath() )
                    .set( record_format, "standard" )
                    .build();
        deps = new Dependencies();
        deps.satisfyDependencies( cfg );
        pageCache = pageCacheRule.getPageCache( fs );
    }

    @Test
    public void upgradeOfStartedDatabaseMustObeyStartAfterUpgradeTrue() throws IOException
    {
        Database database = databaseRule.getDatabase( databaseLayout, fs, pageCache, deps );
        database.start();
        assertThat( getRecordFormat( pageCache, database ) ).isEqualTo( Standard.LATEST_STORE_VERSION );

        cfg.set( record_format, "high_limit" );
        database.upgrade( true );

        assertTrue( database.isStarted() );
        assertThat( getRecordFormat( pageCache, database ) ).isEqualTo( HighLimit.STORE_VERSION );
    }

    @Test
    public void upgradeOfStoppedDatabaseMustObeyStartAfterUpgradeTrue() throws IOException
    {
        Database database = databaseRule.getDatabase( databaseLayout, fs, pageCache, deps );
        database.start();
        assertThat( getRecordFormat( pageCache, database ) ).isEqualTo( Standard.LATEST_STORE_VERSION );
        database.stop();

        cfg.set( record_format, "high_limit" );
        database.upgrade( true );

        assertTrue( database.isStarted() );
        assertThat( getRecordFormat( pageCache, database ) ).isEqualTo( HighLimit.STORE_VERSION );
    }

    @Test
    public void upgradeOfStartedDatabaseMustObeyStartAfterUpgradeFalse() throws IOException
    {
        Database database = databaseRule.getDatabase( databaseLayout, fs, pageCache, deps );
        database.start();
        assertThat( getRecordFormat( pageCache, database ) ).isEqualTo( Standard.LATEST_STORE_VERSION );

        cfg.set( record_format, "high_limit" );
        database.upgrade( false );

        assertFalse( database.isStarted() );
        assertThat( getRecordFormat( pageCache, database ) ).isEqualTo( HighLimit.STORE_VERSION );

        database.start();
        assertTrue( database.isStarted() );
        assertThat( getRecordFormat( pageCache, database ) ).isEqualTo( HighLimit.STORE_VERSION );
    }

    @Test
    public void upgradeOfStoppedDatabaseMustObeyStartAfterUpgradeFalse() throws IOException
    {
        Database database = databaseRule.getDatabase( databaseLayout, fs, pageCache, deps );
        database.start();
        assertThat( getRecordFormat( pageCache, database ) ).isEqualTo( Standard.LATEST_STORE_VERSION );
        database.stop();

        cfg.set( record_format, "high_limit" );
        database.upgrade( false );

        assertFalse( database.isStarted() );
        assertThat( getRecordFormat( pageCache, database ) ).isEqualTo( HighLimit.STORE_VERSION );

        database.start();
        assertTrue( database.isStarted() );
        assertThat( getRecordFormat( pageCache, database ) ).isEqualTo( HighLimit.STORE_VERSION );
    }

    private String getRecordFormat( PageCache pageCache, Database database ) throws IOException
    {
        File file = database.getDatabaseLayout().metadataStore();
        long record = MetaDataStore.getRecord( pageCache, file, MetaDataStore.Position.STORE_VERSION, NULL );
        return MetaDataStore.versionLongToString( record );
    }
}
