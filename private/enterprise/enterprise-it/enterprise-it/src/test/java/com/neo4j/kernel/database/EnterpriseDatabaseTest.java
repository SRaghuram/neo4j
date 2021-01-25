/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.kernel.database;

import com.neo4j.kernel.impl.store.format.highlimit.HighLimit;
import com.neo4j.test.extension.EnterpriseDbmsExtension;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.database.Database;
import org.neo4j.kernel.impl.store.MetaDataStore;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.DbmsController;
import org.neo4j.test.extension.ExtensionCallback;
import org.neo4j.test.extension.Inject;

import static java.lang.Boolean.TRUE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.configuration.GraphDatabaseSettings.allow_upgrade;
import static org.neo4j.configuration.GraphDatabaseSettings.record_format;
import static org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer.NULL;
import static org.neo4j.kernel.impl.store.format.aligned.PageAligned.LATEST_RECORD_FORMATS;

@EnterpriseDbmsExtension( configurationCallback = "configure" )
class EnterpriseDatabaseTest
{
    @Inject
    private Database database;
    @Inject
    private DbmsController controller;
    @Inject
    private DatabaseManagementService managementService;
    @Inject
    private PageCache pageCache;
    private String databaseName;

    @ExtensionCallback
    void configure( TestDatabaseManagementServiceBuilder builder )
    {
        builder.setConfig( allow_upgrade, TRUE );
    }

    @BeforeEach
    void setUp()
    {
        databaseName = database.getNamedDatabaseId().name();
        managementService.dropDatabase( databaseName );
        controller.restartDbms( databaseName, builder -> builder.setConfig( record_format, LATEST_RECORD_FORMATS.name() ) );
    }

    @Test
    void upgradeOfStartedDatabaseMustObeyStartAfterUpgradeTrue() throws IOException
    {
        assertThat( getRecordFormat( pageCache, database ) ).isEqualTo( LATEST_RECORD_FORMATS.storeVersion() );

        controller.restartDbms( databaseName, builder -> builder.setConfig( record_format, HighLimit.NAME ) );
        database.upgrade( true );

        assertTrue( database.isStarted() );
        assertThat( getRecordFormat( pageCache, database ) ).isEqualTo( HighLimit.STORE_VERSION );
    }

    @Test
    void upgradeOfStoppedDatabaseMustObeyStartAfterUpgradeTrue() throws IOException
    {
        assertThat( getRecordFormat( pageCache, database ) ).isEqualTo( LATEST_RECORD_FORMATS.storeVersion() );
        database.stop();

        controller.restartDbms( databaseName, builder -> builder.setConfig( record_format, HighLimit.NAME ) );
        database.upgrade( true );

        assertTrue( database.isStarted() );
        assertThat( getRecordFormat( pageCache, database ) ).isEqualTo( HighLimit.STORE_VERSION );
    }

    @Test
    void upgradeOfStartedDatabaseMustObeyStartAfterUpgradeFalse() throws IOException
    {
        assertThat( getRecordFormat( pageCache, database ) ).isEqualTo( LATEST_RECORD_FORMATS.storeVersion() );

        controller.restartDbms( builder -> builder.setConfig( record_format, HighLimit.NAME ) );
        database.upgrade( false );

        assertFalse( database.isStarted() );
        assertThat( getRecordFormat( pageCache, database ) ).isEqualTo( HighLimit.STORE_VERSION );

        database.start();
        assertTrue( database.isStarted() );
        assertThat( getRecordFormat( pageCache, database ) ).isEqualTo( HighLimit.STORE_VERSION );
    }

    @Test
    void upgradeOfStoppedDatabaseMustObeyStartAfterUpgradeFalse() throws IOException
    {
        assertThat( getRecordFormat( pageCache, database ) ).isEqualTo( LATEST_RECORD_FORMATS.storeVersion() );

        controller.restartDbms( builder -> builder.setConfig( record_format, HighLimit.NAME ) );
        database.upgrade( false );

        assertFalse( database.isStarted() );
        assertThat( getRecordFormat( pageCache, database ) ).isEqualTo( HighLimit.STORE_VERSION );

        database.start();
        assertTrue( database.isStarted() );
        assertThat( getRecordFormat( pageCache, database ) ).isEqualTo( HighLimit.STORE_VERSION );
    }

    private String getRecordFormat( PageCache pageCache, Database database ) throws IOException
    {
        long record = MetaDataStore.getRecord( pageCache, database.getDatabaseLayout().metadataStore(), MetaDataStore.Position.STORE_VERSION, NULL );
        return MetaDataStore.versionLongToString( record );
    }
}
