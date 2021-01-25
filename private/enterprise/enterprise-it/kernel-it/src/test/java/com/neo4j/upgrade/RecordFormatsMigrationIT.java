/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.upgrade;

import com.neo4j.configuration.OnlineBackupSettings;
import com.neo4j.kernel.impl.store.format.highlimit.HighLimit;
import com.neo4j.kernel.impl.store.format.highlimit.v300.HighLimitV3_0_0;
import com.neo4j.kernel.impl.store.format.highlimit.v306.HighLimitV3_0_6;
import com.neo4j.kernel.impl.store.format.highlimit.v310.HighLimitV3_1_0;
import com.neo4j.kernel.impl.store.format.highlimit.v320.HighLimitV3_2_0;
import com.neo4j.kernel.impl.store.format.highlimit.v340.HighLimitV3_4_0;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.function.Consumer;

import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.dbms.DatabaseStateService;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.dbms.api.DatabaseManagementServiceBuilder;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.impl.pagecache.ConfigurableStandalonePageCacheFactory;
import org.neo4j.kernel.impl.store.format.RecordFormatSelector;
import org.neo4j.kernel.impl.store.format.RecordFormats;
import org.neo4j.kernel.impl.store.format.standard.Standard;
import org.neo4j.kernel.impl.storemigration.StoreUpgrader.UnexpectedUpgradingStoreFormatException;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.Neo4jLayoutExtension;
import org.neo4j.test.scheduler.ThreadPoolJobScheduler;

import static org.apache.commons.lang3.exception.ExceptionUtils.getRootCause;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.io.pagecache.tracing.PageCacheTracer.NULL;

@Neo4jLayoutExtension
class RecordFormatsMigrationIT
{
    private static final Label LABEL = Label.label( "Centipede" );
    private static final String PROPERTY = "legs";
    private static final int VALUE = 42;

    @Inject
    private DefaultFileSystemAbstraction fileSystem;
    @Inject
    private DatabaseLayout databaseLayout;
    private DatabaseManagementService managementService;

    @Test
    void migrateLatestStandardToLatestHighLimit() throws Exception
    {
        executeAndStopDb( startStandardFormatDb(), RecordFormatsMigrationIT::createNode );
        assertLatestStandardStore();

        executeAndStopDb( startDb( HighLimit.NAME ), RecordFormatsMigrationIT::assertNodeExists );
        assertLatestHighLimitStore();
    }

    @ParameterizedTest
    @ValueSource( strings = {HighLimitV3_0_0.NAME, HighLimitV3_0_6.NAME, HighLimitV3_1_0.NAME, HighLimitV3_2_0.NAME, HighLimitV3_4_0.NAME} )
    void migrateOldHighLimitToLatestHighLimit( String recordFormatName ) throws Exception
    {
        executeAndStopDb( startDb( recordFormatName ), RecordFormatsMigrationIT::createNode );
        assertStoreFormat( recordFormatName );

        executeAndStopDb( startDb(), RecordFormatsMigrationIT::assertNodeExists );
        assertLatestHighLimitStore();
    }

    @Test
    void migrateHighLimitV3_4ToLatestHighLimit() throws Exception
    {
        executeAndStopDb( startDb( HighLimitV3_4_0.NAME ), RecordFormatsMigrationIT::createNode );
        assertStoreFormat( HighLimitV3_4_0.RECORD_FORMATS.name() );

        executeAndStopDb( startDb(), RecordFormatsMigrationIT::assertNodeExists );
        assertLatestHighLimitStore();
    }

    @Test
    void migrateHighLimitToStandard() throws Exception
    {
        executeAndStopDb( startDb( HighLimit.NAME ), RecordFormatsMigrationIT::createNode );
        assertLatestHighLimitStore();

        GraphDatabaseAPI database = (GraphDatabaseAPI) startStandardFormatDb();
        try
        {
            DatabaseStateService dbStateService = database.getDependencyResolver().resolveDependency( DatabaseStateService.class );
            assertTrue( dbStateService.causeOfFailure( database.databaseId() ).isPresent() );
            assertThat( getRootCause( dbStateService.causeOfFailure( database.databaseId() ).get() ) ).isInstanceOf(
                    UnexpectedUpgradingStoreFormatException.class );
        }
        finally
        {
            managementService.shutdown();
        }
        assertLatestHighLimitStore();
    }

    private static void createNode( GraphDatabaseService db )
    {
        try ( Transaction tx = db.beginTx() )
        {
            Node start = tx.createNode( LABEL );
            start.setProperty( PROPERTY, VALUE );
            tx.commit();
        }
    }

    private static void assertNodeExists( GraphDatabaseService db )
    {
        try ( Transaction tx = db.beginTx() )
        {
            assertNotNull( tx.findNode( LABEL, PROPERTY, VALUE ) );
            tx.commit();
        }
    }

    private GraphDatabaseService startStandardFormatDb()
    {
        return startDb( Standard.LATEST_NAME );
    }

    private GraphDatabaseService startDb( String recordFormatName )
    {
        managementService = getGraphDatabaseBuilder()
                .setConfig( GraphDatabaseSettings.record_format, recordFormatName ).build();
        return managementService.database( DEFAULT_DATABASE_NAME );
    }

    private GraphDatabaseService startDb()
    {
        managementService = getGraphDatabaseBuilder().build();
        return managementService.database( DEFAULT_DATABASE_NAME );
    }

    private DatabaseManagementServiceBuilder getGraphDatabaseBuilder()
    {
        return new TestDatabaseManagementServiceBuilder( databaseLayout ).setConfig( GraphDatabaseSettings.allow_upgrade, true )
                .setConfig( OnlineBackupSettings.online_backup_enabled, false );
    }

    private void assertLatestStandardStore() throws Exception
    {
        assertStoreFormat( Standard.LATEST_RECORD_FORMATS.name() );
    }

    private void assertLatestHighLimitStore() throws Exception
    {
        assertStoreFormat( HighLimit.RECORD_FORMATS.name() );
    }

    private void assertStoreFormat( String formatName ) throws Exception
    {
        Config config = Config.defaults( GraphDatabaseSettings.pagecache_memory, "8m" );
        try ( JobScheduler jobScheduler = new ThreadPoolJobScheduler();
              PageCache pageCache = ConfigurableStandalonePageCacheFactory.createPageCache( fileSystem, config, jobScheduler, NULL ) )
        {
            RecordFormats actual = RecordFormatSelector.selectForStoreOrConfig( config, databaseLayout,
                    fileSystem, pageCache, NullLogProvider.getInstance(), NULL );
            assertNotNull( actual );
            assertEquals( formatName, actual.name() );
        }
    }

    private void executeAndStopDb( GraphDatabaseService db, Consumer<GraphDatabaseService> action )
    {
        try
        {
            action.accept( db );
        }
        finally
        {
            managementService.shutdown();
        }
    }
}
