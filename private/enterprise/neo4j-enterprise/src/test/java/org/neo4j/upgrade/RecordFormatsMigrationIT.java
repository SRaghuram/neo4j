/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.upgrade;

import com.neo4j.kernel.impl.enterprise.configuration.OnlineBackupSettings;
import com.neo4j.kernel.impl.store.format.highlimit.HighLimit;
import com.neo4j.kernel.impl.store.format.highlimit.v300.HighLimitV3_0_0;
import com.neo4j.kernel.impl.store.format.highlimit.v340.HighLimitV3_4_0;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.function.Consumer;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.configuration.Settings;
import org.neo4j.kernel.impl.pagecache.ConfigurableStandalonePageCacheFactory;
import org.neo4j.kernel.impl.store.format.RecordFormatSelector;
import org.neo4j.kernel.impl.store.format.RecordFormats;
import org.neo4j.kernel.impl.store.format.standard.Standard;
import org.neo4j.kernel.impl.storemigration.StoreUpgrader.UnexpectedUpgradingStoreFormatException;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.scheduler.ThreadPoolJobScheduler;
import org.neo4j.test.TestGraphDatabaseFactory;
import org.neo4j.test.extension.DefaultFileSystemExtension;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

import static org.apache.commons.lang3.exception.ExceptionUtils.getRootCause;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

@ExtendWith( {DefaultFileSystemExtension.class, TestDirectoryExtension.class} )
class RecordFormatsMigrationIT
{
    private static final Label LABEL = Label.label( "Centipede" );
    private static final String PROPERTY = "legs";
    private static final int VALUE = 42;

    @Inject
    private DefaultFileSystemAbstraction fileSystem;
    @Inject
    private TestDirectory testDirectory;

    @Test
    void migrateLatestStandardToLatestHighLimit() throws Exception
    {
        executeAndStopDb( startStandardFormatDb(), RecordFormatsMigrationIT::createNode );
        assertLatestStandardStore();

        executeAndStopDb( startHighLimitFormatDb(), RecordFormatsMigrationIT::assertNodeExists );
        assertLatestHighLimitStore();
    }

    @Test
    void migrateHighLimitV3_0ToLatestHighLimit() throws Exception
    {
        executeAndStopDb( startDb( HighLimitV3_0_0.NAME ), RecordFormatsMigrationIT::createNode );
        assertStoreFormat( HighLimitV3_0_0.RECORD_FORMATS );

        executeAndStopDb( startHighLimitFormatDb(), RecordFormatsMigrationIT::assertNodeExists );
        assertLatestHighLimitStore();
    }

    @Test
    void migrateHighLimitV3_4ToLatestHighLimit() throws Exception
    {
        executeAndStopDb( startDb( HighLimitV3_4_0.NAME ), RecordFormatsMigrationIT::createNode );
        assertStoreFormat( HighLimitV3_4_0.RECORD_FORMATS );

        executeAndStopDb( startHighLimitFormatDb(), RecordFormatsMigrationIT::assertNodeExists );
        assertLatestHighLimitStore();
    }

    @Test
    void migrateHighLimitToStandard() throws Exception
    {
        executeAndStopDb( startHighLimitFormatDb(), RecordFormatsMigrationIT::createNode );
        assertLatestHighLimitStore();

        Exception exception = assertThrows( Exception.class, this::startStandardFormatDb );
        assertThat( getRootCause( exception ), instanceOf( UnexpectedUpgradingStoreFormatException.class ) );
        assertLatestHighLimitStore();
    }

    private static void createNode( GraphDatabaseService db )
    {
        try ( Transaction tx = db.beginTx() )
        {
            Node start = db.createNode( LABEL );
            start.setProperty( PROPERTY, VALUE );
            tx.success();
        }
    }

    private static void assertNodeExists( GraphDatabaseService db )
    {
        try ( Transaction tx = db.beginTx() )
        {
            assertNotNull( db.findNode( LABEL, PROPERTY, VALUE ) );
            tx.success();
        }
    }

    private GraphDatabaseService startStandardFormatDb()
    {
        return startDb( Standard.LATEST_NAME );
    }

    private GraphDatabaseService startHighLimitFormatDb()
    {
        return startDb( HighLimit.NAME );
    }

    private GraphDatabaseService startDb( String recordFormatName )
    {
        return new TestGraphDatabaseFactory().newEmbeddedDatabaseBuilder( testDirectory.databaseDir() )
                .setConfig( GraphDatabaseSettings.allow_upgrade, Settings.TRUE )
                .setConfig( GraphDatabaseSettings.record_format, recordFormatName )
                .setConfig( OnlineBackupSettings.online_backup_enabled, Settings.FALSE )
                .newGraphDatabase();
    }

    private void assertLatestStandardStore() throws Exception
    {
        assertStoreFormat( Standard.LATEST_RECORD_FORMATS );
    }

    private void assertLatestHighLimitStore() throws Exception
    {
        assertStoreFormat( HighLimit.RECORD_FORMATS );
    }

    private void assertStoreFormat( RecordFormats expected ) throws Exception
    {
        Config config = Config.defaults( GraphDatabaseSettings.pagecache_memory, "8m" );
        try ( JobScheduler jobScheduler = new ThreadPoolJobScheduler();
              PageCache pageCache = ConfigurableStandalonePageCacheFactory.createPageCache( fileSystem, config, jobScheduler ) )
        {
            RecordFormats actual = RecordFormatSelector.selectForStoreOrConfig( config, testDirectory.databaseLayout(),
                    fileSystem, pageCache, NullLogProvider.getInstance() );
            assertNotNull( actual );
            assertEquals( expected.storeVersion(), actual.storeVersion() );
        }
    }

    private static void executeAndStopDb( GraphDatabaseService db, Consumer<GraphDatabaseService> action )
    {
        try
        {
            action.accept( db );
        }
        finally
        {
            db.shutdown();
        }
    }
}
