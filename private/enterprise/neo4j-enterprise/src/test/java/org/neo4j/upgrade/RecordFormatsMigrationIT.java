/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.upgrade;

import com.neo4j.kernel.impl.enterprise.configuration.OnlineBackupSettings;
import com.neo4j.kernel.impl.store.format.highlimit.HighLimit;
import com.neo4j.kernel.impl.store.format.highlimit.v300.HighLimitV3_0_0;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;

import java.util.function.Consumer;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.helpers.Exceptions;
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
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.test.rule.fs.DefaultFileSystemRule;

import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

public class RecordFormatsMigrationIT
{
    private static final Label LABEL = Label.label( "Centipede" );
    private static final String PROPERTY = "legs";
    private static final int VALUE = 42;

    private final DefaultFileSystemRule fileSystemRule = new DefaultFileSystemRule();
    private final TestDirectory testDirectory = TestDirectory.testDirectory( fileSystemRule.get() );

    @Rule
    public RuleChain ruleChain = RuleChain.outerRule( testDirectory ).around( fileSystemRule );

    @Test
    public void migrateLatestStandardToLatestHighLimit() throws Exception
    {
        executeAndStopDb( startStandardFormatDb(), RecordFormatsMigrationIT::createNode );
        assertLatestStandardStore();

        executeAndStopDb( startHighLimitFormatDb(), RecordFormatsMigrationIT::assertNodeExists );
        assertLatestHighLimitStore();
    }

    @Test
    public void migrateHighLimitV3_0ToLatestHighLimit() throws Exception
    {
        executeAndStopDb( startDb( HighLimitV3_0_0.NAME ), RecordFormatsMigrationIT::createNode );
        assertStoreFormat( HighLimitV3_0_0.RECORD_FORMATS );

        executeAndStopDb( startHighLimitFormatDb(), RecordFormatsMigrationIT::assertNodeExists );
        assertLatestHighLimitStore();
    }

    @Test
    public void migrateHighLimitToStandard() throws Exception
    {
        executeAndStopDb( startHighLimitFormatDb(), RecordFormatsMigrationIT::createNode );
        assertLatestHighLimitStore();

        try
        {
            startStandardFormatDb();
            fail( "Should not be possible to downgrade" );
        }
        catch ( Exception e )
        {
            assertThat( Exceptions.rootCause( e ), instanceOf( UnexpectedUpgradingStoreFormatException.class ) );
        }
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
              PageCache pageCache = ConfigurableStandalonePageCacheFactory.createPageCache( fileSystemRule.get(), config, jobScheduler ) )
        {
            RecordFormats actual = RecordFormatSelector.selectForStoreOrConfig( config, testDirectory.databaseLayout(),
                    fileSystemRule, pageCache, NullLogProvider.getInstance() );
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
