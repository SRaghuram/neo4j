/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.kernel.impl.storemigration;

import com.neo4j.graphdb.factory.EnterpriseGraphDatabaseFactory;
import org.apache.commons.io.IOUtils;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import org.neo4j.consistency.ConsistencyCheckService;
import org.neo4j.consistency.checking.full.ConsistencyCheckIncompleteException;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.helpers.progress.ProgressMonitorFactory;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.configuration.Settings;
import org.neo4j.kernel.impl.store.format.RecordFormatSelector;
import org.neo4j.kernel.impl.store.format.RecordFormats;
import org.neo4j.kernel.impl.storemigration.RecordStoreVersionCheck;
import org.neo4j.kernel.impl.storemigration.UpgradableDatabase;
import org.neo4j.kernel.impl.transaction.log.ReadableClosablePositionAwareChannel;
import org.neo4j.kernel.impl.transaction.log.entry.VersionAwareLogEntryReader;
import org.neo4j.kernel.impl.transaction.log.files.LogFiles;
import org.neo4j.kernel.impl.transaction.log.files.LogFilesBuilder;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.kernel.recovery.LogTailScanner;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.test.rule.PageCacheRule;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.test.rule.fs.DefaultFileSystemRule;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

@RunWith( Parameterized.class )
public class StoreMigrationIT
{
    private static final PageCacheRule pageCacheRule = new PageCacheRule();
    private static final DefaultFileSystemRule fileSystemRule = new DefaultFileSystemRule();
    private static final TestDirectory testDir = TestDirectory.testDirectory( fileSystemRule );
    @ClassRule
    public static final RuleChain ruleChain = RuleChain.outerRule( fileSystemRule ).around( pageCacheRule ).around( testDir );

    public static final String CREATE_QUERY = readQuery();
    protected final RecordFormats from;
    protected final RecordFormats to;

    @Parameterized.Parameters( name = "Migrate: {0}->{1}" )
    public static Iterable<Object[]> data() throws IOException
    {
        FileSystemAbstraction fs = fileSystemRule.get();
        PageCache pageCache = pageCacheRule.getPageCache( fs );
        TestDirectory testDirectory = TestDirectory.testDirectory();
        testDirectory.prepareDirectory( StoreMigrationIT.class, "migration" );
        DatabaseLayout databaseLayout = testDirectory.databaseLayout();
        RecordStoreVersionCheck storeVersionCheck = new RecordStoreVersionCheck( pageCache );
        VersionAwareLogEntryReader<ReadableClosablePositionAwareChannel> logEntryReader = new VersionAwareLogEntryReader<>();
        LogFiles logFiles = LogFilesBuilder.logFilesBasedOnlyBuilder( databaseLayout.databaseDirectory(), fs ).withLogEntryReader( logEntryReader ).build();
        LogTailScanner tailScanner = new LogTailScanner( logFiles, logEntryReader, new Monitors() );
        List<Object[]> data = new ArrayList<>();
        ArrayList<RecordFormats> recordFormats = new ArrayList<>();
        RecordFormatSelector.allFormats().forEach( f -> addIfNotThere( f, recordFormats ) );
        for ( RecordFormats toFormat : recordFormats )
        {
            UpgradableDatabase upgradableDatabase =
                    new UpgradableDatabase( storeVersionCheck, toFormat, tailScanner );
            for ( RecordFormats fromFormat : recordFormats )
            {
                try
                {
                    createDb( fromFormat, databaseLayout.databaseDirectory() );
                    if ( !upgradableDatabase.hasCurrentVersion( databaseLayout ) )
                    {
                        upgradableDatabase.checkUpgradable( databaseLayout );
                        data.add( new Object[]{fromFormat, toFormat} );
                    }
                }
                catch ( Exception e )
                {
                    //This means that the combination is not migratable.
                }
                fs.deleteRecursively( databaseLayout.databaseDirectory() );
            }
        }

        return data;
    }

    private static String baseDirName( RecordFormats toFormat, RecordFormats fromFormat )
    {
        return fromFormat.storeVersion() + toFormat.storeVersion();
    }

    private static void addIfNotThere( RecordFormats f, ArrayList<RecordFormats> recordFormats )
    {
        for ( RecordFormats format : recordFormats )
        {
            if ( format.storeVersion().equals( f.storeVersion() ) )
            {
                return;
            }
        }
        recordFormats.add( f );
    }

    private static void createDb( RecordFormats recordFormat, File storeDir )
    {
        GraphDatabaseService database = new GraphDatabaseFactory().newEmbeddedDatabaseBuilder( storeDir )
                .setConfig( GraphDatabaseSettings.allow_upgrade, Settings.TRUE )
                .setConfig( GraphDatabaseSettings.record_format, recordFormat.name() ).newGraphDatabase();
        database.shutdown();
    }

    public StoreMigrationIT( RecordFormats from, RecordFormats to )
    {
        this.from = from;
        this.to = to;
    }

    @Test
    public void shouldMigrate() throws Exception
    {
        DatabaseLayout databaseLayout = testDir.databaseLayout( baseDirName( to, from ) );
        GraphDatabaseService database = getGraphDatabaseService( databaseLayout.databaseDirectory(), from.name() );

        database.execute( "CREATE INDEX ON :Person(name)" );
        database.execute( "CREATE INDEX ON :Person(born)" );
        database.execute( "CREATE CONSTRAINT ON (person:Person) ASSERT exists(person.name)" );
        database.execute( CREATE_QUERY );
        long beforeNodes;
        long beforeLabels;
        long beforeKeys;
        long beforeRels;
        long beforeRelTypes;
        long beforeIndexes;
        long beforeConstraints;
        try ( Transaction ignore = database.beginTx() )
        {
            beforeNodes = database.getAllNodes().stream().count();
            beforeLabels = database.getAllLabels().stream().count();
            beforeKeys = database.getAllPropertyKeys().stream().count();
            beforeRels = database.getAllRelationships().stream().count();
            beforeRelTypes = database.getAllRelationshipTypes().stream().count();
            beforeIndexes = stream( database.schema().getIndexes() ).count();
            beforeConstraints = stream( database.schema().getConstraints() ).count();
        }
        database.shutdown();

        database = getGraphDatabaseService( databaseLayout.databaseDirectory(), to.name() );
        long afterNodes;
        long afterLabels;
        long afterKeys;
        long afterRels;
        long afterRelTypes;
        long afterIndexes;
        long afterConstraints;
        try ( Transaction ignore = database.beginTx() )
        {
            afterNodes = database.getAllNodes().stream().count();
            afterLabels = database.getAllLabels().stream().count();
            afterKeys = database.getAllPropertyKeys().stream().count();
            afterRels = database.getAllRelationships().stream().count();
            afterRelTypes = database.getAllRelationshipTypes().stream().count();
            afterIndexes = stream( database.schema().getIndexes() ).count();
            afterConstraints = stream( database.schema().getConstraints() ).count();
        }
        database.shutdown();

        assertEquals( beforeNodes, afterNodes ); //171
        assertEquals( beforeLabels, afterLabels ); //2
        assertEquals( beforeKeys, afterKeys ); //8
        assertEquals( beforeRels, afterRels ); //253
        assertEquals( beforeRelTypes, afterRelTypes ); //6
        assertEquals( beforeIndexes, afterIndexes ); //2
        assertEquals( beforeConstraints, afterConstraints ); //1
        ConsistencyCheckService consistencyCheckService = new ConsistencyCheckService( );
        ConsistencyCheckService.Result result =
                runConsistencyChecker( databaseLayout, fileSystemRule.get(), consistencyCheckService, to.name() );
        if ( !result.isSuccessful() )
        {
            fail( "Database is inconsistent after migration." );
        }
    }

    protected <T> Stream<T> stream( Iterable<T> iterable )
    {
        return StreamSupport.stream( iterable.spliterator(), false );
    }

    protected static ConsistencyCheckService.Result runConsistencyChecker( DatabaseLayout databaseLayout, FileSystemAbstraction fs,
            ConsistencyCheckService consistencyCheckService, String formatName )
            throws ConsistencyCheckIncompleteException
    {
        Config config = Config.defaults( GraphDatabaseSettings.record_format, formatName );
        return consistencyCheckService.runFullConsistencyCheck( databaseLayout, config, ProgressMonitorFactory.NONE,
                NullLogProvider.getInstance(), fs, false );
    }

    protected static GraphDatabaseService getGraphDatabaseService( File db, String formatName )
    {
        return new EnterpriseGraphDatabaseFactory().newEmbeddedDatabaseBuilder( db )
                .setConfig( GraphDatabaseSettings.allow_upgrade, Settings.TRUE )
                .setConfig( GraphDatabaseSettings.record_format, formatName ).newGraphDatabase();
    }

    private static String readQuery()
    {
        try
        {
            return IOUtils.resourceToString( "store-migration-data.txt", StandardCharsets.UTF_8, StoreMigrationIT.class.getClassLoader() );
        }
        catch ( IOException io )
        {
            throw new UncheckedIOException( io );
        }
    }
}
