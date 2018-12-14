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
package org.neo4j.kernel.impl.storemigration;

import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.rules.RuleChain;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;

import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.impl.store.MetaDataStore;
import org.neo4j.kernel.impl.store.format.RecordFormatSelector;
import org.neo4j.kernel.impl.store.format.RecordFormats;
import org.neo4j.kernel.impl.store.format.StoreVersion;
import org.neo4j.kernel.impl.store.format.standard.Standard;
import org.neo4j.kernel.impl.store.format.standard.StandardFormatFamily;
import org.neo4j.kernel.impl.store.format.standard.StandardV3_4;
import org.neo4j.kernel.impl.transaction.log.ReadableClosablePositionAwareChannel;
import org.neo4j.kernel.impl.transaction.log.entry.VersionAwareLogEntryReader;
import org.neo4j.kernel.impl.transaction.log.files.LogFiles;
import org.neo4j.kernel.impl.transaction.log.files.LogFilesBuilder;
import org.neo4j.kernel.internal.Version;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.kernel.recovery.LogTailScanner;
import org.neo4j.test.rule.PageCacheRule;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.test.rule.fs.DefaultFileSystemRule;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.neo4j.kernel.impl.store.MetaDataStore.Position.STORE_VERSION;
import static org.neo4j.kernel.impl.storemigration.MigrationTestUtils.changeVersionNumber;
import static org.neo4j.kernel.impl.storemigration.MigrationTestUtils.removeCheckPointFromTxLog;
import static org.neo4j.kernel.impl.storemigration.StoreUpgrader.UnexpectedUpgradingStoreVersionException.MESSAGE;

@Ignore
@RunWith( Enclosed.class )
public class UpgradableDatabaseTest
{
    @RunWith( Parameterized.class )
    public static class SupportedVersions
    {

        private final TestDirectory testDirectory = TestDirectory.testDirectory();
        private final PageCacheRule pageCacheRule = new PageCacheRule();
        private final DefaultFileSystemRule fileSystemRule = new DefaultFileSystemRule();

        @Rule
        public RuleChain ruleChain = RuleChain.outerRule( testDirectory )
                                              .around( fileSystemRule ).around( pageCacheRule );

        private DatabaseLayout databaseLayout;
        private FileSystemAbstraction fileSystem;
        private LogTailScanner tailScanner;

        @Parameterized.Parameter( 0 )
        public String version;

        @Parameterized.Parameters( name = "{0}" )
        public static Collection<String> versions()
        {
            return Collections.singletonList( StandardV3_4.STORE_VERSION );
        }

        @Before
        public void setup() throws IOException
        {
            fileSystem = fileSystemRule.get();
            databaseLayout = testDirectory.databaseLayout();
            MigrationTestUtils.findFormatStoreDirectoryForVersion( version, databaseLayout.databaseDirectory() );
            VersionAwareLogEntryReader<ReadableClosablePositionAwareChannel> logEntryReader = new VersionAwareLogEntryReader<>();
            LogFiles logFiles = LogFilesBuilder.logFilesBasedOnlyBuilder( databaseLayout.databaseDirectory(), fileSystem ).build();
            tailScanner = new LogTailScanner( logFiles, logEntryReader, new Monitors() );
        }

        boolean storeFilesUpgradable( DatabaseLayout databaseLayout, UpgradableDatabase upgradableDatabase )
        {
            try
            {
                upgradableDatabase.checkUpgradable( databaseLayout );
                return true;
            }
            catch ( StoreUpgrader.UnableToUpgradeException e )
            {
                return false;
            }
        }

        @Test
        public void shouldAcceptTheStoresInTheSampleDatabaseAsBeingEligibleForUpgrade()
        {
            // given
            final UpgradableDatabase upgradableDatabase = getUpgradableDatabase();

            // when
            final boolean result = storeFilesUpgradable( databaseLayout, upgradableDatabase );

            // then
            assertTrue( result );
        }

        @Test
        public void shouldDetectOldVersionAsDifferentFromCurrent()
        {
            // given
            final UpgradableDatabase upgradableDatabase = getUpgradableDatabase();
            // when
            boolean currentVersion = upgradableDatabase.hasCurrentVersion( databaseLayout );

            // then
            assertFalse( currentVersion );
        }

        @Test
        public void shouldRejectStoresIfDBIsNotShutdownCleanly() throws IOException
        {
            // given
            removeCheckPointFromTxLog( fileSystem, databaseLayout.databaseDirectory() );
            final UpgradableDatabase upgradableDatabase = getUpgradableDatabase();

            // when
            final boolean result = storeFilesUpgradable( databaseLayout, upgradableDatabase );

            // then
            assertFalse( result );
        }

        private UpgradableDatabase getUpgradableDatabase()
        {
            return new UpgradableDatabase( new RecordStoreVersionCheck( pageCacheRule.getPageCache( fileSystem ) ),
                    getRecordFormat(), tailScanner );
        }
    }

    @RunWith( Parameterized.class )
    public static class UnsupportedVersions
    {
        private final TestDirectory testDirectory = TestDirectory.testDirectory();
        private final PageCacheRule pageCacheRule = new PageCacheRule();
        private final DefaultFileSystemRule fileSystemRule = new DefaultFileSystemRule();

        @Rule
        public RuleChain ruleChain = RuleChain.outerRule( testDirectory )
                .around( fileSystemRule ).around( pageCacheRule );

        private DatabaseLayout databaseLayout;
        private FileSystemAbstraction fileSystem;
        private LogTailScanner tailScanner;

        @Parameterized.Parameter( 0 )
        public String version;

        @Parameterized.Parameters( name = "{0}" )
        public static Collection<String> versions()
        {
            return Arrays.asList( "v0.A.4", StoreVersion.HIGH_LIMIT_V3_4_0.versionString() );
        }

        @Before
        public void setup() throws IOException
        {
            fileSystem = fileSystemRule.get();
            databaseLayout = testDirectory.databaseLayout();
            // doesn't matter which version we pick we are changing it to the wrong one...
            MigrationTestUtils.findFormatStoreDirectoryForVersion( StandardV3_4.STORE_VERSION, databaseLayout.databaseDirectory() );
            changeVersionNumber( fileSystem, databaseLayout.metadataStore(), version );
            File metadataStore = databaseLayout.metadataStore();
            PageCache pageCache = pageCacheRule.getPageCache( fileSystem );
            MetaDataStore.setRecord( pageCache, metadataStore, STORE_VERSION, MetaDataStore.versionStringToLong( version ) );
            VersionAwareLogEntryReader<ReadableClosablePositionAwareChannel> logEntryReader = new VersionAwareLogEntryReader<>();
            LogFiles logFiles = LogFilesBuilder.logFilesBasedOnlyBuilder( databaseLayout.databaseDirectory(), fileSystem ).build();
            tailScanner = new LogTailScanner( logFiles, logEntryReader, new Monitors() );
        }

        @Test
        public void shouldDetectOldVersionAsDifferentFromCurrent()
        {
            // given
            final UpgradableDatabase upgradableDatabase = getUpgradableDatabase();

            // when
            boolean currentVersion = upgradableDatabase.hasCurrentVersion( databaseLayout );

            // then
            assertFalse( currentVersion );
        }

        @Test
        public void shouldCommunicateWhatCausesInabilityToUpgrade()
        {
            // given
            final UpgradableDatabase upgradableDatabase = getUpgradableDatabase();
            try
            {
                // when
                upgradableDatabase.checkUpgradable( databaseLayout );
                fail( "should not have been able to upgrade" );
            }
            catch ( StoreUpgrader.UnexpectedUpgradingStoreVersionException e )
            {
                // then
                assertEquals( String.format( MESSAGE, version, upgradableDatabase.currentVersion(),
                        Version.getNeo4jVersion() ), e.getMessage() );
            }
            catch ( StoreUpgrader.UnexpectedUpgradingStoreFormatException e )
            {
                // then
                assertNotSame( StandardFormatFamily.INSTANCE,
                        RecordFormatSelector.selectForVersion( version ).getFormatFamily());
                assertEquals( String.format( StoreUpgrader.UnexpectedUpgradingStoreFormatException.MESSAGE,
                        GraphDatabaseSettings.record_format.name() ), e.getMessage() );
            }
        }

        private UpgradableDatabase getUpgradableDatabase()
        {
            return new UpgradableDatabase( new RecordStoreVersionCheck( pageCacheRule.getPageCache( fileSystem ) ),
                    getRecordFormat(), tailScanner );
        }
    }

    private static RecordFormats getRecordFormat()
    {
        return Standard.LATEST_RECORD_FORMATS;
    }
}
