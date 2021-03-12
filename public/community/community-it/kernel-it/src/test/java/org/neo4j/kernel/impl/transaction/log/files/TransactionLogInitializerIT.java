/*
 * Copyright (c) "Neo4j"
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
package org.neo4j.kernel.impl.transaction.log.files;

import org.junit.jupiter.api.Test;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.recordstorage.RecordStorageCommandReaderFactory;
import org.neo4j.internal.recordstorage.RecordStorageEngine;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.kernel.impl.store.MetaDataStore;
import org.neo4j.test.extension.DbmsExtension;
import org.neo4j.test.extension.Inject;

import static org.apache.commons.lang3.RandomStringUtils.randomAlphanumeric;
import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.kernel.impl.transaction.log.entry.LogVersions.CURRENT_FORMAT_LOG_HEADER_SIZE;

@DbmsExtension
class TransactionLogInitializerIT
{
    @Inject
    private FileSystemAbstraction fileSystem;
    @Inject
    private RecordStorageEngine recordStorageEngine;
    @Inject
    private GraphDatabaseService database;
    @Inject
    private DatabaseLayout databaseLayout;
    @Inject
    private LogFiles logFiles;

    @Test
    void resetLogFileOffsetOnEmptyLogFileCreation() throws Exception
    {
        try ( Transaction transaction = database.beginTx() )
        {
            Node node = transaction.createNode();
            node.setProperty( "a", randomAlphanumeric( 1024 ) );
            transaction.commit();
        }
        logFiles.stop();
        logFiles.shutdown();
        fileSystem.deleteRecursively( logFiles.logFilesDirectory() );

        MetaDataStore metaDataStore = recordStorageEngine.testAccessNeoStores().getMetaDataStore();
        long offset = metaDataStore.getLastClosedTransaction()[2];
        assertThat( offset ).isGreaterThan( CURRENT_FORMAT_LOG_HEADER_SIZE );

        TransactionLogInitializer logInitializer =
                new TransactionLogInitializer( fileSystem, metaDataStore, RecordStorageCommandReaderFactory.INSTANCE, PageCacheTracer.NULL );
        logInitializer.initializeEmptyLogFile( databaseLayout, databaseLayout.getTransactionLogsDirectory(), "LostFiles" );

        long offsetAfterReset = metaDataStore.getLastClosedTransaction()[2];
        assertThat( offsetAfterReset ).isLessThan( offset );
    }
}
