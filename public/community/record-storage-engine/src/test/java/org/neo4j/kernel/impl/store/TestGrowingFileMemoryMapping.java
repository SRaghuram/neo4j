/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.kernel.impl.store;

import org.apache.commons.lang3.SystemUtils;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;

import org.neo4j.configuration.Config;
import org.neo4j.internal.id.DefaultIdGeneratorFactory;
import org.neo4j.io.ByteUnit;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.pagecache.PageCacheExtension;
import org.neo4j.test.rule.TestDirectory;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.kernel.impl.store.record.RecordLoad.NORMAL;

@PageCacheExtension
class TestGrowingFileMemoryMapping
{
    @Inject
    private PageCache pageCache;
    @Inject
    private TestDirectory testDirectory;

    @Test
    void shouldGrowAFileWhileContinuingToMemoryMapNewRegions()
    {
        // don't run on windows because memory mapping doesn't work properly there
        Assumptions.assumeTrue( !SystemUtils.IS_OS_WINDOWS );

        // given
        final int NUMBER_OF_RECORDS = 1000000;

        Config config = Config.defaults();
        DefaultIdGeneratorFactory idGeneratorFactory = new DefaultIdGeneratorFactory( testDirectory.getFileSystem() );
        StoreFactory storeFactory = new StoreFactory( testDirectory.databaseLayout(), config, idGeneratorFactory, pageCache,
                testDirectory.getFileSystem(), NullLogProvider.getInstance() );

        NeoStores neoStores = storeFactory.openAllNeoStores( true );
        NodeStore nodeStore = neoStores.getNodeStore();

        // when
        int iterations = 2 * NUMBER_OF_RECORDS;
        long startingId = nodeStore.nextId();
        long nodeId = startingId;
        for ( int i = 0; i < iterations; i++ )
        {
            NodeRecord record = new NodeRecord( nodeId, false, i, 0 );
            record.setInUse( true );
            nodeStore.updateRecord( record );
            nodeId = nodeStore.nextId();
        }

        // then
        NodeRecord record = new NodeRecord( 0, false, 0, 0 );
        for ( int i = 0; i < iterations; i++ )
        {
            record.setId( startingId + i );
            nodeStore.getRecord( i, record, NORMAL );
            assertTrue( record.inUse(), "record[" + i + "] should be in use" );
            assertThat( "record[" + i + "] should have nextRelId of " + i,
                    record.getNextRel(), is( (long) i ) );
        }

        neoStores.close();
    }

    private static String mmapSize( int numberOfRecords, int recordSize )
    {
        int bytes = numberOfRecords * recordSize;
        long mebiByte = ByteUnit.mebiBytes( 1 );
        if ( bytes < mebiByte )
        {
            throw new IllegalArgumentException( "too few records: " + numberOfRecords );
        }
        return bytes / mebiByte + "M";
    }
}
