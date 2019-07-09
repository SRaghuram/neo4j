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
package org.neo4j.batchinsert.internal;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.IOException;

import org.neo4j.batchinsert.BatchInserter;
import org.neo4j.batchinsert.BatchInserters;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.layout.StoreLayout;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.impl.muninn.MuninnPageCache;
import org.neo4j.kernel.StoreLockException;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.internal.locker.StoreLocker;
import org.neo4j.test.ReflectionUtil;
import org.neo4j.test.extension.DefaultFileSystemExtension;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.startsWith;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.neo4j.internal.helpers.collection.MapUtil.stringMap;
import static org.neo4j.io.ByteUnit.kibiBytes;

@ExtendWith( {DefaultFileSystemExtension.class, TestDirectoryExtension.class} )
class BatchInserterImplTest
{
    @Inject
    private TestDirectory testDirectory;
    @Inject
    private FileSystemAbstraction fileSystem;

    @Test
    void testHonorsPassedInParams() throws Exception
    {
        BatchInserter inserter = BatchInserters.inserter( testDirectory.databaseLayout(), fileSystem,
                stringMap( GraphDatabaseSettings.pagecache_memory.name(), "280K" ) );
        NeoStores neoStores = ReflectionUtil.getPrivateField( inserter, "neoStores", NeoStores.class );
        PageCache pageCache = ReflectionUtil.getPrivateField( neoStores, "pageCache", PageCache.class );
        inserter.shutdown();
        long mappedMemoryTotalSize = MuninnPageCache.memoryRequiredForPages( pageCache.maxCachedPages() );
        assertThat( "memory mapped config is active", mappedMemoryTotalSize,
                is( allOf( greaterThan( kibiBytes( 270 ) ), lessThan( kibiBytes( 290 ) ) ) ) );
    }

    @Test
    void testCreatesStoreLockFile() throws Exception
    {
        // Given
        DatabaseLayout databaseLayout = testDirectory.databaseLayout();

        // When
        BatchInserter inserter = BatchInserters.inserter( databaseLayout, fileSystem );

        // Then
        assertThat( databaseLayout.getStoreLayout().storeLockFile().exists(), equalTo( true ) );
        inserter.shutdown();
    }

    @Test
    void testFailsOnExistingStoreLockFile() throws IOException
    {
        // Given
        StoreLayout storeLayout = testDirectory.storeLayout();
        try ( StoreLocker lock = new StoreLocker( fileSystem, storeLayout ) )
        {
            lock.checkLock();

            var e = assertThrows( StoreLockException.class,
                () -> BatchInserters.inserter( storeLayout.databaseLayout( "any" ), fileSystem ) );
            assertThat( e.getMessage(), startsWith( "Unable to obtain lock on store lock file" ) );
        }
    }
}
