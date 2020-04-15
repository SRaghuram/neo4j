/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.internal.tokenstore;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.neo4j.internal.id.DefaultIdGeneratorFactory;
import org.neo4j.internal.id.IdCapacityExceededException;
import org.neo4j.internal.id.IdGeneratorFactory;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.IOLimiter;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.RandomExtension;
import org.neo4j.test.extension.pagecache.PageCacheExtension;
import org.neo4j.test.rule.RandomRule;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.token.api.NamedToken;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.neo4j.index.internal.gbptree.RecoveryCleanupWorkCollector.immediate;
import static org.neo4j.internal.id.IdType.LABEL_TOKEN;

@PageCacheExtension
@ExtendWith( RandomExtension.class )
class GBPTreeTokenStoreTest
{
    @Inject
    private TestDirectory testDirectory;
    @Inject
    private FileSystemAbstraction fileSystem;
    @Inject
    private PageCache pageCache;
    @Inject
    private RandomRule random;

    @Test
    void shouldCreateAndLoadTokens() throws IOException
    {
        // given
        try ( GBPTreeTokenStore store = new GBPTreeTokenStore( pageCache, testDirectory.file( "store" ), immediate(),
                new DefaultIdGeneratorFactory( fileSystem, immediate() ), LABEL_TOKEN, Short.MAX_VALUE, false, PageCacheTracer.NULL, PageCursorTracer.NULL ) )
        {
            Set<NamedToken> expected = new HashSet<>();
            for ( int i = 0; i < 10; i++ )
            {
                int id = store.nextTokenId( PageCursorTracer.NULL );
                NamedToken token = new NamedToken( "Token_" + id, id );
                store.writeToken( token, PageCursorTracer.NULL );
                expected.add( token );
            }

            // when
            Set<NamedToken> found = new HashSet<>();
            store.loadTokens( PageCursorTracer.NULL ).forEach( found::add );

            // then
            assertEquals( expected, found );
            for ( NamedToken expectedToken : expected )
            {
                NamedToken loadedToken = store.loadToken( expectedToken.id(), PageCursorTracer.NULL );
                assertThat( loadedToken ).isEqualTo( expectedToken );
            }
        }
    }

    @Test
    void shouldNotGoBeyondMaxId() throws IOException
    {
        // given
        int maxId = Short.MAX_VALUE;
        try ( GBPTreeTokenStore store = new GBPTreeTokenStore( pageCache, testDirectory.file( "store" ), immediate(),
                new DefaultIdGeneratorFactory( fileSystem, immediate() ), LABEL_TOKEN, maxId, false, PageCacheTracer.NULL, PageCursorTracer.NULL ) )
        {
            store.setHighId( maxId );
            assertEquals( maxId, store.nextTokenId( PageCursorTracer.NULL ) );

            // when/then
            assertThrows( IdCapacityExceededException.class, () -> store.nextTokenId( PageCursorTracer.NULL ) );
        }
    }

    @Test
    void shouldKeepTokensBetweenRestarts() throws IOException
    {
        // given
        List<NamedToken> expected = new ArrayList<>();
        IdGeneratorFactory idGeneratorFactory = new DefaultIdGeneratorFactory( fileSystem, immediate() );
        try ( GBPTreeTokenStore store = new GBPTreeTokenStore( pageCache, testDirectory.file( "store" ), immediate(), idGeneratorFactory, LABEL_TOKEN,
                Short.MAX_VALUE, false, PageCacheTracer.NULL, PageCursorTracer.NULL ) )
        {
            int id = 0;
            for ( int i = 0; i < 100; i++ )
            {
                NamedToken token = new NamedToken( "Token_" + id, id, false );
                store.writeToken( token, PageCursorTracer.NULL );
                id += random.nextInt( 1, 5 );
                expected.add( token );
            }

            // when
            store.checkpoint( IOLimiter.UNLIMITED, PageCursorTracer.NULL );
        }

        // then
        try ( GBPTreeTokenStore store = new GBPTreeTokenStore( pageCache, testDirectory.file( "store" ), immediate(), idGeneratorFactory, LABEL_TOKEN,
                Short.MAX_VALUE, false, PageCacheTracer.NULL, PageCursorTracer.NULL ) )
        {
            assertEquals( expected, store.loadTokens( PageCursorTracer.NULL ) );
        }
    }
}
