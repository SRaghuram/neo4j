/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.tools.dump;

import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.file.Path;

import org.neo4j.index.internal.gbptree.GBPTree;
import org.neo4j.index.internal.gbptree.GBPTreeBuilder;
import org.neo4j.io.pagecache.IOController;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.impl.index.schema.TokenScanLayout;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.pagecache.PageCacheExtension;
import org.neo4j.test.rule.TestDirectory;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer.NULL;

@PageCacheExtension
class GBPTreeDumpToolTest
{
    @Inject
    TestDirectory dir;
    @Inject
    PageCache pageCache;

    @Test
    void shouldPrintSomething() throws Exception
    {
        // Given a tree
        Path file = dir.file( "index" );
        try ( GBPTree<?,?> tree = new GBPTreeBuilder<>( pageCache, file, new TokenScanLayout() ).build() )
        {
            tree.checkpoint( IOController.DISABLED, NULL );
        }

        // When dumping
        final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try ( PrintStream ps = new PrintStream( baos, true, UTF_8.name() ) )
        {
            new GBPTreeDumpTool().run( file, ps );
        }
        String output = baos.toString( UTF_8 );

        // Then should print stuff to print stream
        assertThat( output ).contains( "Dump tree " + file.toAbsolutePath() );
        assertThat( output ).contains( "Level 0" );
    }
}
