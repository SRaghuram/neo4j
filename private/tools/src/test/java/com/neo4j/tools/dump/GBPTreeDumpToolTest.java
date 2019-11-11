/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.tools.dump;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.File;

import org.neo4j.index.internal.gbptree.GBPTreeBuilder;
import org.neo4j.internal.index.label.LabelScanLayout;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.SuppressOutputExtension;
import org.neo4j.test.extension.pagecache.PageCacheExtension;
import org.neo4j.test.rule.SuppressOutput;
import org.neo4j.test.rule.TestDirectory;

import static org.junit.jupiter.api.Assertions.assertTrue;

@ExtendWith( SuppressOutputExtension.class )
@PageCacheExtension
class GBPTreeDumpToolTest
{
    @Inject
    SuppressOutput suppressOutput;
    @Inject
    TestDirectory dir;
    @Inject
    FileSystemAbstraction fs;
    @Inject
    PageCache pageCache;

    @Test
    void shouldPrintSomething() throws Exception
    {
        // Given a tree
        File file = dir.file( "index" );
        new GBPTreeBuilder<>( pageCache, file, new LabelScanLayout() ).build().close();

        // When dumping
        GBPTreeDumpTool dumpTool = new GBPTreeDumpTool();
        dumpTool.run( file );

        // Then should print stuff
        SuppressOutput.Voice outputVoice = suppressOutput.getOutputVoice();
        assertTrue( outputVoice.containsMessage( "Dump tree " + file.getAbsolutePath() ) );
        assertTrue( outputVoice.containsMessage( "Level 0" ) );
    }
}
