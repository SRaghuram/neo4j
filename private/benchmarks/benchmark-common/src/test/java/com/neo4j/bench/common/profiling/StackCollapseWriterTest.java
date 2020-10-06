/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.common.profiling;

import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;

@TestDirectoryExtension
public class StackCollapseWriterTest
{
    @Inject
    public TestDirectory temporaryFolder;

    @Test
    public void writeStackCollapse() throws IOException
    {
        // given
        Path file = temporaryFolder.file( "stackcollapse" );
        ImmutableMap<String,Long> map = ImmutableMap.of( "stack1", 500L, "stack1;stack2", 600L );
        StackCollapse stackCollapse =
                consumer -> map.entrySet().forEach( entry -> consumer.accept( entry.getKey(), entry.getValue() ) );
        // when
        StackCollapseWriter.write( stackCollapse, file );
        // then
        assertThat( Files.readAllLines( file ), containsInAnyOrder( "stack1 500", "stack1;stack2 600" ) );
    }
}
