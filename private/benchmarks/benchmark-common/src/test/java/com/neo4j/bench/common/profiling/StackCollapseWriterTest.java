/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.common.profiling;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertThat;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import com.google.common.collect.ImmutableMap;

public class StackCollapseWriterTest
{
    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    @Test
    public void writeStackCollapse() throws IOException
    {
        // given
        File file = temporaryFolder.newFile();
        ImmutableMap<String,Long> map = ImmutableMap.of( "stack1", 500L, "stack1;stack2", 600L );
        StackCollapse stackCollapse =
                consumer -> map.entrySet().forEach( entry -> consumer.accept( entry.getKey(), entry.getValue() ) );
        // when
        StackCollapseWriter.write( stackCollapse, file.toPath() );
        // then
        assertThat( Files.readAllLines( file.toPath() ), containsInAnyOrder( "stack1 500", "stack1;stack2 600" ) );
    }
}
