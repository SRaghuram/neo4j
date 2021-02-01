/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.common.profiling.metrics;

import org.junit.jupiter.api.Test;
import org.skyscreamer.jsonassert.JSONAssert;
import org.skyscreamer.jsonassert.JSONCompareMode;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.List;

import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

@TestDirectoryExtension
public class ChartWriterTest
{
    @Inject
    private TestDirectory temporaryFolder;

    @Test
    public void shouldWriteChartAsJson() throws Exception
    {
        List<Chart> charts = Collections.singletonList( new Chart( new Layout( "Chart Title", new Axis( "y-axis title" ) ), Collections.singletonList(
                new Series( "Series 1", Collections.singletonList( 1605782001000L ), Collections.singletonList( 1.5 ) )
        ) ) );

        Path target = temporaryFolder.createFile( "temp.json" );
        ChartWriter.write( charts, target );

        String actualJson = readString( target );
        String expectedJson = readString( Paths.get( getClass().getResource( "/charts/plots.json" ).toURI() ) );
        JSONAssert.assertEquals( expectedJson, actualJson, JSONCompareMode.STRICT );
    }

    private String readString( Path path ) throws IOException
    {
        return Files.readString( path );
    }
}
