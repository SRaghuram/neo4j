/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.common.profiling;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static java.lang.String.format;

/**
 * Helper class, which writes collapsed stack frames in a format that can be later digested by <a href="https://github.com/brendangregg/FlameGraph">Brendan Greg's Flamegraph</a>.
 * Collapsed stack frames are, stack frames written in a single line where each frame is
 * separated by ';', and bottom frame is first frame in a line.
 * At the end of each line there is single numeric value (it can represent time, memory usage, etc), which represents measured value.
 *
 * For example:
 * <pre>
 * Main#main(String[]);java.lang.String#indexOf(int) 500
 * </pre>
 * WARNING: same stack traces have to be summed up, you can't have same stack traces in collapsed stack frame.
 *
 */
public class StackCollapseWriter
{
    public static void write( StackCollapse stackCollapse, Path file )
    {
        try ( PrintWriter printWriter = new PrintWriter( Files.newOutputStream( file ), true /*auto flush*/ ) )
        {
            stackCollapse.forEachStackTrace( ( stackTrace, sum ) -> printWriter.println( format( "%s %d", stackTrace, sum ) ) );
        }
        catch ( IOException e )
        {
            throw new UncheckedIOException( "Error trying to write stack collapse to file", e );
        }
    }
}
