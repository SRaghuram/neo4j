/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.common.profiling;

import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.io.UncheckedIOException;
import java.nio.file.Path;

import static java.lang.String.format;

public class StackCollapseWriter
{
    public static void write( StackCollapse stackCollapse, Path file )
    {
        try ( PrintWriter printWriter = new PrintWriter( file.toFile() ) )
        {
            stackCollapse.forEachStackTrace( ( stackTrace, sum ) -> printWriter.println( format( "%s %d", stackTrace, sum ) ) );
        }
        catch ( FileNotFoundException e )
        {
            throw new UncheckedIOException( "Error trying to write stack collapse to file", e );
        }
    }
}
