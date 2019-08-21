/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.common.profiling;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

import static java.lang.String.format;

public class StackCollapseWriter
{
    public static void write( StackCollapse stackCollapse, Path file )
    {
        try ( PrintWriter printWriter = new PrintWriter( Files.newOutputStream( file, StandardOpenOption.SYNC ), true /*auto flush*/ ) )
        {
            stackCollapse.forEachStackTrace( ( stackTrace, sum ) -> printWriter.println( format( "%s %d", stackTrace, sum ) ) );
        }
        catch ( IOException e )
        {
            throw new UncheckedIOException( "Error trying to write stack collapse to file", e );
        }
    }
}
