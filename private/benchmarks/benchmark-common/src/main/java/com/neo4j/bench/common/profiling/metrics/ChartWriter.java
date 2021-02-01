/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.common.profiling.metrics;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.util.List;

public class ChartWriter
{
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    public static void write( List<Chart> charts, Path target )
    {
        try
        {
            OBJECT_MAPPER.writeValue( target.toFile(), charts );
        }
        catch ( IOException e )
        {
            throw new UncheckedIOException( e );
        }
    }
}
