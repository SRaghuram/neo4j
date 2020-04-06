/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.jmh.api;

import com.neo4j.bench.model.profiling.RecordingType;
import com.neo4j.bench.common.util.BenchmarkUtil;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.stream.Stream;

import static java.lang.String.format;

class ProfilerRecordingsTestUtil
{
    static int recordingCountIn( Path dir, RecordingType recordingType )
    {
        BenchmarkUtil.assertDirectoryExists( dir );
        try ( Stream<Path> paths = Files.walk( dir ) )
        {
            return (int) paths
                    .filter( Files::isRegularFile )
                    .filter( file -> file.toString().endsWith( recordingType.extension() ) )
                    .count();
        }
        catch ( IOException e )
        {
            throw new UncheckedIOException( format( "Error counting number of '%s' in: %s", recordingType, dir.toAbsolutePath() ), e );
        }
    }
}
