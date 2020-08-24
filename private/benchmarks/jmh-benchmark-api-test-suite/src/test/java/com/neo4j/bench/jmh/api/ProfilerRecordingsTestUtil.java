/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.jmh.api;

import com.neo4j.bench.common.profiling.RecordingDescriptor;
import com.neo4j.bench.common.results.BenchmarkDirectory;
import com.neo4j.bench.common.results.BenchmarkGroupDirectory;
import com.neo4j.bench.common.results.ForkDirectory;
import com.neo4j.bench.common.util.BenchmarkUtil;
import com.neo4j.bench.model.profiling.RecordingType;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

class ProfilerRecordingsTestUtil
{
    static int recordingCountIn( Path dir, RecordingType recordingType )
    {
        BenchmarkUtil.assertDirectoryExists( dir );
        AtomicInteger count = new AtomicInteger();
        for ( BenchmarkGroupDirectory groupDir : BenchmarkGroupDirectory.searchAllIn( dir ) )
        {
            for ( BenchmarkDirectory benchDir : groupDir.benchmarkDirectories() )
            {
                for ( ForkDirectory forkDir : benchDir.forks() )
                {
                    Map<RecordingDescriptor,Path> recordings = forkDir.recordings();
                    recordings.forEach( ( descriptor, recording ) ->
                                        {
                                            if ( descriptor.recordingType().equals( recordingType ) && Files.exists( recording ) )
                                            {
                                                count.incrementAndGet();
                                            }
                                        } );
                }
            }
        }
        return count.get();
    }
}
