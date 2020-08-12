/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.infra;

import com.neo4j.bench.model.profiling.RecordingType;

import java.io.File;
import java.io.FileFilter;
import java.util.Arrays;

public class IgnoreProfilerFileFilter implements FileFilter
{
    @Override
    public boolean accept( File pathname )
    {
        return Arrays.stream( RecordingType.values() )
                     .noneMatch( r -> pathname.toString().endsWith( r.extension() ) );
    }
}
