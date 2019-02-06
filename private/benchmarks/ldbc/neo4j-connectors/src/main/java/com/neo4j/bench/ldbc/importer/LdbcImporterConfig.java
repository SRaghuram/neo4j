/**
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.ldbc.importer;

import org.neo4j.unsafe.impl.batchimport.Configuration;

public class LdbcImporterConfig implements Configuration
{
    private final int denseNodeThreshold;

    public LdbcImporterConfig( int denseNodeThreshold )
    {
        this.denseNodeThreshold = denseNodeThreshold;
    }

    @Override
    public int denseNodeThreshold()
    {
        return denseNodeThreshold;
    }
}
