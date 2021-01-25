/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.infra;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Path;

/**
 * A data set used during benchmarking.
 *
 */
public interface Dataset
{
    void copyInto( OutputStream newOutputStream ) throws IOException;

    void extractInto( Path dir );
}
