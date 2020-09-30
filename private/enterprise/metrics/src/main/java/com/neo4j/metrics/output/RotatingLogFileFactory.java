/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.metrics.output;

import java.nio.file.Path;

import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.logging.log4j.RotatingLogFileWriter;

@FunctionalInterface
public interface RotatingLogFileFactory
{
    RotatingLogFileWriter createWriter( FileSystemAbstraction fs, Path logPath, long rotationThreshold, int maxArchives, String fileSuffix, String header );
}
