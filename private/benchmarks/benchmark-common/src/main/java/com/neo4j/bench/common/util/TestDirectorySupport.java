/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.common.util;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Objects;

public final class TestDirectorySupport
{
    public static Path createTempFilePath( File temporaryFolder ) throws IOException
    {
        Objects.requireNonNull( temporaryFolder, "temporary folder cannot be null" );
        return Files.createTempFile( temporaryFolder.toPath(), "", "" );
    }

    public static File createTempFile( File temporaryFolder ) throws IOException
    {
        return createTempFilePath( temporaryFolder ).toFile();
    }

    public static Path createTempDirectoryPath( File temporaryFolder ) throws IOException
    {
        Objects.requireNonNull( temporaryFolder, "temporary folder cannot be null" );
        return Files.createTempDirectory( temporaryFolder.toPath(), "" );
    }

    public static File createTempDirectory( File temporaryFolder ) throws IOException
    {
        return createTempDirectoryPath( temporaryFolder ).toFile();
    }
}
