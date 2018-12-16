/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.backup.impl;

import java.nio.file.Path;
import java.util.Optional;

import org.neo4j.kernel.impl.util.OptionalHostnamePort;

class OnlineBackupRequiredArguments
{
    private final OptionalHostnamePort address;
    private final String databaseName;
    private final Path directory;
    private final String name;
    private final boolean fallbackToFull;
    private final boolean doConsistencyCheck;
    private final Path reportDir;

    OnlineBackupRequiredArguments( OptionalHostnamePort address, String databaseName, Path directory, String name,
            boolean fallbackToFull, boolean doConsistencyCheck, Path reportDir )
    {
        this.address = address;
        this.databaseName = databaseName;
        this.directory = directory;
        this.name = name;
        this.fallbackToFull = fallbackToFull;
        this.doConsistencyCheck = doConsistencyCheck;
        this.reportDir = reportDir;
    }

    public OptionalHostnamePort getAddress()
    {
        return address;
    }

    public Optional<String> getDatabaseName()
    {
        return Optional.ofNullable( databaseName );
    }

    public Path getDirectory()
    {
        return directory;
    }

    public String getName()
    {
        return name;
    }

    public boolean isFallbackToFull()
    {
        return fallbackToFull;
    }

    public boolean isDoConsistencyCheck()
    {
        return doConsistencyCheck;
    }

    public Path getReportDir()
    {
        return reportDir;
    }

    public Path getResolvedLocationFromName()
    {
        return directory.resolve( name );
    }
}
