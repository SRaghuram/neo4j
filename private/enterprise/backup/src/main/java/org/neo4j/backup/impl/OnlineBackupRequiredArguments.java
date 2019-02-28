/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.backup.impl;

import java.nio.file.Path;
import java.util.Optional;

import org.neo4j.helpers.AdvertisedSocketAddress;

class OnlineBackupRequiredArguments
{
    private final AdvertisedSocketAddress address;
    private final String databaseName;
    private final Path databaseBackupDir;
    private final boolean fallbackToFull;
    private final boolean doConsistencyCheck;
    private final Path reportDir;

    OnlineBackupRequiredArguments( AdvertisedSocketAddress address, String databaseName, Path databaseBackupDir,
            boolean fallbackToFull, boolean doConsistencyCheck, Path reportDir )
    {
        this.address = address;
        this.databaseName = databaseName;
        this.databaseBackupDir = databaseBackupDir;
        this.fallbackToFull = fallbackToFull;
        this.doConsistencyCheck = doConsistencyCheck;
        this.reportDir = reportDir;
    }

    public AdvertisedSocketAddress getAddress()
    {
        return address;
    }

    public Optional<String> getDatabaseName()
    {
        return Optional.ofNullable( databaseName );
    }

    public Path getDatabaseBackupDir()
    {
        return databaseBackupDir;
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
}
