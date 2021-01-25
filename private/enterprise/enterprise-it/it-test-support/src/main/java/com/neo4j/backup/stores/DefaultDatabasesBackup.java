/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.backup.stores;

import java.nio.file.Path;

/**
 * Simple struct for the pair of databases created by Neo4j by default: system and neo4j
 */
public class DefaultDatabasesBackup
{
    private final Path defaultDbBackup;
    private final Path systemDbBackup;

    DefaultDatabasesBackup( Path defaultDbBackup, Path systemDbBackup )
    {
        this.defaultDbBackup = defaultDbBackup;
        this.systemDbBackup = systemDbBackup;
    }

    public Path defaultDb()
    {
        return defaultDbBackup;
    }

    public Path systemDb()
    {
        return systemDbBackup;
    }
}
