/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.backup.stores;

import java.io.File;

/**
 * Simple struct for the pair of databases created by Neo4j by default: system and neo4j
 */
public class DefaultDatabasesBackup
{
    private final File defaultDbBackup;
    private final File systemDbBackup;

    DefaultDatabasesBackup( File defaultDbBackup, File systemDbBackup )
    {
        this.defaultDbBackup = defaultDbBackup;
        this.systemDbBackup = systemDbBackup;
    }

    public File defaultDb()
    {
        return defaultDbBackup;
    }

    public File systemDb()
    {
        return systemDbBackup;
    }
}
