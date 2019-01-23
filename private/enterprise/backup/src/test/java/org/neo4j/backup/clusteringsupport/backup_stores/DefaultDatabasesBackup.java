/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.backup.clusteringsupport.backup_stores;

import java.io.File;

/**
 * Simple struct for the pair of databases created by Neo4j by default: system.db and graph.db
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
