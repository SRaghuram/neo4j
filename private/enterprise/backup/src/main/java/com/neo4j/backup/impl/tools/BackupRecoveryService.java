/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.backup.impl.tools;

import java.io.IOException;

import org.neo4j.configuration.Config;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.database.DatabaseTracers;
import org.neo4j.memory.MemoryTracker;

import static org.neo4j.kernel.recovery.Recovery.performRecovery;

public class BackupRecoveryService
{
    private final FileSystemAbstraction fs;
    private final PageCache pageCache;
    private final Config config;
    private final MemoryTracker memoryTracker;

    public BackupRecoveryService( FileSystemAbstraction fs, PageCache pageCache, Config config, MemoryTracker memoryTracker )
    {
        this.fs = fs;
        this.pageCache = pageCache;
        this.config = config;
        this.memoryTracker = memoryTracker;
    }

    public void recover( DatabaseLayout databaseLayout ) throws IOException
    {
        performRecovery( fs, pageCache, DatabaseTracers.EMPTY, config, databaseLayout, memoryTracker );
    }
}
