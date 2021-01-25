/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.backup.impl;

import java.io.IOException;

import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.impl.pagecache.ConfigurableStandalonePageCacheFactory;
import org.neo4j.kernel.impl.transaction.log.files.LogFiles;
import org.neo4j.kernel.impl.transaction.log.files.LogFilesBuilder;
import org.neo4j.test.scheduler.ThreadPoolJobScheduler;

import static org.neo4j.io.pagecache.tracing.PageCacheTracer.NULL;

class BackupTransactionLogFilesHelper
{
    static LogFiles readLogFiles( DatabaseLayout databaseLayout ) throws IOException
    {
        FileSystemAbstraction fileSystemAbstraction = new DefaultFileSystemAbstraction();
        PageCache pageCache = ConfigurableStandalonePageCacheFactory.createPageCache( fileSystemAbstraction, new ThreadPoolJobScheduler(), NULL );
        return LogFilesBuilder.activeFilesBuilder( databaseLayout, fileSystemAbstraction, pageCache ).build();
    }
}
