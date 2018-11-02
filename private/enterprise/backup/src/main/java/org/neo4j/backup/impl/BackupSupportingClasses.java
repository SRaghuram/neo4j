/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.backup.impl;

import java.util.Collection;

import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.impl.api.CloseableResourceManager;

class BackupSupportingClasses implements AutoCloseable
{
    // Strategies
    private final BackupDelegator backupDelegator;
    private final BackupProtocolService backupProtocolService;
    private final CloseableResourceManager closeableResourceManager;

    // Dependency Helpers
    private final PageCache pageCache;

    BackupSupportingClasses( BackupDelegator backupDelegator, BackupProtocolService backupProtocolService, PageCache pageCache,
            Collection<AutoCloseable> closeables )
    {
        this.backupDelegator = backupDelegator;
        this.backupProtocolService = backupProtocolService;
        this.pageCache = pageCache;
        this.closeableResourceManager = new CloseableResourceManager();
        closeables.forEach( closeableResourceManager::registerCloseableResource );
    }

    public BackupDelegator getBackupDelegator()
    {
        return backupDelegator;
    }

    public BackupProtocolService getBackupProtocolService()
    {
        return backupProtocolService;
    }

    public PageCache getPageCache()
    {
        return pageCache;
    }

    @Override
    public void close()
    {
        closeableResourceManager.closeAllCloseableResources();
    }
}
