/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.backup.impl;

import java.util.Collection;

import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.impl.api.CloseableResourceManager;
import org.neo4j.storageengine.api.StorageEngineFactory;

class BackupSupportingClasses implements AutoCloseable
{
    private final BackupDelegator backupDelegator;
    private final CloseableResourceManager closeableResourceManager;
    private final PageCache pageCache;
    private final StorageEngineFactory storageEngineFactory;

    BackupSupportingClasses( BackupDelegator backupDelegator, PageCache pageCache,
            Collection<AutoCloseable> closeables )
    {
        this.backupDelegator = backupDelegator;
        this.pageCache = pageCache;
        this.closeableResourceManager = new CloseableResourceManager();
        this.storageEngineFactory = StorageEngineFactory.selectStorageEngine();
        closeables.forEach( closeableResourceManager::registerCloseableResource );
    }

    public BackupDelegator getBackupDelegator()
    {
        return backupDelegator;
    }

    public PageCache getPageCache()
    {
        return pageCache;
    }

    public StorageEngineFactory getStorageEngineFactory()
    {
        return storageEngineFactory;
    }

    @Override
    public void close()
    {
        closeableResourceManager.closeAllCloseableResources();
    }
}
