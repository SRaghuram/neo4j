/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.com.storecopy;

import java.io.File;
import java.io.IOException;
import java.nio.file.OpenOption;
import java.util.List;
import java.util.Optional;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.facade.GraphDatabaseFacadeFactory;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.graphdb.factory.module.PlatformModule;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.IOLimiter;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.PagedFile;
import org.neo4j.io.pagecache.tracing.cursor.context.VersionContextSupplier;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.configuration.Settings;
import org.neo4j.kernel.extension.KernelExtensionFactory;
import org.neo4j.kernel.impl.enterprise.EnterpriseEditionModule;
import org.neo4j.kernel.impl.factory.DatabaseInfo;
import org.neo4j.kernel.monitoring.tracing.Tracers;
import org.neo4j.logging.internal.LogService;
import org.neo4j.scheduler.JobScheduler;

/**
 * A PageCache implementation that delegates to another page cache, whose life cycle is managed elsewhere.
 *
 * This page cache implementation DOES NOT delegate close() method calls, so it can be used to safely share a page
 * cache with a component that might try to close the page cache it gets.
 */
public class ExternallyManagedPageCache implements PageCache
{
    private final PageCache delegate;

    private ExternallyManagedPageCache( PageCache delegate )
    {
        this.delegate = delegate;
    }

    @Override
    public void close()
    {
        // Don't close the delegate, because we are not in charge of its life cycle.
    }

    @Override
    public PagedFile map( File file, int pageSize, OpenOption... openOptions ) throws IOException
    {
        return delegate.map( file, pageSize, openOptions );
    }

    @Override
    public Optional<PagedFile> getExistingMapping( File file ) throws IOException
    {
        return delegate.getExistingMapping( file );
    }

    @Override
    public List<PagedFile> listExistingMappings() throws IOException
    {
        return delegate.listExistingMappings();
    }

    @Override
    public void flushAndForce() throws IOException
    {
        delegate.flushAndForce();
    }

    @Override
    public void flushAndForce( IOLimiter limiter ) throws IOException
    {
        delegate.flushAndForce( limiter );
    }

    @Override
    public int pageSize()
    {
        return delegate.pageSize();
    }

    @Override
    public long maxCachedPages()
    {
        return delegate.maxCachedPages();
    }

    @Override
    public void reportEvents()
    {
        delegate.reportEvents();
    }

    /**
     * Create a GraphDatabaseFactory that will build EmbeddedGraphDatabase instances that all use the given page cache.
     */
    public static GraphDatabaseFactoryWithPageCacheFactory graphDatabaseFactoryWithPageCache(
            final PageCache delegatePageCache )
    {
        return new GraphDatabaseFactoryWithPageCacheFactory( delegatePageCache );
    }

    public static class GraphDatabaseFactoryWithPageCacheFactory extends GraphDatabaseFactory
    {
        private final PageCache delegatePageCache;

        GraphDatabaseFactoryWithPageCacheFactory( PageCache delegatePageCache )
        {
            this.delegatePageCache = delegatePageCache;
        }

        @Override
        protected GraphDatabaseService newDatabase( File storeDir, Config config,
                GraphDatabaseFacadeFactory.Dependencies dependencies )
        {
            File absoluteStoreDir = storeDir.getAbsoluteFile();
            File databasesRoot = absoluteStoreDir.getParentFile();
            config.augment( GraphDatabaseSettings.ephemeral, Settings.FALSE );
            config.augment( GraphDatabaseSettings.active_database, absoluteStoreDir.getName() );
            config.augment( GraphDatabaseSettings.databases_root_path, databasesRoot.getAbsolutePath() );
            return new GraphDatabaseFacadeFactory( DatabaseInfo.ENTERPRISE, EnterpriseEditionModule::new )
            {
                @Override
                protected PlatformModule createPlatform( File storeDir, Config config, Dependencies dependencies )
                {
                    return new PlatformModule( storeDir, config, databaseInfo, dependencies )
                    {
                        @Override
                        protected PageCache createPageCache( FileSystemAbstraction fileSystem, Config config,
                                LogService logging, Tracers tracers, VersionContextSupplier versionContextSupplier, JobScheduler jobScheduler )
                        {
                            return new ExternallyManagedPageCache( delegatePageCache );
                        }
                    };
                }
            }.newFacade( databasesRoot, config, dependencies );
        }

        public GraphDatabaseFactoryWithPageCacheFactory setKernelExtensions( Iterable<KernelExtensionFactory<?>> newKernelExtensions )
        {
            getCurrentState().setKernelExtensions( newKernelExtensions );
            return this;
        }
    }
}
