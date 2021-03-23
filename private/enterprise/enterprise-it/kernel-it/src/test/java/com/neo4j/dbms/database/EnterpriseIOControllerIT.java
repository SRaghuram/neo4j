/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.dbms.database;

import com.neo4j.kernel.impl.pagecache.iocontroller.ConfigurableIOController;
import com.neo4j.test.extension.EnterpriseDbmsExtension;
import org.eclipse.collections.api.set.ImmutableSet;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.io.IOException;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.neo4j.collection.Dependencies;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.DelegatingPageCache;
import org.neo4j.io.pagecache.DelegatingPagedFile;
import org.neo4j.io.pagecache.IOController;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.PagedFile;
import org.neo4j.io.pagecache.tracing.cursor.context.VersionContextSupplier;
import org.neo4j.kernel.database.Database;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.ExtensionCallback;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.pagecache.PageCacheSupportExtension;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertFalse;

@EnterpriseDbmsExtension( configurationCallback = "configure" )
class EnterpriseIOControllerIT
{
    @RegisterExtension
    static PageCacheSupportExtension pageCacheExtension = new PageCacheSupportExtension();
    @Inject
    private FileSystemAbstraction fs;
    @Inject
    private IOController ioController;
    @Inject
    private Database database;
    private PageCacheWrapper pageCacheWrapper;

    @ExtensionCallback
    void configure( TestDatabaseManagementServiceBuilder builder )
    {
        Dependencies dependencies = new Dependencies();
        pageCacheWrapper = new PageCacheWrapper( pageCacheExtension.getPageCache( fs ) );
        dependencies.satisfyDependency( pageCacheWrapper );
        builder.setExternalDependencies( dependencies );
    }

    @Test
    void useEnterpriseIOController()
    {
        assertThat( ioController ).isInstanceOf( ConfigurableIOController.class );
    }

    @Test
    void shutdownOfDatabaseShouldFlushWithoutAnyIOLimitations()
    {
        pageCacheWrapper.disabledIOController.set( true );

        assertThat( pageCacheWrapper.ioControllerChecks.get() ).isZero();
        assertDoesNotThrow( () -> database.stop() );

        assertThat( pageCacheWrapper.ioControllerChecks.get() ).isPositive();
        assertThat( ioController.isEnabled() ).isFalse();
    }

    private static class PageCacheWrapper extends DelegatingPageCache
    {
        private final AtomicInteger ioControllerChecks = new AtomicInteger();
        private final AtomicBoolean disabledIOController = new AtomicBoolean();

        PageCacheWrapper( PageCache delegate )
        {
            super( delegate );
        }

        @Override
        public PagedFile map( Path path, int pageSize, String databaseName ) throws IOException
        {
            return new PageFileWrapper( super.map( path, pageSize, databaseName ), IOController.DISABLED, disabledIOController, ioControllerChecks );
        }

        @Override
        public PagedFile map( Path path, int pageSize, String databaseName, ImmutableSet<OpenOption> openOptions ) throws IOException
        {
            return new PageFileWrapper( super.map( path, pageSize, databaseName, openOptions ), IOController.DISABLED, disabledIOController,
                    ioControllerChecks );
        }

        @Override
        public PagedFile map( Path path, VersionContextSupplier versionContextSupplier, int pageSize, String databaseName, ImmutableSet<OpenOption> openOptions,
                IOController ioController ) throws IOException
        {
            return new PageFileWrapper( super.map( path, versionContextSupplier, pageSize, databaseName, openOptions, ioController ), ioController,
                    disabledIOController, ioControllerChecks );
        }
    }

    private static class PageFileWrapper extends DelegatingPagedFile
    {
        private final IOController ioController;
        private final AtomicBoolean disabledIOController;
        private final AtomicInteger ioControllerChecks;

        PageFileWrapper( PagedFile delegate, IOController ioController, AtomicBoolean disabledIOController, AtomicInteger ioControllerChecks )
        {
            super( delegate );
            this.ioController = ioController;
            this.disabledIOController = disabledIOController;
            this.ioControllerChecks = ioControllerChecks;
        }

        @Override
        public void flushAndForce() throws IOException
        {
            if ( disabledIOController.get() )
            {
                assertFalse( ioController.isEnabled() );
                ioControllerChecks.incrementAndGet();
            }
            super.flushAndForce();
        }
    }

}
