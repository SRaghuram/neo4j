/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.kernel.api.impl.bloom.surface;

import com.neo4j.kernel.api.impl.bloom.FulltextIndexType;
import com.neo4j.kernel.api.impl.bloom.FulltextProviderImpl;

import java.io.File;
import java.io.IOException;
import java.util.function.Supplier;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.AvailabilityGuard;
import com.neo4j.kernel.api.impl.bloom.FulltextProvider;

import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.logging.LogService;
import org.neo4j.kernel.impl.proc.Procedures;
import org.neo4j.kernel.impl.transaction.log.TransactionIdStore;
import org.neo4j.kernel.impl.transaction.state.NeoStoreFileListing;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.logging.Log;
import org.neo4j.scheduler.JobScheduler;

import static com.neo4j.kernel.api.impl.bloom.surface.BloomKernelExtensionFactory.BLOOM_NODES;
import static com.neo4j.kernel.api.impl.bloom.surface.BloomKernelExtensionFactory.BLOOM_RELATIONSHIPS;

class BloomKernelExtension extends LifecycleAdapter
{
    private final File storeDir;
    private final Config config;
    private final FileSystemAbstraction fileSystem;
    private final GraphDatabaseService db;
    private final Procedures procedures;
    private final LogService logService;
    private final AvailabilityGuard availabilityGuard;
    private final JobScheduler scheduler;
    private final Supplier<TransactionIdStore> transactionIdStore;
    private final Supplier<NeoStoreFileListing> fileListing;
    private FulltextProvider provider;

    BloomKernelExtension( FileSystemAbstraction fileSystem, File storeDir, Config config, GraphDatabaseService db, Procedures procedures, LogService logService,
            AvailabilityGuard availabilityGuard, JobScheduler scheduler, Supplier<TransactionIdStore> transactionIdStore,
            Supplier<NeoStoreFileListing> fileListing )
    {
        this.storeDir = storeDir;
        this.config = config;
        this.fileSystem = fileSystem;
        this.db = db;
        this.procedures = procedures;
        this.logService = logService;
        this.availabilityGuard = availabilityGuard;
        this.scheduler = scheduler;
        this.transactionIdStore = transactionIdStore;
        this.fileListing = fileListing;
    }

    @Override
    public void start() throws IOException
    {
        if ( config.get( BloomFulltextConfig.bloom_enabled ) )
        {
            String analyzer = config.get( BloomFulltextConfig.bloom_default_analyzer );

            Log log = logService.getInternalLog( FulltextProviderImpl.class );
            provider = new FulltextProviderImpl( db, log, availabilityGuard, scheduler, transactionIdStore.get(),
                    fileSystem, storeDir, analyzer );
            provider.openIndex( BLOOM_NODES, FulltextIndexType.NODES );
            provider.openIndex( BLOOM_RELATIONSHIPS, FulltextIndexType.RELATIONSHIPS );
            provider.registerFileListing( fileListing.get() );

            procedures.registerComponent( FulltextProvider.class, context -> provider, true );
        }
        else
        {
            procedures.registerComponent( FulltextProvider.class, context -> FulltextProvider.NULL_PROVIDER, true );
        }
    }

    @Override
    public void stop() throws Exception
    {
        if ( provider != null )
        {
            provider.close();
        }
    }
}
