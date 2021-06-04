/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.internal.batchimport;

import org.neo4j.annotations.service.Service;
import org.neo4j.configuration.Config;
import org.neo4j.internal.batchimport.input.Collector;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.logging.log4j.Log4jLogProvider;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.service.NamedService;
import org.neo4j.service.Services;

import java.io.PrintStream;
import java.util.Collection;
import java.util.NoSuchElementException;


@Service
public abstract class BaseBatchImporterFactory implements NamedService
{
    public final int priority;

    @Override
    public String getName()
    {
        return "standard";
    }

    protected BaseBatchImporterFactory( int priority )
    {
        this.priority = priority;
    }

    public BatchImporter instantiate(DatabaseLayout directoryStructure,
                                              FileSystemAbstraction fileSystem,
                                              PageCache externalPageCache,
                                              PageCacheTracer pageCacheTracer,
                                              Configuration configuration,
                                              Log4jLogProvider logProvider,
                                              Config dbConfig,
                                              boolean verbose,
                                              JobScheduler jobScheduler,
                                              Collector badCollector,
                                              MemoryTracker memoryTracker,
                                              PrintStream stdOut, PrintStream stdErr)
    {
        return instantiate( directoryStructure,
                 fileSystem,
                 externalPageCache,
                 pageCacheTracer,
                 configuration,
                 logProvider,
                 dbConfig,
         verbose,
         jobScheduler,
         badCollector,
         memoryTracker,
         stdOut,  stdErr, null);
    }
    public abstract BatchImporter instantiate(DatabaseLayout directoryStructure,
                                              FileSystemAbstraction fileSystem,
                                              PageCache externalPageCache,
                                              PageCacheTracer pageCacheTracer,
                                              Configuration configuration,
                                              Log4jLogProvider logProvider,
                                              Config dbConfig,
                                              boolean verbose,
                                              JobScheduler jobScheduler,
                                              Collector badCollector,
                                              MemoryTracker memoryTracker,
                                              PrintStream stdOut, PrintStream stdErr, String graphName);


    //public boolean forStorageType( StorageEngineType storageEngineType)
    //{ return true; }

    public static BaseBatchImporterFactory withHighestPriority()
    {
        BaseBatchImporterFactory highestPrioritized = null;
        Collection<BaseBatchImporterFactory> candidates = Services.loadAll( BaseBatchImporterFactory.class);
        for ( BaseBatchImporterFactory candidate : Services.loadAll( BaseBatchImporterFactory.class ) )
        {
            //if (!candidate.forStorageType( storageEngineType ))
            //    continue;
            if ( highestPrioritized == null || candidate.priority >= highestPrioritized.priority )
            {
                highestPrioritized = candidate;
            }
        }
        if ( highestPrioritized == null )
        {
            throw new NoSuchElementException( "No batch importers found" );
        }
        if (highestPrioritized != null)
            System.out.println("Using [" + highestPrioritized.getName() + "] importer.");
        else
            System.out.println("No suitable importer was found.");
        return highestPrioritized;
    }
    public BaseBatchImporterFactory withHighestPriority1()
    {
        BaseBatchImporterFactory highestPrioritized = null;
        Collection<BaseBatchImporterFactory> candidates = Services.loadAll( BaseBatchImporterFactory.class);
        for ( BaseBatchImporterFactory candidate : Services.loadAll( BaseBatchImporterFactory.class ) )
        {
            if ( highestPrioritized == null || candidate.priority > highestPrioritized.priority )
            {
                highestPrioritized = candidate;
            }
        }
        if ( highestPrioritized == null )
        {
            throw new NoSuchElementException( "No batch importers found" );
        }
        return highestPrioritized;
    }
}


