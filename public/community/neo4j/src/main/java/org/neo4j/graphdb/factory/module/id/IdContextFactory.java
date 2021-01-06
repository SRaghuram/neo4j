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
package org.neo4j.graphdb.factory.module.id;

import org.neo4j.configuration.Config;
import org.neo4j.internal.id.*;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.kernel.database.NamedDatabaseId;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.util.FeatureToggles;

import java.util.function.Function;

public class IdContextFactory
{
     private static final boolean ID_BUFFERING_FLAG = FeatureToggles.flag( IdContextFactory.class, "safeIdBuffering", true );

    private final JobScheduler jobScheduler;
    private final Function<NamedDatabaseId,IdGeneratorFactory> idFactoryProvider;
    private final Function<IdGeneratorFactory,IdGeneratorFactory> factoryWrapper;
    private final PageCacheTracer cacheTracer;
    private Config config = null;

    IdContextFactory( JobScheduler jobScheduler, Function<NamedDatabaseId,IdGeneratorFactory> idFactoryProvider,
            Function<IdGeneratorFactory,IdGeneratorFactory> factoryWrapper, PageCacheTracer cacheTracer )
    {
        this.jobScheduler = jobScheduler;
        this.idFactoryProvider = idFactoryProvider;
        this.factoryWrapper = factoryWrapper;
        this.cacheTracer = cacheTracer;
    }

    IdContextFactory(JobScheduler jobScheduler, Function<NamedDatabaseId,IdGeneratorFactory> idFactoryProvider,
                     Function<IdGeneratorFactory,IdGeneratorFactory> factoryWrapper, Config config, PageCacheTracer cacheTracer )
    {
        this.jobScheduler = jobScheduler;
        this.idFactoryProvider = idFactoryProvider;
        this.factoryWrapper = factoryWrapper;
        this.cacheTracer = cacheTracer;
        this.config = config;
    }
    public DatabaseIdContext createIdContext( NamedDatabaseId namedDatabaseId )
    {
        return ID_BUFFERING_FLAG ? createBufferingIdContext( idFactoryProvider, jobScheduler, cacheTracer, namedDatabaseId )
                                 : createDefaultIdContext( idFactoryProvider, namedDatabaseId );
    }

    private DatabaseIdContext createDefaultIdContext( Function<NamedDatabaseId,? extends IdGeneratorFactory> idGeneratorFactoryProvider,
            NamedDatabaseId namedDatabaseId )
    {
        return createIdContext( idGeneratorFactoryProvider.apply( namedDatabaseId ), createDefaultIdController() );
    }

    private DatabaseIdContext createBufferingIdContext( Function<NamedDatabaseId,? extends IdGeneratorFactory> idGeneratorFactoryProvider,
            JobScheduler jobScheduler, PageCacheTracer cacheTracer, NamedDatabaseId namedDatabaseId )
    {
        IdGeneratorFactory idGeneratorFactory = idGeneratorFactoryProvider.apply( namedDatabaseId );
        idGeneratorFactory.setOneIDFile( config.isOneIDFile(namedDatabaseId.name()));
        BufferingIdGeneratorFactory bufferingIdGeneratorFactory = new BufferingIdGeneratorFactory( idGeneratorFactory );
        BufferedIdController bufferingController = createBufferedIdController( bufferingIdGeneratorFactory, jobScheduler, cacheTracer, namedDatabaseId.name() );
        return createIdContext( bufferingIdGeneratorFactory, bufferingController );
    }

    private DatabaseIdContext createIdContext( IdGeneratorFactory idGeneratorFactory, IdController idController )
    {
        return new DatabaseIdContext( factoryWrapper.apply( idGeneratorFactory ), idController );
    }

    private static BufferedIdController createBufferedIdController( BufferingIdGeneratorFactory idGeneratorFactory, JobScheduler scheduler,
            PageCacheTracer cacheTracer, String databaseName )
    {
        return new BufferedIdController( idGeneratorFactory, scheduler, cacheTracer, databaseName );
    }

    private static DefaultIdController createDefaultIdController()
    {
        return new DefaultIdController();
    }
}
