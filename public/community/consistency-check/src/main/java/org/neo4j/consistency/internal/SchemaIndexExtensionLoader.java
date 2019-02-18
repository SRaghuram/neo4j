/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.consistency.internal;

import org.neo4j.common.Service;
import org.neo4j.configuration.Config;
import org.neo4j.index.internal.gbptree.RecoveryCleanupWorkCollector;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.api.index.IndexProvider;
import org.neo4j.kernel.extension.DatabaseExtensions;
import org.neo4j.kernel.extension.ExtensionFactory;
import org.neo4j.kernel.extension.ExtensionFailureStrategies;
import org.neo4j.kernel.extension.context.DatabaseExtensionContext;
import org.neo4j.kernel.impl.core.TokenHolders;
import org.neo4j.kernel.impl.factory.DatabaseInfo;
import org.neo4j.kernel.impl.util.Dependencies;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.logging.internal.LogService;
import org.neo4j.scheduler.JobScheduler;

/**
 * Utility for loading {@link IndexProvider} instances from {@link DatabaseExtensions}.
 */
public class SchemaIndexExtensionLoader
{

    @SuppressWarnings( "unchecked" )
    public static DatabaseExtensions instantiateExtensions( DatabaseLayout databaseLayout, FileSystemAbstraction fileSystem, Config config,
            LogService logService, PageCache pageCache, JobScheduler jobScheduler, RecoveryCleanupWorkCollector recoveryCollector, DatabaseInfo databaseInfo,
            Monitors monitors, TokenHolders tokenHolders )
    {
        Dependencies deps = new Dependencies();
        deps.satisfyDependencies( fileSystem, config, logService, pageCache, recoveryCollector, monitors, jobScheduler, tokenHolders );
        @SuppressWarnings( "rawtypes" )
        Iterable extensions = Service.loadAll( ExtensionFactory.class );
        DatabaseExtensionContext extensionContext = new DatabaseExtensionContext( databaseLayout, databaseInfo, deps );
        return new DatabaseExtensions( extensionContext, extensions, deps, ExtensionFailureStrategies.ignore() );
    }
}
