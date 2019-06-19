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
package org.neo4j.dbms.database;

import java.util.Arrays;
import java.util.List;
import java.util.NavigableMap;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiConsumer;

import org.neo4j.collection.Dependencies;
import org.neo4j.common.DependencyResolver;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.graphdb.factory.module.GlobalModule;
import org.neo4j.graphdb.factory.module.ModularDatabaseCreationContext;
import org.neo4j.graphdb.factory.module.edition.AbstractEditionModule;
import org.neo4j.graphdb.factory.module.edition.context.EditionDatabaseComponents;
import org.neo4j.io.pagecache.tracing.cursor.context.EmptyVersionContextSupplier;
import org.neo4j.io.pagecache.tracing.cursor.context.VersionContextSupplier;
import org.neo4j.kernel.api.procedure.GlobalProcedures;
import org.neo4j.kernel.database.Database;
import org.neo4j.kernel.database.DatabaseId;
import org.neo4j.kernel.impl.context.TransactionVersionContextSupplier;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;
import org.neo4j.monitoring.Monitors;

import static java.util.Collections.unmodifiableNavigableMap;

public abstract class AbstractDatabaseManager<DB extends DatabaseContext> extends LifecycleAdapter implements DatabaseManager<DB>
{
    protected final ConcurrentHashMap<DatabaseId,DB> databaseMap;
    protected final GlobalModule globalModule;
    protected final AbstractEditionModule edition;
    protected final Log log;
    private final boolean manageDatabasesOnStartAndStop;
    protected final Config config;
    protected final LogProvider logProvider;

    protected AbstractDatabaseManager( GlobalModule globalModule, AbstractEditionModule edition, Log log, boolean manageDatabasesOnStartAndStop )
    {
        this.logProvider = globalModule.getLogService().getInternalLogProvider();
        this.log = log;
        this.globalModule = globalModule;
        this.config = globalModule.getGlobalConfig();
        this.edition = edition;
        this.manageDatabasesOnStartAndStop = manageDatabasesOnStartAndStop;
        this.databaseMap = new ConcurrentHashMap<>();
    }

    List<String> defaultDatabaseNames()
    {
        return Arrays.asList( GraphDatabaseSettings.SYSTEM_DATABASE_NAME, config.get( GraphDatabaseSettings.default_database ) );
    }

    @Override
    public final void init()
    { //no-op
    }

    @Override
    public void start() throws Exception
    {
        if ( manageDatabasesOnStartAndStop )
        {
            startAllDatabases();
        }
    }

    private void startAllDatabases()
    {
        forEachDatabase( this::startDatabase, false );
    }

    @Override
    public void stop() throws Exception
    {
        if ( manageDatabasesOnStartAndStop )
        {
            stopAllDatabases();
        }
    }

    private void stopAllDatabases()
    {
        forEachDatabase( this::stopDatabase, true );
    }

    @Override
    public final SortedMap<DatabaseId,DB> registeredDatabases()
    {
        return databasesSnapshot();
    }

    private NavigableMap<DatabaseId,DB> databasesSnapshot()
    {
        return unmodifiableNavigableMap( new TreeMap<>( databaseMap ) );
    }

    protected abstract DB createDatabaseContext( DatabaseId databaseId );

    protected ModularDatabaseCreationContext newDatabaseCreationContext( DatabaseId databaseId, Dependencies parentDependencies, Monitors parentMonitors )
    {
        EditionDatabaseComponents editionDatabaseComponents = edition.createDatabaseComponents( databaseId );
        GlobalProcedures globalProcedures = edition.getGlobalProcedures();
        var databaseConfig = DatabaseConfig.from( config, databaseId );
        return new ModularDatabaseCreationContext( databaseId, globalModule, parentDependencies, parentMonitors,
                editionDatabaseComponents, globalProcedures, createVersionContextSupplier( databaseConfig ), databaseConfig );
    }

    private void forEachDatabase( BiConsumer<DatabaseId,DB> consumer, boolean systemDatabaseLast )
    {
        var snapshot = systemDatabaseLast ? databasesSnapshot().descendingMap().entrySet() : databasesSnapshot().entrySet();

        for ( var entry : snapshot )
        {
            DatabaseId databaseId = entry.getKey();
            DB context = entry.getValue();
            try
            {
                consumer.accept( databaseId, context );
            }
            catch ( Throwable t )
            {
                context.fail( t );
                log.error( "Failed to perform operation with database " + databaseId, t );
            }
        }
    }

    protected DB startDatabase( DatabaseId databaseId, DB context )
    {
        log.info( "Starting '%s' database.", databaseId.name() );
        Database database = context.database();
        database.start();
        return context;
    }

    protected DB stopDatabase( DatabaseId databaseId, DB context )
    {
        log.info( "Stop '%s' database.", databaseId.name() );
        Database database = context.database();
        database.stop();
        return context;
    }

    protected VersionContextSupplier createVersionContextSupplier( DatabaseConfig databaseConfig )
    {
        DependencyResolver externalDependencyResolver = globalModule.getExternalDependencyResolver();
        Class<VersionContextSupplier> klass = VersionContextSupplier.class;
        if ( externalDependencyResolver.resolveTypeDependencies( klass ).iterator().hasNext() )
        {
            return externalDependencyResolver.resolveDependency( klass );
        }
        else
        {
            return databaseConfig.get( GraphDatabaseSettings.snapshot_query ) ? new TransactionVersionContextSupplier() : EmptyVersionContextSupplier.EMPTY;
        }
    }
}
