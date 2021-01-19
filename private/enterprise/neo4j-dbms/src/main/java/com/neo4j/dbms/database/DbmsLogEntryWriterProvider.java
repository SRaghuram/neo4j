/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.dbms.database;

import java.util.Optional;
import java.util.function.Supplier;

import org.neo4j.exceptions.UnsatisfiedDependencyException;
import org.neo4j.io.fs.WritableChecksumChannel;
import org.neo4j.kernel.database.Database;
import org.neo4j.kernel.database.DbmsLogEntryWriterFactory;
import org.neo4j.kernel.database.LogEntryWriterFactory;
import org.neo4j.kernel.database.NamedDatabaseId;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryWriter;
import org.neo4j.storageengine.api.KernelVersionRepository;

public class DbmsLogEntryWriterProvider
{
    private DbmsLogEntryWriterProvider()
    {
        this( null );
    }

    private final MultiDatabaseManager<?> databaseManager;

    public DbmsLogEntryWriterProvider( MultiDatabaseManager<?> databaseManager )
    {
        this.databaseManager = databaseManager;
    }

    public LogEntryWriterFactory getEntryWriterFactory( NamedDatabaseId namedDatabaseId )
    {
        // Because of chicken and egg type of problem in cluster bootstrap this might get called
        // before database manager is populated with databases,
        // so the initialization of the log entry factory is delayed until its first use.
        return new LazyLogEntryWriterFactory( () -> Optional.ofNullable( namedDatabaseId )
                                                            .flatMap( databaseManager::getDatabaseContext )
                                                            .flatMap( ctx -> resolveEntryWriterFactoryOpt( ctx.database() ) )
                                                            .orElse( LogEntryWriterFactory.LATEST ) );
    }

    public static LogEntryWriterFactory resolveEntryWriterFactory( Database database )
    {
        return new LazyLogEntryWriterFactory( () -> resolveEntryWriterFactoryOpt( database ).orElse( LogEntryWriterFactory.LATEST ) );
    }

    private static Optional<LogEntryWriterFactory> resolveEntryWriterFactoryOpt( Database database )
    {
        try
        {
            return Optional.ofNullable( database ).map(
                    db -> new DbmsLogEntryWriterFactory( db.getDependencyResolver().resolveDependency( KernelVersionRepository.class ) ) );
        }
        catch ( UnsatisfiedDependencyException e )
        {
            return Optional.empty();
        }
    }

    public <T extends WritableChecksumChannel> LogEntryWriter<T> createEntryWriter( NamedDatabaseId namedDatabaseId, T channel )
    {
        return getEntryWriterFactory( namedDatabaseId ).createEntryWriter( channel );
    }

    public static DbmsLogEntryWriterProvider ALWAYS_LATEST = new DbmsLogEntryWriterProvider()
    {
        @Override
        public LogEntryWriterFactory getEntryWriterFactory( NamedDatabaseId namedDatabaseId )
        {
            return LogEntryWriterFactory.LATEST;
        }
    };

    private static class LazyLogEntryWriterFactory implements LogEntryWriterFactory
    {
        private final Supplier<LogEntryWriterFactory> factorySupplier;
        private LogEntryWriterFactory wrappedFactory;

        private LazyLogEntryWriterFactory( Supplier<LogEntryWriterFactory> factorySupplier )
        {
            this.factorySupplier = factorySupplier;
        }

        @Override
        public <T extends WritableChecksumChannel> LogEntryWriter<T> createEntryWriter( T channel )
        {
            if ( wrappedFactory == null )
            {
                wrappedFactory = factorySupplier.get();
            }

            return wrappedFactory.createEntryWriter( channel );
        }
    }
}
