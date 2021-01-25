/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.kernel.impl.locking;

import org.neo4j.annotations.service.ServiceProvider;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.Description;
import org.neo4j.configuration.Internal;
import org.neo4j.configuration.SettingImpl;
import org.neo4j.configuration.SettingsDeclaration;
import org.neo4j.graphdb.config.Setting;
import org.neo4j.kernel.impl.locking.Locks;
import org.neo4j.kernel.impl.locking.SimpleStatementLocks;
import org.neo4j.kernel.impl.locking.StatementLocks;
import org.neo4j.kernel.impl.locking.StatementLocksFactory;

import static java.util.Objects.requireNonNull;
import static org.neo4j.configuration.SettingValueParsers.BOOL;

/**
 * A {@link StatementLocksFactory} that created {@link DeferringStatementLocks} based on the given
 * {@link Locks} and {@link Config}.
 */
@ServiceProvider
public class DeferringStatementLocksFactory implements StatementLocksFactory
{
    @ServiceProvider
    public static class Configuration implements SettingsDeclaration
    {
        @Internal
        @Description( "Enable deferring of locks to commit time. This feature weakens the isolation level. " +
                "It can result in both domain and storage level inconsistencies." )
        public static final Setting<Boolean> deferred_locks_enabled =
                SettingImpl.newBuilder( "unsupported.dbms.deferred_locks.enabled", BOOL, false ).build();
    }

    private Locks locks;
    private boolean deferredLocksEnabled;

    @Override
    public void initialize( Locks locks, Config config )
    {
        this.locks = requireNonNull( locks );
        this.deferredLocksEnabled = config.get( Configuration.deferred_locks_enabled );
    }

    @Override
    public StatementLocks newInstance()
    {
        if ( locks == null )
        {
            throw new IllegalStateException( "Factory has not been initialized" );
        }

        Locks.Client client = locks.newClient();
        return deferredLocksEnabled ? new DeferringStatementLocks( client ) : new SimpleStatementLocks( client );
    }
}
