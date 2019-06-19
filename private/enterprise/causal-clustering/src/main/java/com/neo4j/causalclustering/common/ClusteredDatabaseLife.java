/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.common;

import org.neo4j.dbms.database.DatabaseManager;
import org.neo4j.kernel.database.Database;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.kernel.lifecycle.Lifecycle;

/**
 * Instances of this interface encapsulate the lifecycle control for all components required by a
 * clustered database instance, including the Raft/Catchup servers and the kernel {@link Database}.
 *
 * Instances of this interface should be thought of as Clustered versions of {@link Database}.
 *
 * Note: These instances only *appear* to be like {@link Lifecycle}s, due to providing init/start/stop/shutdown.
 * In fact, instances of this interface are only ever managed directly by a {@link DatabaseManager},
 * never by a {@link LifeSupport}.
 *
 * If these instances were to implement the {@link Lifecycle} interface and added to a {@link LifeSupport},
 * the behaviour would be undefined.
 *
 * TODO: @Martin sidenote: This makes me think that Database probably shouldn't be a full lifecycle either
 *  (it is also, only ever called via the database manager). Sidenote the second: We're losing the safety of
 *  SafeLifecycle with this move, but that should be replaced with the control provided by the Reconciler, do you agree?
 */
public interface ClusteredDatabaseLife
{
    void init() throws Exception;

    void start() throws Exception;

    void stop() throws Exception;

    void shutdown() throws Exception;

    void add( Lifecycle lifecycledComponent );
}
