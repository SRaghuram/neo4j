/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.dbms.database;

import com.neo4j.causalclustering.catchup.CatchupComponentsRepository.CatchupComponents;
import com.neo4j.causalclustering.common.ClusteredDatabase;
import com.neo4j.causalclustering.core.consensus.LeaderLocator;
import com.neo4j.causalclustering.core.consensus.RaftMachine;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Optional;

import org.neo4j.dbms.database.DatabaseContext;
import org.neo4j.dbms.database.DatabaseManager;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.kernel.database.Database;
import org.neo4j.kernel.database.NamedDatabaseId;
import org.neo4j.monitoring.Monitors;
import org.neo4j.storageengine.api.StoreId;

/**
 * Instances implementing this interface represent individual clustered databases in Neo4j.
 *
 * Instances are responsible for the lifecycle management of per-database components, as well as exposing
 * per database dependency management, monitoring and io operations.
 *
 * Collections of these instances should be managed by a {@link DatabaseManager}
 */
public interface ClusteredDatabaseContext extends DatabaseContext
{
    /**
     * Reads metadata about this database from disk and calculates a uniquely {@link StoreId}.
     * The store id should be cached so that future calls do not require IO.
     * @return store id for this database
     */
    StoreId storeId();

    /**
     * Returns per-database {@link Monitors}
     * @return monitors for this database
     */
    Monitors monitors();

    /**
     * Delete the store files for this database
     */
    void delete() throws IOException;

    /**
     * @return Whether or not the store files for this database are empty/non-existent.
     */
    boolean isEmpty();

    /**
     * @return A listing of all store files which comprise this database
     */
    DatabaseLayout databaseLayout();

    /**
     * Replace the store files for this database
     * @param sourceDir the store files to replace this databases's current files with
     */
    void replaceWith( Path sourceDir ) throws IOException;

    /**
     * @return the identifier for this database
     */
    NamedDatabaseId databaseId();

    /**
     * @return the {@link CatchupComponents} for this clustered database
     */
    CatchupComponents catchupComponents();

    /**
     * Object which encapsulates the lifecycle of all components required by a
     * clustered database instance, including the {@link RaftMachine} and kernel {@link Database}
     * @return lifecycle controller for this database and its supporting lifecycled components
     */
    ClusteredDatabase clusteredDatabase();

    /**
     *
     * @return {@link LeaderLocator} for this database. This is only available if this database participates in Raft
     */
    Optional<LeaderLocator> leaderLocator();
}
