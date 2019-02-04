/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.state;

import com.neo4j.causalclustering.core.CommitProcessInstaller;

public interface CoreStateFactory
{
    DatabaseCoreStateComponents create( String databaseName, DatabaseCoreStateComponents.LifecycleDependencies dependencies, CommitProcessInstaller installer );
}
