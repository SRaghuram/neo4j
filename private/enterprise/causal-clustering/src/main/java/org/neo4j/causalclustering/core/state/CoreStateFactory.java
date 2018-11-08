/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.causalclustering.core.state;

import org.neo4j.causalclustering.common.LocalDatabase;

public interface CoreStateFactory<DB extends LocalDatabase>
{
    PerDatabaseCoreStateComponents create( DB localDatabase );
}
