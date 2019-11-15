/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.state;

import org.neo4j.kernel.database.NamedDatabaseId;

class BootstrapException extends RuntimeException
{
    BootstrapException( NamedDatabaseId namedDatabaseId, Exception cause )
    {
        super( "Failed to bootstrap database " + namedDatabaseId.name(), cause );
    }
}
