/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.server.rest.causalclustering;

import com.neo4j.causalclustering.core.CoreGraphDatabase;
import com.neo4j.causalclustering.readreplica.ReadReplicaGraphDatabase;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.server.rest.repr.OutputFormat;

public class CausalClusteringStatusFactory
{
    public static CausalClusteringStatus build( OutputFormat output, GraphDatabaseService db )
    {
        if ( db instanceof CoreGraphDatabase )
        {
            return new CoreStatus( output, (CoreGraphDatabase) db );
        }
        else if ( db instanceof ReadReplicaGraphDatabase )
        {
            return new ReadReplicaStatus( output, (ReadReplicaGraphDatabase) db );
        }
        else
        {
            return new NotCausalClustering( output );
        }
    }
}
