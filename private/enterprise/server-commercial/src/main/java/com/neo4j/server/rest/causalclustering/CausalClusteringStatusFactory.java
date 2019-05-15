/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.server.rest.causalclustering;

import org.neo4j.common.DependencyResolver;
import org.neo4j.kernel.impl.factory.DatabaseInfo;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;
import org.neo4j.server.database.DatabaseService;
import org.neo4j.server.rest.repr.OutputFormat;

public class CausalClusteringStatusFactory
{
    public static CausalClusteringStatus build( OutputFormat output, DatabaseService database )
    {
        GraphDatabaseFacade databaseFacade = database.getDatabase();
        DependencyResolver dependencyResolver = databaseFacade.getDependencyResolver();
        DatabaseInfo databaseInfo = dependencyResolver.resolveDependency( DatabaseInfo.class );
        switch ( databaseInfo )
        {
            case CORE:
                return new CoreStatus( output, databaseFacade );
            case READ_REPLICA:
                return new ReadReplicaStatus( output, databaseFacade );
            default:
                return new NotCausalClustering( output );
        }
    }
}
