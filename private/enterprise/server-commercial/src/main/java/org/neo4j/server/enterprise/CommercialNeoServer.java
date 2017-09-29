/*
 * Copyright (c) 2002-2017 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.server.enterprise;

import java.io.File;

import org.neo4j.causalclustering.core.CommercialCoreGraphDatabase;
import org.neo4j.causalclustering.readreplica.CommercialReadReplicaGraphDatabase;
import org.neo4j.dbms.DatabaseManagementSystemSettings;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.enterprise.configuration.EnterpriseEditionSettings;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacadeFactory.Dependencies;
import org.neo4j.logging.LogProvider;
import org.neo4j.server.database.Database;
import org.neo4j.server.database.LifecycleManagingDatabase.GraphFactory;

import static org.neo4j.server.database.LifecycleManagingDatabase.lifecycleManagingDatabase;

public class CommercialNeoServer extends EnterpriseNeoServer
{
    private static final GraphFactory CORE_FACTORY = ( config, dependencies ) ->
    {
        File storeDir = config.get( DatabaseManagementSystemSettings.database_path );
        return new CommercialCoreGraphDatabase( storeDir, config, dependencies );
    };

    private static final GraphFactory READ_REPLICA_FACTORY = ( config, dependencies ) ->
    {
        File storeDir = config.get( DatabaseManagementSystemSettings.database_path );
        return new CommercialReadReplicaGraphDatabase( storeDir, config, dependencies );
    };

    public CommercialNeoServer( Config config, Dependencies dependencies, LogProvider logProvider )
    {
        super( config, createDbFactory( config ), dependencies, logProvider );
    }

    protected static Database.Factory createDbFactory( Config config )
    {
        final EnterpriseEditionSettings.Mode mode = config.get( EnterpriseEditionSettings.mode );

        switch ( mode )
        {
        case CORE:
            return lifecycleManagingDatabase( CORE_FACTORY );
        case READ_REPLICA:
            return lifecycleManagingDatabase( READ_REPLICA_FACTORY );
        default:
            return EnterpriseNeoServer.createDbFactory( config );
        }
    }
}
