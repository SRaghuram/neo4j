/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.common;

import com.neo4j.causalclustering.core.CoreClusterMember;
import com.neo4j.causalclustering.core.consensus.roles.Role;
import com.neo4j.causalclustering.discovery.DiscoveryServiceFactory;
import com.neo4j.configuration.EnterpriseEditionSettings;
import com.neo4j.enterprise.edition.EnterpriseEditionModule;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.configuration.helpers.SocketAddress;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.dbms.identity.ServerId;
import org.neo4j.dbms.identity.ServerIdentity;
import org.neo4j.graphdb.facade.DatabaseManagementServiceFactory;
import org.neo4j.graphdb.facade.GraphDatabaseDependencies;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.impl.factory.DbmsInfo;

import static java.util.Collections.emptyMap;

public class StandaloneMember extends CoreClusterMember
{
    public StandaloneMember(  int discoveryPort,
                              int txPort,
                              int boltPort,
                              int intraClusterBoltPort,
                              Path loopbackBoltFile,
                              int httpPort,
                              int backupPort,
                              List<SocketAddress> discoveryAddresses,
                              DiscoveryServiceFactory discoveryServiceFactory,
                              String recordFormat,
                              Path parentDir,
                              Map<String, String> extraParams,
                              String listenHost,
                              String advertisedHost )
    {
        super( discoveryPort, discoveryPort, txPort, 0, boltPort, intraClusterBoltPort, loopbackBoltFile, httpPort, backupPort, 2, discoveryAddresses,
               discoveryServiceFactory, recordFormat, parentDir, extraParams, emptyMap(), listenHost, advertisedHost );

        config.set( EnterpriseEditionSettings.enable_clustering_in_standalone, true );
        config.set( GraphDatabaseSettings.mode, GraphDatabaseSettings.Mode.SINGLE );
        memberConfig = config.build();
    }

    @Override
    protected DatabaseManagementService createManagementService( GraphDatabaseDependencies dependencies )
    {
        return new DatabaseManagementServiceFactory( DbmsInfo.ENTERPRISE, EnterpriseEditionModule::new )
                .build( memberConfig, dependencies );
    }

    @Override
    public final String toString()
    {
        return "StandaloneMember{serverId=" + (systemDatabase == null ? null : serverId()) + "}";
    }

    @Override
    public int index()
    {
        return 0;
    }

    @Override
    public ServerId serverId()
    {
        return systemDatabase.getDependencyResolver().resolveDependency( ServerIdentity.class ).serverId();
    }

    @Override
    public Optional<Role> roleFor( String databaseName )
    {
        return Optional.of( Role.LEADER );
    }

    public Path clusterStateDirectory()
    {
        throw new UnsupportedOperationException( "Not supported on standalone member" );
    }

    public Path raftLogDirectory( String databaseName )
    {
        throw new UnsupportedOperationException( "Not supported on standalone member" );
    }

    public void unbind( FileSystemAbstraction fs ) throws IOException
    {
        fs.deleteFile( neo4jLayout.serverIdFile() );
    }
}
