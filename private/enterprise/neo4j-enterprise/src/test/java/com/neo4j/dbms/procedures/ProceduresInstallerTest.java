/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.dbms.procedures;

import com.neo4j.causalclustering.catchup.CatchupClientFactory;
import com.neo4j.causalclustering.core.CoreProceduresInstaller;
import com.neo4j.causalclustering.core.replication.ReplicationBenchmarkProcedure;
import com.neo4j.causalclustering.discovery.TopologyService;
import com.neo4j.causalclustering.discovery.procedures.AlwaysLeaderRoleProcedure;
import com.neo4j.causalclustering.discovery.procedures.CoreRoleProcedure;
import com.neo4j.causalclustering.discovery.procedures.InstalledProtocolsProcedure;
import com.neo4j.causalclustering.discovery.procedures.ReadReplicaRoleProcedure;
import com.neo4j.causalclustering.discovery.procedures.ReadReplicaToggleProcedure;
import com.neo4j.causalclustering.discovery.procedures.StandaloneClusterOverviewProcedure;
import com.neo4j.causalclustering.discovery.procedures.TopologyBasedClusterOverviewProcedure;
import com.neo4j.causalclustering.identity.InMemoryCoreServerIdentity;
import com.neo4j.causalclustering.net.InstalledProtocolHandler;
import com.neo4j.causalclustering.readreplica.ReadReplicaProceduresInstaller;
import com.neo4j.configuration.EnterpriseEditionSettings;
import com.neo4j.dbms.QuarantineOperator;
import com.neo4j.dbms.StandaloneDbmsReconcilerModule;
import com.neo4j.dbms.procedures.wait.WaitProcedure;
import com.neo4j.enterprise.edition.EnterpriseProceduresInstaller;
import com.neo4j.procedure.enterprise.builtin.EnterpriseBuiltInDbmsProcedures;
import com.neo4j.procedure.enterprise.builtin.EnterpriseBuiltInProcedures;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.stream.Stream;

import org.neo4j.configuration.Config;
import org.neo4j.dbms.database.DatabaseManager;
import org.neo4j.dbms.identity.ServerIdentity;
import org.neo4j.dbms.procedures.StandaloneDatabaseStateProcedure;
import org.neo4j.exceptions.KernelException;
import org.neo4j.graphdb.factory.module.GlobalModule;
import org.neo4j.kernel.api.procedure.GlobalProcedures;
import org.neo4j.logging.internal.NullLogService;
import org.neo4j.time.Clocks;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

class ProceduresInstallerTest
{
    private final GlobalProcedures globalProcedures = mock( GlobalProcedures.class );
    private final DatabaseManager<?> databaseManager = mock( DatabaseManager.class );
    private final StandaloneDbmsReconcilerModule reconcilerModule = mock( StandaloneDbmsReconcilerModule.class );
    private final Config config = Config.defaults();
    private final GlobalModule globalModule = mock( GlobalModule.class );
    private final ServerIdentity identityModule = new InMemoryCoreServerIdentity();
    private final InstalledProtocolHandler installedProtocolHandler = new InstalledProtocolHandler();
    private final CatchupClientFactory catchupClientFactory =  mock( CatchupClientFactory.class );
    private final TopologyService topologyService = mock( TopologyService.class );
    private final QuarantineOperator quarantineOperator = mock( QuarantineOperator.class );

    @BeforeEach
    void setup()
    {
        when( globalModule.getGlobalConfig() ).thenReturn( config );
        when( globalModule.getLogService() ).thenReturn( NullLogService.getInstance() );
        when( globalModule.getGlobalClock() ).thenReturn( Clocks.nanoClock() );
    }

    @Test
    void shouldRegisterAllCoreProcedures() throws KernelException
    {
        // when
        new CoreProceduresInstaller( globalProcedures, databaseManager, reconcilerModule, globalModule, identityModule, installedProtocolHandler,
                catchupClientFactory, topologyService, Stream::of, quarantineOperator ).register();

        // then
        verify( globalProcedures ).register( any( QuarantineProcedure.class ) );
        verify( globalProcedures ).register( any( TopologyBasedClusterOverviewProcedure.class ) );
        verify( globalProcedures ).register( any( CoreRoleProcedure.class ) );
        verify( globalProcedures ).register( any( InstalledProtocolsProcedure.class ) );
        verify( globalProcedures ).register( any( ClusteredDatabaseStateProcedure.class ) );
        verify( globalProcedures ).register( any( WaitProcedure.class ) );
        verify( globalProcedures ).register( any( ClusterSetDefaultDatabaseProcedure.class ) );

        verify( globalProcedures ).registerProcedure( EnterpriseBuiltInDbmsProcedures.class, true );
        verify( globalProcedures ).registerProcedure( EnterpriseBuiltInProcedures.class, true );
        verify( globalProcedures ).registerProcedure( ReplicationBenchmarkProcedure.class );

        verify( globalProcedures ).registerComponent( any(), any(), anyBoolean() );

        verifyNoMoreInteractions( globalProcedures );
    }

    @Test
    void shouldRegisterAllReadReplicaProcedures() throws KernelException
    {
        // when
        new ReadReplicaProceduresInstaller( globalProcedures, databaseManager, reconcilerModule, globalModule, identityModule, installedProtocolHandler,
                catchupClientFactory, topologyService, quarantineOperator ).register();

        // then
        verify( globalProcedures ).register( any( QuarantineProcedure.class ) );
        verify( globalProcedures ).register( any( TopologyBasedClusterOverviewProcedure.class ) );
        verify( globalProcedures ).register( any( ReadReplicaRoleProcedure.class ) );
        verify( globalProcedures ).register( any( InstalledProtocolsProcedure.class ) );
        verify( globalProcedures ).register( any( ClusteredDatabaseStateProcedure.class ) );
        verify( globalProcedures ).register( any( WaitProcedure.class ) );
        verify( globalProcedures ).register( any( ReadReplicaToggleProcedure.class ) );

        verify( globalProcedures ).registerProcedure( EnterpriseBuiltInDbmsProcedures.class, true );
        verify( globalProcedures ).registerProcedure( EnterpriseBuiltInProcedures.class, true );

        verify( globalProcedures ).registerComponent( any(), any(), anyBoolean() );

        verifyNoMoreInteractions( globalProcedures );
    }

    @Test
    void shouldRegisterAllStandaloneWithClusterProcedures() throws KernelException
    {
        // given
        config.set( EnterpriseEditionSettings.enable_clustering_in_standalone, true );

        // when
        new EnterpriseProceduresInstaller( globalProcedures, databaseManager, reconcilerModule, globalModule, identityModule, installedProtocolHandler,
                catchupClientFactory, topologyService ).register();

        // then
        verify( globalProcedures ).register( any( TopologyBasedClusterOverviewProcedure.class ) );
        verify( globalProcedures ).register( any( AlwaysLeaderRoleProcedure.class ) );
        verify( globalProcedures ).register( any( InstalledProtocolsProcedure.class ) );
        verify( globalProcedures ).register( any( ClusteredDatabaseStateProcedure.class ) );
        verify( globalProcedures ).register( any( WaitProcedure.class ) );
        verify( globalProcedures ).register( any( ClusterSetDefaultDatabaseProcedure.class ) );

        verify( globalProcedures ).registerProcedure( EnterpriseBuiltInDbmsProcedures.class, true );
        verify( globalProcedures ).registerProcedure( EnterpriseBuiltInProcedures.class, true );

        verify( globalProcedures ).registerComponent( any(), any(), anyBoolean() );

        verifyNoMoreInteractions( globalProcedures );
    }

    @Test
    void shouldRegisterAllStandaloneWithoutClusterProcedures() throws KernelException
    {
        // given
        config.set( EnterpriseEditionSettings.enable_clustering_in_standalone, false );

        // when
        new EnterpriseProceduresInstaller( globalProcedures, databaseManager, reconcilerModule, globalModule, identityModule, installedProtocolHandler,
                catchupClientFactory, topologyService ).register();

        // then
        verify( globalProcedures ).register( any( StandaloneClusterOverviewProcedure.class ) );
        verify( globalProcedures ).register( any( AlwaysLeaderRoleProcedure.class ) );
        verify( globalProcedures ).register( any( InstalledProtocolsProcedure.class ) );
        verify( globalProcedures ).register( any( StandaloneDatabaseStateProcedure.class ) );
        verify( globalProcedures ).register( any( WaitProcedure.class ) );

        verify( globalProcedures ).registerProcedure( EnterpriseBuiltInDbmsProcedures.class, true );
        verify( globalProcedures ).registerProcedure( EnterpriseBuiltInProcedures.class, true );

        verifyNoMoreInteractions( globalProcedures );
    }
}
