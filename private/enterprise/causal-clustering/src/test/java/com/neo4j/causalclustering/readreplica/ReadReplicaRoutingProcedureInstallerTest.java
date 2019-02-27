/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.readreplica;

import com.neo4j.causalclustering.routing.load_balancing.procedure.ReadReplicaGetRoutingTableProcedure;
import org.junit.jupiter.api.Test;

import java.util.Set;

import org.neo4j.configuration.Config;
import org.neo4j.configuration.connectors.ConnectorPortRegister;
import org.neo4j.internal.kernel.api.procs.ProcedureSignature;
import org.neo4j.internal.kernel.api.procs.QualifiedName;
import org.neo4j.kernel.api.procedure.GlobalProcedures;
import org.neo4j.procedure.impl.GlobalProceduresRegistry;

import static java.util.stream.Collectors.toSet;
import static org.eclipse.collections.impl.set.mutable.UnifiedSet.newSetWith;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

class ReadReplicaRoutingProcedureInstallerTest
{
    @Test
    void shouldRegisterRoutingProcedures() throws Exception
    {
        ConnectorPortRegister portRegister = mock( ConnectorPortRegister.class );
        ReadReplicaRoutingProcedureInstaller installer = new ReadReplicaRoutingProcedureInstaller( portRegister, Config.defaults() );
        GlobalProcedures procedures = spy( new GlobalProceduresRegistry() );

        installer.install( procedures );

        verify( procedures, times( 2 ) ).register( any( ReadReplicaGetRoutingTableProcedure.class ) );

        Set<QualifiedName> expectedNames = newSetWith(
                new QualifiedName( new String[]{"dbms", "routing"}, "getRoutingTable" ),
                new QualifiedName( new String[]{"dbms", "cluster", "routing"}, "getRoutingTable" ) );

        Set<QualifiedName> actualNames = procedures.getAllProcedures()
                .stream()
                .map( ProcedureSignature::name )
                .collect( toSet() );

        assertEquals( expectedNames, actualNames );
    }
}
