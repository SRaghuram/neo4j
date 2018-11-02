/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.causalclustering.routing.load_balancing.procedure;

import org.neo4j.causalclustering.routing.procedure.ProcedureNamesEnum;

/**
 * This is part of the cluster / driver interface specification and
 * defines the procedure names involved in the load balancing solution.
 *
 * These procedures are used by cluster driver software to discover endpoints,
 * their capabilities and to eventually schedule work appropriately.
 *
 * The intention is for this class to eventually move over to a support package
 * which can be included by driver software.
 */
public enum ProcedureNames implements ProcedureNamesEnum
{
    GET_SERVERS_V1( "getServers" ),
    GET_SERVERS_V2( "getRoutingTable" );

    private static final String[] nameSpace = new String[]{"dbms", "cluster", "routing"};
    private final String name;

    ProcedureNames( String name )
    {
        this.name = name;
    }

    @Override
    public String procedureName()
    {
        return name;
    }

    @Override
    public String[] procedureNameSpace()
    {
        return nameSpace;
    }
}
