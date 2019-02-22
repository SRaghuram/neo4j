/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.routing.multi_cluster.procedure;

import org.neo4j.internal.kernel.api.procs.QualifiedName;

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
public enum ProcedureNames
{
    GET_ROUTERS_FOR_DATABASE( "getRoutersForDatabase" ),
    GET_ROUTERS_FOR_ALL_DATABASES( "getRoutersForAllDatabases" );

    private static final String[] nameSpace = new String[]{"dbms", "cluster", "routing"};
    private final String name;

    ProcedureNames( String name )
    {
        this.name = name;
    }

    public QualifiedName fullyQualifiedName()
    {
        return new QualifiedName( nameSpace, name );
    }
}
