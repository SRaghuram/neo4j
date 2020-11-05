/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.server.security.enterprise.systemgraph.versions;

import java.util.HashMap;
import java.util.Map;

import org.neo4j.graphdb.Node;

public class PrivilegeStore
{
    private final Map<PRIVILEGE,Node> privilegeNodeMap = new HashMap<>();

    void setPrivilege( PRIVILEGE privilege, Node node )
    {
        privilegeNodeMap.put( privilege, node );
    }

    Node getPrivilege( PRIVILEGE privilege )
    {
        return privilegeNodeMap.get( privilege );
    }

    enum PRIVILEGE
    {
        TRAVERSE_NODE,
        TRAVERSE_RELATIONSHIP,
        READ_NODE_PROPERTY,
        READ_RELATIONSHIP_PROPERTY,
        MATCH_NODE,
        MATCH_RELATIONSHIP,
        WRITE_NODE,
        WRITE_RELATIONSHIP,
        ACCESS_ALL,
        ACCESS_DEFAULT,
        INDEX,
        CONSTRAINT,
        TOKEN,
        ADMIN,
        SCHEMA,
        EXECUTE_ALL_FUNCTIONS,
        EXECUTE_ALL_PROCEDURES
    }
}
