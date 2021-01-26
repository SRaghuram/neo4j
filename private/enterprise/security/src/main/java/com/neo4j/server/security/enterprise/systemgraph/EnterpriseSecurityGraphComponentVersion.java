/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.server.security.enterprise.systemgraph;

import org.neo4j.dbms.database.ComponentVersion;
import org.neo4j.server.security.systemgraph.UserSecurityGraphComponent;

import static org.neo4j.dbms.database.KnownSystemComponentVersion.UNKNOWN_VERSION;

public enum EnterpriseSecurityGraphComponentVersion implements ComponentVersion
{
    /**
     * Version scheme of SECURITY_PRIVILEGE_COMPONENT with breaking changes to the schema:
     *
     * Version 0 (Neo4j 3.5):
     *  - Users were stored in a file and the roles for each users in another enterprise-only file.
     *
     * Version 1 (Neo4j 3.6):
     *  - Introduction of the system database as a way to store users and roles.
     *    Users and roles can be migrated from the old file-based format.
     *    Note that the system database was enterprise-only and had a very different schema compare with today.
     *
     * Version 2 (Neo4j 4.0):
     *  - A whole new schema was introduced, so all roles and users must be re-added and the default privileges created.
     *    Each role is represented by a node with label :Role that is connected to zero or more users from the {@link UserSecurityGraphComponent}.
     *    A privilege is represented of a relationship of type :GRANTED or :DENIED from a role node to a node with label (:Privilege),
     *    which in turn is connected as below (where the database node is part of the DefaultSystemGraphComponent).
     *
     *   (:Privilege)-[:SCOPE]->(s:Segment)-[:APPLIES_TO]->(:Resource), (s)-[:FOR]->(database), (s)-[:Qualified]->(qualifier)
     *
     * Version 3 (Neo4j 4.1.0-Drop01):
     *  - The global write privilege became connected to a GraphResource instead of an AllPropertiesResource
     *  - The schema privilege was split into separate index and constraint privileges
     *  - Introduced the PUBLIC role
     *
     * Version 4 (Neo4j 4.1):
     *  - Introduced the version node in the system database
     *
     * Version 5 (Neo4j 4.2.0-Drop04):
     *   - Added support for execute procedure privileges
     *
     * Version 6 (Neo4j 4.2.0-Drop06):
     *   - Added support for execute function privileges
     *
     * Version 7 (Neo4j 4.2.0-Drop07):
     *   - Added support for show index and show constraint privileges
     *
     * Version 8 (Neo4j 4.2.1):
     *
     * Version 9 (Neo4j 4.3.0-Drop01):
     *   - Split out Admin privilege into it's component privileges to allow it to be recreated
     *
     * Version 10 (Neo4j 4.3.0-Drop02):
     *   - Added privilege for setting user's default database
     */
    ENTERPRISE_SECURITY_35( 0, SECURITY_PRIVILEGE_COMPONENT, ComponentVersion.Neo4jVersions.VERSION_35 ),
    ENTERPRISE_SECURITY_36( 1, SECURITY_PRIVILEGE_COMPONENT, ComponentVersion.Neo4jVersions.VERSION_36 ),
    ENTERPRISE_SECURITY_40( 2, SECURITY_PRIVILEGE_COMPONENT, ComponentVersion.Neo4jVersions.VERSION_40 ),
    ENTERPRISE_SECURITY_41D1( 3, SECURITY_PRIVILEGE_COMPONENT, ComponentVersion.Neo4jVersions.VERSION_41D1 ),
    ENTERPRISE_SECURITY_41( 4, SECURITY_PRIVILEGE_COMPONENT, ComponentVersion.Neo4jVersions.VERSION_41 ),
    ENTERPRISE_SECURITY_42D4( 5, SECURITY_PRIVILEGE_COMPONENT, ComponentVersion.Neo4jVersions.VERSION_42D4 ),
    ENTERPRISE_SECURITY_42D6( 6, SECURITY_PRIVILEGE_COMPONENT, ComponentVersion.Neo4jVersions.VERSION_42D6 ),
    ENTERPRISE_SECURITY_42D7( 7, SECURITY_PRIVILEGE_COMPONENT, ComponentVersion.Neo4jVersions.VERSION_42D7 ),
    ENTERPRISE_SECURITY_42P1( 8, SECURITY_PRIVILEGE_COMPONENT, ComponentVersion.Neo4jVersions.VERSION_42P1 ),
    ENTERPRISE_SECURITY_43D1( 9, SECURITY_PRIVILEGE_COMPONENT, ComponentVersion.Neo4jVersions.VERSION_43D1 ),
    ENTERPRISE_SECURITY_43D2( 10, SECURITY_PRIVILEGE_COMPONENT, ComponentVersion.Neo4jVersions.VERSION_43D2 ),

    ENTERPRISE_SECURITY_UNKNOWN_VERSION( UNKNOWN_VERSION, SECURITY_PRIVILEGE_COMPONENT, String.format( "no '%s' graph found", SECURITY_PRIVILEGE_COMPONENT ) ),
    ENTERPRISE_SECURITY_FUTURE_VERSION( Integer.MIN_VALUE, SECURITY_PRIVILEGE_COMPONENT, "Unrecognized future version" ),

    // Used for testing only:
    ENTERPRISE_SECURITY_FAKE_VERSION( Integer.MAX_VALUE, SECURITY_PRIVILEGE_COMPONENT, "Neo4j 8.8.88" );

    // Static variables for SECURITY_PRIVILEGE_COMPONENT versions
    public static final int FIRST_VALID_ENTERPRISE_SECURITY_COMPONENT_VERSION = ENTERPRISE_SECURITY_35.getVersion();
    public static final int FIRST_RUNTIME_SUPPORTED_ENTERPRISE_SECURITY_COMPONENT_VERSION = ENTERPRISE_SECURITY_40.getVersion();
    public static final int LATEST_ENTERPRISE_SECURITY_COMPONENT_VERSION = ENTERPRISE_SECURITY_43D2.getVersion();

    private final String componentName;
    private final int version;
    private final String description;

    EnterpriseSecurityGraphComponentVersion( int version, String componentName, String description )
    {
        this.version = version;
        this.componentName = componentName;
        this.description = description;
    }

    @Override
    public int getVersion()
    {
        return version;
    }

    @Override
    public String getComponentName()
    {
        return componentName;
    }

    @Override
    public String getDescription()
    {
        return description;
    }

    @Override
    public boolean isCurrent()
    {
        return version == LATEST_ENTERPRISE_SECURITY_COMPONENT_VERSION;
    }

    @Override
    public boolean migrationSupported()
    {
        return version >= FIRST_VALID_ENTERPRISE_SECURITY_COMPONENT_VERSION && version <= LATEST_ENTERPRISE_SECURITY_COMPONENT_VERSION;
    }

    @Override
    public boolean runtimeSupported()
    {
        return version >= FIRST_RUNTIME_SUPPORTED_ENTERPRISE_SECURITY_COMPONENT_VERSION && version <= LATEST_ENTERPRISE_SECURITY_COMPONENT_VERSION;
    }
}
