/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.server.security.enterprise.auth;

import org.neo4j.internal.kernel.api.security.PrivilegeAction;
import org.neo4j.internal.kernel.api.security.Segment;
import org.neo4j.kernel.api.exceptions.InvalidArgumentsException;

import static org.neo4j.configuration.GraphDatabaseSettings.SYSTEM_DATABASE_NAME;
import static org.neo4j.internal.kernel.api.security.PrivilegeAction.ADMIN;
import static org.neo4j.internal.kernel.api.security.PrivilegeAction.TRANSACTION_MANAGEMENT;

public class ResourcePrivilege
{
    private final GrantOrDeny privilegeType;
    private final PrivilegeAction action;
    private final Resource resource;
    private final Segment segment;
    private final String dbName;
    private final boolean allDatabases, defaultDatabase;

    public ResourcePrivilege( GrantOrDeny privilegeType, PrivilegeAction action, Resource resource, Segment segment, SpecialDatabase specialDatabase )
            throws InvalidArgumentsException
    {
        this.privilegeType = privilegeType;
        this.action = action;
        this.resource = resource;
        this.segment = segment;
        this.dbName = "";
        if ( specialDatabase.equals( SpecialDatabase.ALL ) )
        {
            this.allDatabases = true;
            this.defaultDatabase = false;
        }
        else
        {
            this.allDatabases = false;
            this.defaultDatabase = true;
        }
        resource.assertValidCombination( action );
    }

    public ResourcePrivilege( GrantOrDeny privilegeType, PrivilegeAction action, Resource resource, Segment segment, String dbName )
            throws InvalidArgumentsException
    {
        this.privilegeType = privilegeType;
        this.action = action;
        this.resource = resource;
        this.segment = segment;
        this.dbName = dbName;
        this.allDatabases = false;
        this.defaultDatabase = false;
        resource.assertValidCombination( action );
    }

    boolean appliesTo( String database )
    {
        if ( database.equals( SYSTEM_DATABASE_NAME ) )
        {
            if ( ADMIN.satisfies( action ) )
            {
                return true;
            }
        }
        return allDatabases || database.equals( dbName ) || TRANSACTION_MANAGEMENT.satisfies(action);
    }

    boolean appliesToDefault()
    {
        return defaultDatabase;
    }

    GrantOrDeny getPrivilegeType()
    {
        return privilegeType;
    }

    Resource getResource()
    {
        return resource;
    }

    PrivilegeAction getAction()
    {
        return action;
    }

    Segment getSegment()
    {
        return segment;
    }

    public String getDbName()
    {
        return this.dbName;
    }

    public boolean appliesToAll()
    {
        return allDatabases;
    }

    public String asGrantFor( String role )
    {
        return asGrantFor( role, "", false );
    }

    public String asGrantFor( String role, String database )
    {
        return asGrantFor( role, database, true );
    }

    private String asGrantFor( String role, String database, boolean replaceDbName )
    {
        String databaseName;
        if ( replaceDbName )
        {
            databaseName = database;
        }
        else
        {
            databaseName = allDatabases ? "*" : "`" + dbName + "`";
        }

        switch ( action )
        {
        case ACCESS:
            return String.format( "%s ACCESS ON DATABASE %s TO `%s`", privilegeType.prefix, databaseName, role );

        case TRAVERSE:
            return String.format( "%s TRAVERSE ON GRAPH %s %s TO `%s`", privilegeType.prefix, databaseName, segment.toString(), role );
        case READ:
            return String.format( "%s READ {%s} ON GRAPH %s %s TO `%s`", privilegeType.prefix, resource.toString(), databaseName, segment.toString(), role );
        case MATCH:
            return String.format( "%s MATCH {%s} ON GRAPH %s %s TO `%s`", privilegeType.prefix, resource.toString(), databaseName, segment.toString(), role );

        case MERGE:
            return String.format( "%s MERGE {%s} ON GRAPH %s %s TO `%s`", privilegeType.prefix, resource.toString(), databaseName, segment.toString(), role );

        case WRITE:
            return String.format( "%s WRITE ON GRAPH %s TO `%s`", privilegeType.prefix, databaseName, role );

        case SET_LABEL:
            return String.format( "%s SET LABEL %s ON GRAPH %s TO `%s`", privilegeType.prefix, resource.toString(), databaseName, role );
        case REMOVE_LABEL:
            return String.format( "%s REMOVE LABEL %s ON GRAPH %s TO `%s`", privilegeType.prefix, resource.toString(), databaseName, role );
        case CREATE_ELEMENT:
            return String.format( "%s CREATE ON GRAPH %s %s TO `%s`", privilegeType.prefix, databaseName, segment.toString(), role );
        case DELETE_ELEMENT:
            return String.format( "%s DELETE ON GRAPH %s %s TO `%s`", privilegeType.prefix, databaseName, segment.toString(), role );
        case SET_PROPERTY:
            return String.format( "%s SET PROPERTY {%s} ON GRAPH %s %s TO `%s`", privilegeType.prefix, resource.toString(), databaseName, segment.toString(), role );

        case GRAPH_ACTIONS:
            return String.format( "%s ALL GRAPH PRIVILEGES ON GRAPH %s TO `%s`", privilegeType.prefix, databaseName, role );

        case CREATE_LABEL:
            return String.format( "%s CREATE NEW NODE LABEL ON DATABASE %s TO `%s`", privilegeType.prefix, databaseName, role );
        case CREATE_RELTYPE:
            return String.format( "%s CREATE NEW RELATIONSHIP TYPE ON DATABASE %s TO `%s`", privilegeType.prefix, databaseName, role );
        case CREATE_PROPERTYKEY:
            return String.format( "%s CREATE NEW PROPERTY NAME ON DATABASE %s TO `%s`", privilegeType.prefix, databaseName, role );
        case TOKEN:
            return String.format( "%s NAME MANAGEMENT ON DATABASE %s TO `%s`", privilegeType.prefix, databaseName, role );

        case CREATE_INDEX:
            return String.format( "%s CREATE INDEX ON DATABASE %s TO `%s`", privilegeType.prefix, databaseName, role );
        case DROP_INDEX:
            return String.format( "%s DROP INDEX ON DATABASE %s TO `%s`", privilegeType.prefix, databaseName, role );
        case INDEX:
            return String.format( "%s INDEX MANAGEMENT ON DATABASE %s TO `%s`", privilegeType.prefix, databaseName, role );
        case CREATE_CONSTRAINT:
            return String.format( "%s CREATE CONSTRAINT ON DATABASE %s TO `%s`", privilegeType.prefix, databaseName, role );
        case DROP_CONSTRAINT:
            return String.format( "%s DROP CONSTRAINT ON DATABASE %s TO `%s`", privilegeType.prefix, databaseName, role );
        case CONSTRAINT:
            return String.format( "%s CONSTRAINT MANAGEMENT ON DATABASE %s TO `%s`", privilegeType.prefix, databaseName, role );

        case START_DATABASE:
            return String.format( "%s START ON DATABASE %s TO `%s`", privilegeType.prefix, databaseName, role );
        case STOP_DATABASE:
            return String.format( "%s STOP ON DATABASE %s TO `%s`", privilegeType.prefix, databaseName, role );

        case CREATE_DATABASE:
            return String.format( "%s CREATE DATABASE ON DBMS TO `%s`", privilegeType.prefix, role );
        case DROP_DATABASE:
            return String.format( "%s DROP DATABASE ON DBMS TO `%s`", privilegeType.prefix, role );
        case DATABASE_MANAGEMENT:
            return String.format( "%s DATABASE MANAGEMENT ON DBMS TO `%s`", privilegeType.prefix, role );

        case SHOW_TRANSACTION:
            return String.format( "%s SHOW TRANSACTION (%s) ON DATABASE %s TO `%s`", privilegeType.prefix, segment.toString(), databaseName, role );
        case TERMINATE_TRANSACTION:
            return String.format( "%s TERMINATE TRANSACTION (%s) ON DATABASE %s TO `%s`", privilegeType.prefix, segment.toString(), databaseName, role );
        case SHOW_CONNECTION:
            // NOT USED
            break;
        case TERMINATE_CONNECTION:
            // NOT USED
            break;
        case TRANSACTION_MANAGEMENT:
            return String.format( "%s TRANSACTION MANAGEMENT (%s) ON DATABASE %s TO `%s`", privilegeType.prefix, segment.toString(), databaseName, role );


        case SHOW_USER:
            break;
        case CREATE_USER:
            break;
        case SET_USER_STATUS:
            break;
        case SET_PASSWORDS:
            break;
        case DROP_USER:
            break;
        case ALTER_USER:
            break;
        case USER_MANAGEMENT:
            break;

        case SHOW_ROLE:
            break;
        case CREATE_ROLE:
            break;
        case DROP_ROLE:
            break;
        case ASSIGN_ROLE:
            break;
        case REMOVE_ROLE:
            break;
        case ROLE_MANAGEMENT:
            break;

        case SHOW_PRIVILEGE:
            break;
        case ASSIGN_PRIVILEGE:
            break;
        case REMOVE_PRIVILEGE:
            break;
        case PRIVILEGE_MANAGEMENT:
            break;

        case ADMIN:
            break;


        case DATABASE_ACTIONS:
            break;
        case DBMS_ACTIONS:
            break;

        case EXECUTE:
            return String.format( "%s EXECUTE PROCEDURE %s ON DBMS TO `%s`", privilegeType.prefix, segment.toString(), role );
        case ADMIN_PROCEDURE:
            break;

        }
        return "";
    }

    @Override
    public String toString()
    {
        return String.format( "(%s, %s, %s, %s)", privilegeType.prefix, getAction(), getResource(), getSegment() );
    }

    @Override
    public int hashCode()
    {
        return action.hashCode() + 31 * resource.hashCode();
    }

    @Override
    public boolean equals( Object obj )
    {
        if ( obj instanceof ResourcePrivilege )
        {
            ResourcePrivilege other = (ResourcePrivilege) obj;
            return other.action.equals( this.action )
                   && other.resource.equals( this.resource )
                   && other.segment.equals( this.segment )
                   && other.dbName.equals( this.dbName )
                   && other.privilegeType == this.privilegeType
                   && other.allDatabases == this.allDatabases;
        }
        return false;
    }

    public enum GrantOrDeny
    {
        GRANT( "GRANTED" ),
        DENY( "DENIED" );

        public final String name;
        public final String prefix;
        public final String relType;

        GrantOrDeny( String relType )
        {
            this.name = super.toString().toLowerCase();
            this.prefix = super.toString().toUpperCase();
            this.relType = relType;
        }

        public boolean isGrant()
        {
            return this == GRANT;
        }

        public boolean isDeny()
        {
            return this == DENY;
        }

        public static GrantOrDeny fromRelType( String relType )
        {
            for ( GrantOrDeny grantOrDeny : GrantOrDeny.values() )
            {
                if ( grantOrDeny.relType.equals( relType ) )
                {
                    return grantOrDeny;
                }
            }
            throw new IllegalArgumentException( "Unknown privilege type: " + relType );
        }

        @Override
        public String toString()
        {
            return name;
        }
    }

    public enum SpecialDatabase
    {
        ALL,
        DEFAULT
    }
}
