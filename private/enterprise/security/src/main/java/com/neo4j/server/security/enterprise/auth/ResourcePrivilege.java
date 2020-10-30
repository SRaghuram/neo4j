/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.server.security.enterprise.auth;

import java.util.List;

import org.neo4j.internal.kernel.api.security.FunctionSegment;
import org.neo4j.internal.kernel.api.security.PrivilegeAction;
import org.neo4j.internal.kernel.api.security.ProcedureSegment;
import org.neo4j.internal.kernel.api.security.Segment;
import org.neo4j.kernel.api.exceptions.InvalidArgumentsException;

import static org.neo4j.configuration.GraphDatabaseSettings.SYSTEM_DATABASE_NAME;
import static org.neo4j.internal.kernel.api.security.PrivilegeAction.ADMIN;
import static org.neo4j.internal.kernel.api.security.PrivilegeAction.DBMS_ACTIONS;
import static org.neo4j.internal.kernel.api.security.PrivilegeAction.EXECUTE;
import static org.neo4j.internal.kernel.api.security.PrivilegeAction.EXECUTE_BOOSTED;
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
        return allDatabases || database.equals( dbName ) || TRANSACTION_MANAGEMENT.satisfies( action );
    }

    public boolean appliesToDefault()
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

    public List<String> asCommandFor( boolean asRevoke, String roleKeyword, boolean withRoleParam ) throws RuntimeException
    {
        return asCommandFor( asRevoke, roleKeyword, withRoleParam, "", false );
    }

    public List<String> asCommandFor( boolean asRevoke, String role, String databaseParam ) throws RuntimeException
    {
        return asCommandFor( asRevoke, role, false, databaseParam, true );
    }

    private List<String> asCommandFor( boolean asRevoke, String roleKeyword, boolean withRoleParam, String databaseKeyword, boolean withDatabaseParam )
            throws RuntimeException
    {
        String preposition = asRevoke ? "FROM" : "TO";
        String optionalRevoke = asRevoke ? "REVOKE " : "";
        String role = withRoleParam ? "$" + roleKeyword : "`" + roleKeyword + "`";

        switch ( action )
        {
        case ACCESS:
            return List.of( String.format( "%s%s ACCESS ON %s %s %s",
                    optionalRevoke, privilegeType.prefix, forScope( "DATABASE", databaseKeyword, withDatabaseParam ), preposition, role ) );

        case TRAVERSE:
            return List.of( String.format( "%s%s TRAVERSE ON %s %s %s %s", optionalRevoke, privilegeType.prefix,
                                           forScope("GRAPH", databaseKeyword, withDatabaseParam), segment.toString(), preposition, role ) );
        case READ:
            return List.of( String.format( "%s%s READ {%s} ON %s %s %s %s", optionalRevoke, privilegeType.prefix, resource.toString(),
                    forScope("GRAPH", databaseKeyword, withDatabaseParam), segment.toString(), preposition, role ) );
        case MATCH:
            return List.of( String.format( "%s%s MATCH {%s} ON %s %s %s %s", optionalRevoke, privilegeType.prefix, resource.toString(),
                    forScope("GRAPH", databaseKeyword, withDatabaseParam), segment.toString(), preposition, role ) );

        case MERGE:
            if ( privilegeType.prefix.equals( GrantOrDeny.GRANT.prefix ) )
            {
                return List.of( String.format( "%s%s MERGE {%s} ON %s %s %s %s", optionalRevoke, privilegeType.prefix, resource.toString(),
                        forScope("GRAPH", databaseKeyword, withDatabaseParam), segment.toString(), preposition, role ) );
            }
            else
            {
                // not supported
                return List.of();
            }

        case WRITE:
            return List.of( String.format( "%s%s WRITE ON %s %s %s", optionalRevoke, privilegeType.prefix,
                    forScope("GRAPH", databaseKeyword, withDatabaseParam), preposition, role ) );

        case SET_LABEL:
            return List.of( String.format( "%s%s SET LABEL %s ON %s %s %s", optionalRevoke, privilegeType.prefix, resource.toString(),
                    forScope("GRAPH", databaseKeyword, withDatabaseParam), preposition, role ) );
        case REMOVE_LABEL:
            return List.of( String.format( "%s%s REMOVE LABEL %s ON %s %s %s", optionalRevoke, privilegeType.prefix, resource.toString(),
                    forScope("GRAPH", databaseKeyword, withDatabaseParam), preposition, role ) );
        case CREATE_ELEMENT:
            return List.of( String.format( "%s%s CREATE ON %s %s %s %s", optionalRevoke, privilegeType.prefix,
                    forScope("GRAPH", databaseKeyword, withDatabaseParam), segment.toString(), preposition, role ) );
        case DELETE_ELEMENT:
            return List.of( String.format( "%s%s DELETE ON %s %s %s %s", optionalRevoke, privilegeType.prefix,
                    forScope("GRAPH", databaseKeyword, withDatabaseParam), segment.toString(), preposition, role ) );
        case SET_PROPERTY:
            return List.of( String.format( "%s%s SET PROPERTY {%s} ON %s %s %s %s", optionalRevoke, privilegeType.prefix, resource.toString(),
                    forScope("GRAPH", databaseKeyword, withDatabaseParam), segment.toString(), preposition, role ) );

        case GRAPH_ACTIONS:
            return List.of( String.format( "%s%s ALL GRAPH PRIVILEGES ON %s %s %s", optionalRevoke, privilegeType.prefix,
                    forScope("GRAPH", databaseKeyword, withDatabaseParam), preposition, role ) );

        case CREATE_LABEL:
            return List.of( String.format( "%s%s CREATE NEW NODE LABEL ON %s %s %s", optionalRevoke, privilegeType.prefix,
                    forScope("DATABASE", databaseKeyword, withDatabaseParam), preposition, role ) );
        case CREATE_RELTYPE:
            return List.of( String.format( "%s%s CREATE NEW RELATIONSHIP TYPE ON %s %s %s", optionalRevoke, privilegeType.prefix,
                    forScope("DATABASE", databaseKeyword, withDatabaseParam), preposition, role ) );
        case CREATE_PROPERTYKEY:
            return List.of( String.format( "%s%s CREATE NEW PROPERTY NAME ON %s %s %s", optionalRevoke, privilegeType.prefix,
                    forScope("DATABASE", databaseKeyword, withDatabaseParam), preposition, role ) );
        case TOKEN:
            return List.of( String.format( "%s%s NAME MANAGEMENT ON %s %s %s", optionalRevoke, privilegeType.prefix,
                    forScope("DATABASE", databaseKeyword, withDatabaseParam), preposition, role ) );

        case CREATE_INDEX:
            return List.of( String.format( "%s%s CREATE INDEX ON %s %s %s", optionalRevoke, privilegeType.prefix,
                    forScope("DATABASE", databaseKeyword, withDatabaseParam), preposition, role ) );
        case DROP_INDEX:
            return List.of( String.format( "%s%s DROP INDEX ON %s %s %s", optionalRevoke, privilegeType.prefix,
                    forScope("DATABASE", databaseKeyword, withDatabaseParam), preposition, role ) );
        case SHOW_INDEX:
            return List.of( String.format( "%s%s SHOW INDEX ON %s %s %s", optionalRevoke, privilegeType.prefix,
                    forScope("DATABASE", databaseKeyword, withDatabaseParam), preposition, role ) );
        case INDEX:
            return List.of( String.format( "%s%s INDEX MANAGEMENT ON %s %s %s", optionalRevoke, privilegeType.prefix,
                    forScope("DATABASE", databaseKeyword, withDatabaseParam), preposition, role ) );
        case CREATE_CONSTRAINT:
            return List.of( String.format( "%s%s CREATE CONSTRAINT ON %s %s %s", optionalRevoke, privilegeType.prefix,
                    forScope("DATABASE", databaseKeyword, withDatabaseParam), preposition, role ) );
        case DROP_CONSTRAINT:
            return List.of( String.format( "%s%s DROP CONSTRAINT ON %s %s %s", optionalRevoke, privilegeType.prefix,
                    forScope("DATABASE", databaseKeyword, withDatabaseParam), preposition, role ) );
        case SHOW_CONSTRAINT:
            return List.of( String.format( "%s%s SHOW CONSTRAINT ON %s %s %s", optionalRevoke, privilegeType.prefix,
                    forScope("DATABASE", databaseKeyword, withDatabaseParam), preposition, role ) );
        case CONSTRAINT:
            return List.of( String.format( "%s%s CONSTRAINT MANAGEMENT ON %s %s %s", optionalRevoke, privilegeType.prefix,
                    forScope("DATABASE", databaseKeyword, withDatabaseParam), preposition, role ) );

        case START_DATABASE:
            return List.of( String.format( "%s%s START ON %s %s %s", optionalRevoke, privilegeType.prefix,
                    forScope("DATABASE", databaseKeyword, withDatabaseParam), preposition, role ) );
        case STOP_DATABASE:
            return List.of( String.format( "%s%s STOP ON %s %s %s", optionalRevoke, privilegeType.prefix,
                    forScope("DATABASE", databaseKeyword, withDatabaseParam), preposition, role ) );

        case SHOW_TRANSACTION:
            return List.of( String.format( "%s%s SHOW TRANSACTION (%s) ON %s %s %s", optionalRevoke, privilegeType.prefix, segment.toString(),
                    forScope("DATABASE", databaseKeyword, withDatabaseParam), preposition, role ) );
        case TERMINATE_TRANSACTION:
            return List.of( String.format( "%s%s TERMINATE TRANSACTION (%s) ON %s %s %s", optionalRevoke, privilegeType.prefix, segment.toString(),
                    forScope("DATABASE", databaseKeyword, withDatabaseParam), preposition, role ) );
        case SHOW_CONNECTION:
            // NOT USED
            return List.of();
        case TERMINATE_CONNECTION:
            // NOT USED
            return List.of();
        case TRANSACTION_MANAGEMENT:
            return List.of( String.format( "%s%s TRANSACTION MANAGEMENT (%s) ON %s %s %s", optionalRevoke, privilegeType.prefix, segment.toString(),
                    forScope("DATABASE", databaseKeyword, withDatabaseParam), preposition, role ) );

        case DATABASE_ACTIONS:
            return List.of( String.format( "%s%s ALL DATABASE PRIVILEGES ON %s %s %s", optionalRevoke, privilegeType.prefix,
                    forScope("DATABASE", databaseKeyword, withDatabaseParam), preposition, role ) );

        case CREATE_DATABASE:
            return List.of( String.format( "%s%s CREATE DATABASE ON DBMS %s %s", optionalRevoke, privilegeType.prefix, preposition, role ) );
        case DROP_DATABASE:
            return List.of( String.format( "%s%s DROP DATABASE ON DBMS %s %s", optionalRevoke, privilegeType.prefix, preposition, role ) );
        case DATABASE_MANAGEMENT:
            return List.of( String.format( "%s%s DATABASE MANAGEMENT ON DBMS %s %s", optionalRevoke, privilegeType.prefix, preposition, role ) );

        case SHOW_USER:
            return List.of( String.format( "%s%s SHOW USER ON DBMS %s %s", optionalRevoke, privilegeType.prefix, preposition, role ) );
        case CREATE_USER:
            return List.of( String.format( "%s%s CREATE USER ON DBMS %s %s", optionalRevoke, privilegeType.prefix, preposition, role ) );
        case SET_USER_STATUS:
            return List.of( String.format( "%s%s SET USER STATUS ON DBMS %s %s", optionalRevoke, privilegeType.prefix, preposition, role ) );
        case SET_PASSWORDS:
            return List.of( String.format( "%s%s SET PASSWORD ON DBMS %s %s", optionalRevoke, privilegeType.prefix, preposition, role ) );
        case DROP_USER:
            return List.of( String.format( "%s%s DROP USER ON DBMS %s %s", optionalRevoke, privilegeType.prefix, preposition, role ) );
        case ALTER_USER:
            return List.of( String.format( "%s%s ALTER USER ON DBMS %s %s", optionalRevoke, privilegeType.prefix, preposition, role ) );
        case USER_MANAGEMENT:
            return List.of( String.format( "%s%s USER MANAGEMENT ON DBMS %s %s", optionalRevoke, privilegeType.prefix, preposition, role ) );

        case SHOW_ROLE:
            return List.of( String.format( "%s%s SHOW ROLE ON DBMS %s %s", optionalRevoke, privilegeType.prefix, preposition, role ) );
        case CREATE_ROLE:
            return List.of( String.format( "%s%s CREATE ROLE ON DBMS %s %s", optionalRevoke, privilegeType.prefix, preposition, role ) );
        case DROP_ROLE:
            return List.of( String.format( "%s%s DROP ROLE ON DBMS %s %s", optionalRevoke, privilegeType.prefix, preposition, role ) );
        case ASSIGN_ROLE:
            return List.of( String.format( "%s%s ASSIGN ROLE ON DBMS %s %s", optionalRevoke, privilegeType.prefix, preposition, role ) );
        case REMOVE_ROLE:
            return List.of( String.format( "%s%s REMOVE ROLE ON DBMS %s %s", optionalRevoke, privilegeType.prefix, preposition, role ) );
        case ROLE_MANAGEMENT:
            return List.of( String.format( "%s%s ROLE MANAGEMENT ON DBMS %s %s", optionalRevoke, privilegeType.prefix, preposition, role ) );

        case SHOW_PRIVILEGE:
            return List.of( String.format( "%s%s SHOW PRIVILEGE ON DBMS %s %s", optionalRevoke, privilegeType.prefix, preposition, role ) );
        case ASSIGN_PRIVILEGE:
            return List.of( String.format( "%s%s ASSIGN PRIVILEGE ON DBMS %s %s", optionalRevoke, privilegeType.prefix, preposition, role ) );
        case REMOVE_PRIVILEGE:
            return List.of( String.format( "%s%s REMOVE PRIVILEGE ON DBMS %s %s", optionalRevoke, privilegeType.prefix, preposition, role ) );
        case PRIVILEGE_MANAGEMENT:
            return List.of( String.format( "%s%s PRIVILEGE MANAGEMENT ON DBMS %s %s", optionalRevoke, privilegeType.prefix, preposition, role ) );

        case ADMIN:
            return List.of(
                    String.format( "%s%s ALL DBMS PRIVILEGES ON DBMS %s %s", optionalRevoke, privilegeType.prefix, preposition, role ),
                    String.format( "%s%s TRANSACTION MANAGEMENT (*) ON %s %s %s", optionalRevoke, privilegeType.prefix,
                            forScope("DATABASE", databaseKeyword, withDatabaseParam), preposition, role ),
                    String.format( "%s%s START ON %s %s %s", optionalRevoke, privilegeType.prefix,
                            forScope("DATABASE", databaseKeyword, withDatabaseParam), preposition, role ),
                    String.format( "%s%s STOP ON %s %s %s", optionalRevoke, privilegeType.prefix,
                            forScope("DATABASE", databaseKeyword, withDatabaseParam), preposition, role )
            );

        case DBMS_ACTIONS:
            return List.of( String.format( "%s%s ALL DBMS PRIVILEGES ON DBMS %s %s", optionalRevoke, privilegeType.prefix, preposition, role ) );

        case EXECUTE:
            if ( segment instanceof ProcedureSegment )
            {
                return List.of(String.format(
                        "%s%s EXECUTE PROCEDURE %s ON DBMS %s %s", optionalRevoke, privilegeType.prefix, segment.toString(), preposition, role ) );
            }
            else if ( segment instanceof FunctionSegment )
            {
                return List.of(String.format(
                        "%s%s EXECUTE FUNCTION %s ON DBMS %s %s", optionalRevoke, privilegeType.prefix, segment.toString(), preposition, role ) );
            }
            else
            {
                throw new RuntimeException( String.format( "Failed to convert ResourcePrivilege to grant: wrong qualifier for action '%s', found '%s'",
                        action.toString(), segment.toString() ) );
            }
        case EXECUTE_BOOSTED:
            if ( segment instanceof ProcedureSegment )
            {
                return List.of( String.format( "%s%s EXECUTE BOOSTED PROCEDURE %s ON DBMS %s %s", optionalRevoke, privilegeType.prefix, segment.toString(),
                        preposition, role ) );
            }
            else if ( segment instanceof FunctionSegment )
            {
                return List.of( String.format( "%s%s EXECUTE BOOSTED FUNCTION %s ON DBMS %s %s", optionalRevoke, privilegeType.prefix, segment.toString(),
                        preposition, role ) );
            }
            else
            {
                throw new RuntimeException( String.format( "Failed to convert ResourcePrivilege to grant: wrong qualifier for action '%s', found '%s'",
                        action.toString(), segment.toString() ) );
            }
        case EXECUTE_ADMIN:
            return List.of( String.format( "%s%s EXECUTE ADMIN PROCEDURES ON DBMS %s %s", optionalRevoke, privilegeType.prefix, preposition, role ) );
        default:
            // throw error
        }
        throw new RuntimeException( String.format( "Failed to convert ResourcePrivilege to grant: conversion for action '%s' is missing", action.toString() ) );
    }

    private String forScope( String keyword, String parameter, boolean withDatabaseParameter )
    {
        if ( withDatabaseParameter )
        {
            return keyword + " $" + parameter;
        }
        else if ( allDatabases )
        {
            return keyword + " *";
        }
        else if ( defaultDatabase )
        {
            return "DEFAULT " + keyword;
        }
        else
        {
            return keyword + " `" + dbName + "`";
        }
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

    public boolean isDbmsPrivilege()
    {
        return DBMS_ACTIONS.satisfies( action ) ||
               EXECUTE.satisfies( action ) ||
               EXECUTE_BOOSTED.satisfies( action ) ||
               ADMIN == action;
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
