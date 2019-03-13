/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.server.security.enterprise.auth;

import java.util.HashSet;
import java.util.Set;

public class DatabasePrivilege
{
    private final String dbname;
    private Set<ResourcePrivilege> privileges = new HashSet<>();
    private boolean isAdmin;

    public DatabasePrivilege( String dbname )
    {
        this.dbname = dbname;
    }

    public void addPrivilege( ResourcePrivilege privilege )
    {
        privileges.add( privilege );
    }

    public void removePrivilege( ResourcePrivilege privilege )
    {
        privileges.remove( privilege );
    }

    public Set<ResourcePrivilege> getPrivileges()
    {
        return privileges;
    }

    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder();
        sb.append( dbname ).append( " { " );
        for ( ResourcePrivilege privilege : privileges )
        {
            sb.append( privilege.toString() );
        }
        if ( isAdmin )
        {
            sb.append( "(admin)" );
        }
        sb.append( " }" );
        return sb.toString();
    }

    @Override
    public int hashCode()
    {
        int hashCode = dbname.hashCode();
        hashCode += 31 * privileges.hashCode();
        if ( isAdmin )
        {
            hashCode *= 7;
        }
        return hashCode;
    }

    @Override
    public boolean equals( Object obj )
    {
        if ( obj instanceof DatabasePrivilege )
        {
            DatabasePrivilege other = (DatabasePrivilege) obj;
            return other.dbname.equals( this.dbname ) && other.privileges.equals( this.privileges ) && other.isAdmin == this.isAdmin;
        }
        return false;
    }

    public String getDbName()
    {
        return this.dbname;
    }

    public void setAdmin( boolean setToAdmin )
    {
        isAdmin = setToAdmin;
    }

    public boolean isAdmin()
    {
        return isAdmin;
    }
}
