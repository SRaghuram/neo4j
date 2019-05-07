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
    private final boolean allDatabases;
    private Set<ResourcePrivilege> privileges = new HashSet<>();

    public DatabasePrivilege()
    {
        dbname = "";
        allDatabases = true;
    }

    public DatabasePrivilege( String dbname )
    {
        assert !dbname.isEmpty();
        this.dbname = dbname;
        allDatabases = false;
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
        sb.append( " }" );
        return sb.toString();
    }

    @Override
    public int hashCode()
    {
        return dbname.hashCode() + 31 * privileges.hashCode();
    }

    @Override
    public boolean equals( Object obj )
    {
        if ( obj instanceof DatabasePrivilege )
        {
            DatabasePrivilege other = (DatabasePrivilege) obj;
            return other.dbname.equals( this.dbname ) && other.privileges.equals( this.privileges );
        }
        return false;
    }

    public String getDbName()
    {
        return this.dbname;
    }

    public boolean isAllDatabases()
    {
        return allDatabases;
    }
}
