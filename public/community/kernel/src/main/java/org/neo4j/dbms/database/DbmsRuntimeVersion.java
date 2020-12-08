/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.dbms.database;

public enum DbmsRuntimeVersion implements ComponentVersion
{
    V4_1( 1, DBMS_RUNTIME_COMPONENT),
    V4_2( 2, DBMS_RUNTIME_COMPONENT);

    public static final DbmsRuntimeVersion LATEST_DBMS_RUNTIME_COMPONENT_VERSION = V4_2;

    DbmsRuntimeVersion(int version, String componentName)
    {
        this.version = version;
        this.componentName = componentName;
    }

    private final String componentName;
    private final int version;

    @Override
    public int getVersion()
    {
        return version;
    }

    //TODO: remove once description is not mandatory
    @Override
    public String getDescription()
    {
        return null;
    }

    @Override
    public String getComponentName()
    {
        return componentName;
    }

    @Override
    public boolean isCurrent()
    {
        return version == LATEST_DBMS_RUNTIME_COMPONENT_VERSION.version;
    }

    @Override
    public boolean migrationSupported()
    {
        return true;
    }

    @Override
    public boolean runtimeSupported()
    {
        return true;
    }

    public static DbmsRuntimeVersion fromVersionNumber( int versionNumber )
    {
        for ( DbmsRuntimeVersion componentVersion : DbmsRuntimeVersion.values() )
        {
            if ( componentVersion.version == versionNumber )
            {
                return componentVersion;
            }
        }
        throw new IllegalArgumentException( "Unrecognised DBMS runtime version number: " + versionNumber );
    }
}
