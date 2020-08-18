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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.function.Consumer;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.helpers.Exceptions;
import org.neo4j.util.Preconditions;

import static org.neo4j.configuration.GraphDatabaseSettings.SYSTEM_DATABASE_NAME;

/**
 * Central collection for managing multiple versioned system graph initializers. There could be several components in the DBMS that each have a requirement on
 * the system database to contain a graph with a specific schema. Each component needs to maintain that schema and support multiple versions of that schema in
 * order to allow rolling upgrades to be possible where newer versions of Neo4j will be able to run on older versions of the system database.
 * <p>
 * The core design is that each component is able to detect the version of their own sub-graph and from that decide if they can support it or not, and how to
 * upgrade from one version to another.
 */
public class SystemGraphComponents implements SystemGraphComponent
{
    private final HashMap<String,SystemGraphComponent> componentMap = new HashMap<>();
    private final ArrayList<SystemGraphComponent> components = new ArrayList<>();

    public void register( SystemGraphComponent initializer )
    {
        deregister( initializer.component() );
        componentMap.put( initializer.component(), initializer );
        components.add( initializer );
    }

    @SuppressWarnings( "WeakerAccess" )
    public void deregister( String key )
    {
        SystemGraphComponent removed = componentMap.remove( key );
        if ( removed != null )
        {
            components.remove( removed );
        }
    }

    public void forEach( Consumer<SystemGraphComponent> process )
    {
        components.forEach( process );
    }

    @Override
    public String component()
    {
        return "system-graph";
    }

    @Override
    public SystemGraphComponent.Status detect( Transaction tx )
    {
        return components.stream().map( c -> c.detect( tx ) ).reduce( SystemGraphComponent.Status::with ).orElse( SystemGraphComponent.Status.CURRENT );
    }

    @Override
    public void initializeSystemGraph( GraphDatabaseService system )
    {
        Preconditions.checkState( system.databaseName().equals( SYSTEM_DATABASE_NAME ),
                "Cannot initialize system graph on database '" + system.databaseName() + "'" );

        Exception failure = null;
        for ( SystemGraphComponent component : components )
        {
            try
            {
                component.initializeSystemGraph( system );
            }
            catch ( Exception e )
            {
                failure = Exceptions.chain( failure, e );
            }
        }

        if ( failure != null )
        {
            throw new IllegalStateException( "Failed to initialize system graph component: " + failure.getMessage(), failure );
        }
    }

    @Override
    public void upgradeToCurrent( GraphDatabaseService system ) throws Exception
    {
        Exception failure = null;
        for ( SystemGraphComponent component : componentsToUpgrade( system ) )
        {
            try
            {
                component.upgradeToCurrent( system );
            }
            catch ( Exception e )
            {
                failure = Exceptions.chain( failure, e );
            }
        }

        if ( failure != null )
        {
            throw new IllegalStateException( "Failed to upgrade system graph:" + failure.getMessage(), failure );
        }
    }

    private List<SystemGraphComponent> componentsToUpgrade( GraphDatabaseService system ) throws Exception
    {
        List<SystemGraphComponent> componentsToUpgrade = new ArrayList<>();
        SystemGraphComponent.executeWithFullAccess( system, tx -> components.stream().filter( c ->
        {
                        Status status = c.detect( tx );
            return status == Status.UNSUPPORTED_BUT_CAN_UPGRADE || status == Status.REQUIRES_UPGRADE;
        } ).forEach( componentsToUpgrade::add ) );
        return componentsToUpgrade;
    }
}
