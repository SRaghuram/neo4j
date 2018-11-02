/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.management;

import java.beans.ConstructorProperties;
import java.io.Serializable;
import java.net.MalformedURLException;
import java.net.URI;
import java.util.Arrays;
import javax.management.remote.JMXServiceURL;

import org.neo4j.helpers.collection.Pair;

/**
 * This class captures the least amount of information available for a cluster member to any
 * cluster participant.
 *
 * @deprecated high availability database/edition is deprecated in favour of causal clustering. It will be removed in next major release.
 */
@Deprecated
public class ClusterMemberInfo implements Serializable
{
    private static final long serialVersionUID = -514433972115185753L;

    private String instanceId;
    private boolean available;
    private boolean alive;
    private String haRole;
    private String[] uris;
    private String[] roles;

    @ConstructorProperties( { "instanceId", "available", "alive", "haRole", "uris", "roles" } )
    public ClusterMemberInfo( String instanceId, boolean available, boolean alive, String haRole, String[] uris,
                              String[] roles )
    {
        this.instanceId = instanceId;
        this.available = available;
        this.alive = alive;
        this.haRole = haRole;
        this.uris = uris;
        this.roles = roles;
    }

    public String getInstanceId()
    {
        return instanceId;
    }

    public boolean isAvailable()
    {
        return available;
    }

    public boolean isAlive()
    {
        return alive;
    }

    public String getHaRole()
    {
        return haRole;
    }

    public String[] getUris()
    {
        return uris;
    }

    public String[] getRoles()
    {
        return roles;
    }

    @Override
    @SuppressWarnings( "boxing" )
    public String toString()
    {
        return String.format( "Neo4jHaInstance[id=%s,available=%s,haRole=%s,HA URI=%s]", instanceId, available, haRole,
                Arrays.toString(uris) );
    }

    public Pair<Neo4jManager, HighAvailability> connect()
    {
        return connect( null, null );
    }

    public Pair<Neo4jManager, HighAvailability> connect( String username, String password )
    {
        URI address = null;
        for ( String uri : uris )
        {
            if ( uri.startsWith( "jmx" ) )
            {
//                address = uri;
            }
        }
        if ( address == null )
        {
            throw new IllegalStateException( "The instance does not have a public JMX server." );
        }
        Neo4jManager manager = Neo4jManager.get( url(address), username, password, instanceId );
        return Pair.of( manager, manager.getHighAvailabilityBean() );
    }

    private JMXServiceURL url( URI address )
    {
        try
        {
            return new JMXServiceURL( address.toASCIIString() );
        }
        catch ( MalformedURLException e )
        {
            throw new IllegalStateException( "The instance does not have a valid JMX server URL." );
        }
    }
}
