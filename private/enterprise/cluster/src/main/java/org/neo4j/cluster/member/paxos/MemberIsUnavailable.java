/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cluster.member.paxos;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.net.URI;

import org.neo4j.cluster.InstanceId;

/**
 * This message is broadcast when a member of the cluster declares that
 * it is not ready to serve a particular role for the cluster.
 */
public class MemberIsUnavailable
        implements Externalizable
{
    private String role;
    private InstanceId instanceId;
    private URI clusterUri;

    public MemberIsUnavailable()
    {
    }

    public MemberIsUnavailable( String role, InstanceId instanceId, URI clusterUri )
    {
        this.role = role;
        this.instanceId = instanceId;
        this.clusterUri = clusterUri;
    }

    public String getRole()
    {
        return role;
    }

    public InstanceId getInstanceId()
    {
        return instanceId;
    }

    public URI getClusterUri()
    {
        return clusterUri;
    }

    @Override
    public void writeExternal( ObjectOutput out ) throws IOException
    {
        out.writeUTF( role );
        out.writeObject( instanceId );
        if ( clusterUri != null )
        {
            out.writeUTF( clusterUri.toString() );
        }
    }

    @Override
    public void readExternal( ObjectInput in ) throws IOException, ClassNotFoundException
    {
        role = in.readUTF();
        instanceId = (InstanceId) in.readObject();
        if ( in.available() != 0 )
        {
            clusterUri = URI.create( in.readUTF() );
        }
    }

    @Override
    public String toString()
    {
        return String.format( "MemberIsUnavailable[ Role: %s, InstanceId: %s, ClusterURI: %s ]",
                role, instanceId.toString(), (clusterUri == null) ? null : clusterUri.toString() );
    }
}
