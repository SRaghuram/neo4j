/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery.kubernetes;

/**
 * See <a href="https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.11/#status-v1-meta">Status</a>
 */
public class Status extends KubernetesType
{
    private String status;
    private String message;
    private String reason;
    private int code;

    public String status()
    {
        return status;
    }

    public void setStatus( String status )
    {
        this.status = status;
    }

    public String message()
    {
        return message;
    }

    public void setMessage( String message )
    {
        this.message = message;
    }

    public String reason()
    {
        return reason;
    }

    public void setReason( String reason )
    {
        this.reason = reason;
    }

    public int code()
    {
        return code;
    }

    public void setCode( int code )
    {
        this.code = code;
    }

    @Override
    public <T> T handle( Visitor<T> visitor )
    {
        return visitor.visit( this );
    }

    @Override
    public String toString()
    {
        return "Status{" + "status='" + status + '\'' + ", message='" + message + '\'' + ", reason='" + reason + '\'' + ", code=" + code + '}';
    }
}
