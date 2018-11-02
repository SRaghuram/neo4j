/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cluster.com;

import org.neo4j.cluster.BindingListener;

/**
 * Instances of this interface notify listeners when the cluster client has been bound to a particular network interface
 * and port.
 */
public interface BindingNotifier
{
    void addBindingListener( BindingListener listener );

    void removeBindingListener( BindingListener listener );
}
