/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.commandline.dbms;

import javax.annotation.Nonnull;

import org.neo4j.commandline.admin.AdminCommandSection;

public class ClusteringCommandSection extends AdminCommandSection
{
    private static final ClusteringCommandSection clusteringCommandSection = new ClusteringCommandSection();

    public static AdminCommandSection instance()
    {
        return clusteringCommandSection;
    }

    @Override
    @Nonnull
    public String printable()
    {
        return "Clustering";
    }
}
