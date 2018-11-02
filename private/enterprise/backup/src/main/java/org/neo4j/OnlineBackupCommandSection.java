/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j;

import javax.annotation.Nonnull;

import org.neo4j.commandline.admin.AdminCommandSection;

public class OnlineBackupCommandSection extends AdminCommandSection
{
    private static final OnlineBackupCommandSection ONLINE_BACKUP_COMMAND_SECTION = new OnlineBackupCommandSection();

    public static AdminCommandSection instance()
    {
        return ONLINE_BACKUP_COMMAND_SECTION;
    }

    @Override
    @Nonnull
    public String printable()
    {
        return "Online backup";
    }

}
