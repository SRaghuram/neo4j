/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.admin;

import org.junit.Before;
import org.junit.Test;

import org.neo4j.commandline.admin.AdminTool;
import org.neo4j.commandline.admin.CommandLocator;
import org.neo4j.commandline.admin.Usage;

import static org.junit.Assert.assertEquals;

public class Neo4jAdminUsageTest
{
    private Usage usageCmd;

    @Before
    public void setup()
    {
        usageCmd = new Usage( AdminTool.scriptName, CommandLocator.fromServiceLocator() );
    }

    @Test
    public void verifyUsageMatchesExpectedCommands()
    {
        final StringBuilder sb = new StringBuilder();
        usageCmd.print( s -> sb.append( s ).append( "\n" ) );

        assertEquals("usage: neo4j-admin <command>\n" +
                        "\n" +
                        "Manage your Neo4j instance.\n" +
                        "\n" +
                        "environment variables:\n" +
                        "    NEO4J_CONF    Path to directory which contains neo4j.conf.\n" +
                        "    NEO4J_DEBUG   Set to anything to enable debug output.\n" +
                        "    NEO4J_HOME    Neo4j home directory.\n" +
                        "    HEAP_SIZE     Set JVM maximum heap size during command execution.\n" +
                        "                  Takes a number and a unit, for example 512m.\n" +
                        "\n" +
                        "available commands:\n" +
                        "\n" +
                        "General\n" +
                        "    check-consistency\n" +
                        "        Check the consistency of a database.\n" +
                        "    import\n" +
                        "        Import from a collection of CSV files.\n" +
                        "    memrec\n" +
                        "        Print Neo4j heap and pagecache memory settings recommendations.\n" +
                        "    report\n" +
                        "        Produces a zip/tar of the most common information needed for remote assessments.\n" +
                        "    store-info\n" +
                        "        Prints information about a Neo4j database store.\n" +
                        "\n" +
                        "Authentication\n" +
                        "    set-default-admin\n" +
                        "        Sets the default admin user when no roles are present.\n" +
                        "    set-initial-password\n" +
                        "        Sets the initial password of the initial admin user ('neo4j').\n" +
                        "\n" +
                        "Clustering\n" +
                        "    unbind\n" +
                        "        Removes cluster state data for the specified database.\n" +
                        "\n" +
                        "Offline backup\n" +
                        "    dump\n" +
                        "        Dump a database into a single-file archive.\n" +
                        "    load\n" +
                        "        Load a database from an archive created with the dump command.\n" +
                        "\n" +
                        "Online backup\n" +
                        "    backup\n" +
                        "        Perform an online backup from a running Neo4j enterprise server.\n" +
                        "    restore\n" +
                        "        Restore a backed up database.\n" +
                        "\n" +
                        "Use neo4j-admin help <command> for more details.\n",
                sb.toString() );
    }
}
