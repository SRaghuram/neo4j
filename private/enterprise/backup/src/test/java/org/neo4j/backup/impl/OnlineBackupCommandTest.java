/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.backup.impl;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import org.neo4j.commandline.admin.CommandLocator;
import org.neo4j.commandline.admin.Usage;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.SuppressOutputExtension;
import org.neo4j.test.rule.SuppressOutput;

import static java.lang.String.format;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;

@ExtendWith( SuppressOutputExtension.class )
class OnlineBackupCommandTest
{
    @Inject
    private SuppressOutput suppressOutput;

    @Test
    void shouldPrintNiceHelp()
    {
        Usage usage = new Usage( "neo4j-admin", mock( CommandLocator.class ) );
        usage.printUsageForCommand( new OnlineBackupCommandProvider(), System.out::println );

        String output = suppressOutput.getOutputVoice().toString();

        assertEquals( format( "usage: neo4j-admin backup --backup-dir=<backup-path> --name=<neo4j-backup>%n" +
                              "                          [--from=<address>] [--database=<neo4j>]%n" +
                              "                          [--fallback-to-full[=<true|false>]] [--pagecache=<8m>]%n" +
                              "                          [--check-consistency[=<true|false>]]%n" +
                              "                          [--cc-report-dir=<directory>]%n" +
                              "                          [--additional-config=<config-file-path>]%n" +
                              "                          [--cc-graph[=<true|false>]]%n" +
                              "                          [--cc-indexes[=<true|false>]]%n" +
                              "                          [--cc-label-scan-store[=<true|false>]]%n" +
                              "                          [--cc-property-owners[=<true|false>]]%n" +
                              "%n" +
                              "environment variables:%n" +
                              "    NEO4J_CONF    Path to directory which contains neo4j.conf.%n" +
                              "    NEO4J_DEBUG   Set to anything to enable debug output.%n" +
                              "    NEO4J_HOME    Neo4j home directory.%n" +
                              "    HEAP_SIZE     Set JVM maximum heap size during command execution.%n" +
                              "                  Takes a number and a unit, for example 512m.%n" +
                              "%n" +
                              "Perform an online backup from a running Neo4j enterprise server. Neo4j's backup%n" +
                              "service must have been configured on the server beforehand.%n" +
                              "%n" +
                              "All consistency checks except 'cc-graph' can be quite expensive so it may be%n" +
                              "useful to turn them off for very large databases. Increasing the heap size can%n" +
                              "also be a good idea. See 'neo4j-admin help' for details.%n" +
                              "%n" +
                              "For more information see:%n" +
                              "https://neo4j.com/docs/operations-manual/current/backup/%n" +
                              "%n" +
                              "options:%n" +
                              "  --backup-dir=<backup-path>               Directory to place backup in.%n" +
                              "  --name=<neo4j-backup>                    Name of backup. If a backup with this%n" +
                              "                                           name already exists an incremental%n" +
                              "                                           backup will be attempted.%n" +
                              "  --from=<address>                         Host and port of Neo4j.%n" +
                              "                                           [default:localhost:6362]%n" +
                              "  --database=<neo4j>                       Name of the remote database to%n" +
                              "                                           backup. [default:null]%n" +
                              "  --fallback-to-full=<true|false>          If an incremental backup fails backup%n" +
                              "                                           will move the old backup to%n" +
                              "                                           <name>.err.<N> and fallback to a full%n" +
                              "                                           backup instead. [default:true]%n" +
                              "  --pagecache=<8m>                         The size of the page cache to use for%n" +
                              "                                           the backup process. [default:8m]%n" +
                              "  --check-consistency=<true|false>         If a consistency check should be%n" +
                              "                                           made. [default:true]%n" +
                              "  --cc-report-dir=<directory>              Directory where consistency report%n" +
                              "                                           will be written. [default:.]%n" +
                              "  --additional-config=<config-file-path>   Configuration file to supply%n" +
                              "                                           additional configuration in. This%n" +
                              "                                           argument is DEPRECATED. [default:]%n" +
                              "  --cc-graph=<true|false>                  Perform consistency checks between%n" +
                              "                                           nodes, relationships, properties,%n" +
                              "                                           types and tokens. [default:true]%n" +
                              "  --cc-indexes=<true|false>                Perform consistency checks on%n" +
                              "                                           indexes. [default:true]%n" +
                              "  --cc-label-scan-store=<true|false>       Perform consistency checks on the%n" +
                              "                                           label scan store. [default:true]%n" +
                              "  --cc-property-owners=<true|false>        Perform additional consistency checks%n" +
                              "                                           on property ownership. This check is%n" +
                              "                                           *very* expensive in time and memory.%n" +
                              "                                           [default:false]%n" ), output );
    }
}
