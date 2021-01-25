/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.dbms.commandline;

import org.junit.jupiter.api.Test;
import picocli.CommandLine;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.file.Path;

import org.neo4j.cli.ExecutionContext;

import static com.neo4j.dbms.commandline.StoreCopyCommand.quoteAwareSplit;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

class StoreCopyCommandTest
{
    @Test
    void printUsageHelp()
    {
        final var baos = new ByteArrayOutputStream();
        final var command = new StoreCopyCommand( new ExecutionContext( Path.of( "." ), Path.of( "." ) ) );
        try ( var out = new PrintStream( baos ) )
        {
            CommandLine.usage( command, new PrintStream( out ), CommandLine.Help.Ansi.OFF );
        }
        assertThat( baos.toString().trim()).isEqualTo( String.format(
                "Copy a database and optionally apply filters.%n" +
                "%n" +
                "USAGE%n" +
                "%n" +
                "copy [--force] [--verbose] [--from-pagecache=<size>] [--from-path-tx=<path>]%n" +
                "     [--neo4j-home-directory=<path>] --to-database=<database>%n" +
                "     [--to-format=<format>] [--to-pagecache=<size>]%n" +
                "     [--delete-nodes-with-labels=<label>[,<label>...]]...%n" +
                "     [--keep-only-node-properties=<label.property>[,<label.property>...]]...%n" +
                "     [--keep-only-nodes-with-labels=<label>[,<label>...]]...%n" +
                "     [--keep-only-relationship-properties=<relationship.property>[,%n" +
                "     <relationship.property>...]]... [--skip-labels=<label>[,<label>...]]...%n" +
                "     [--skip-node-properties=<label.property>[,<label.property>...]]...%n" +
                "     [--skip-properties=<property>[,<property>...]]...%n" +
                "     [--skip-relationship-properties=<relationship.property>[,<relationship.%n" +
                "     property>...]]... [--skip-relationships=<relationship>[,%n" +
                "     <relationship>...]]... (--from-database=<database> | --from-path=<path>)%n" +
                "%n" +
                "DESCRIPTION%n" +
                "%n" +
                "This command will create a copy of a database.%n" +
                "If your labels, properties or relationships contain dots or commas you can use%n" +
                "` to escape them, e.g. `My,label`.property%n" +
                "%n" +
                "OPTIONS%n" +
                "%n" +
                "      --verbose              Enable verbose output.%n" +
                "      --from-database=<database>%n" +
                "                             Name of database to copy from.%n" +
                "      --from-path=<path>     Path to the database to copy from.%n" +
                "      --from-path-tx=<path>  Path to the transaction files, if they are not in%n" +
                "                               the same folder as '--from-path'.%n" +
                "      --to-database=<database>%n" +
                "                             Name of database to copy to.%n" +
                "      --neo4j-home-directory=<path>%n" +
                "                             Path to the home directory for the copied%n" +
                "                               database. Default is the same as the database%n" +
                "                               copied from.%n" +
                "      --force                Force the command to run even if the integrity of%n" +
                "                               the database can not be verified.%n" +
                "      --to-format=<format>   Set the format for the new database. Must be one%n" +
                "                               of same, standard, high_limit, aligned. 'same'%n" +
                "                               will use the same format as the source. WARNING:%n" +
                "                               If you go from 'high_limit' to 'standard' or%n" +
                "                               'aligned' there is no validation that the data%n" +
                "                               will actually fit.%n" +
                "                               Default: same%n" +
                "      --delete-nodes-with-labels=<label>[,<label>...]%n" +
                "                             A comma separated list of labels. All nodes that%n" +
                "                               have ANY of the specified labels will be deleted.%n" +
                "                               Can not be combined with%n" +
                "                               --keep-only-nodes-with-labels.%n" +
                "      --keep-only-nodes-with-labels=<label>[,<label>...]%n" +
                "                             A comma separated list of labels. All nodes that%n" +
                "                               have ANY of the specified labels will be kept.%n" +
                "                               Can not be combined with%n" +
                "                               --delete-nodes-with-labels.%n" +
                "      --skip-labels=<label>[,<label>...]%n" +
                "                             A comma separated list of labels to ignore.%n" +
                "      --skip-properties=<property>[,<property>...]%n" +
                "                             A comma separated list of property keys to ignore.%n" +
                "                               Can not be combined with --skip-node-properties,%n" +
                "                               --keep-only-node-properties,%n" +
                "                               --skip-relationship-properties or%n" +
                "                               --keep-only-relationship-properties.%n" +
                "      --skip-node-properties=<label.property>[,<label.property>...]%n" +
                "                             A comma separated list of property keys to ignore%n" +
                "                               for nodes with the specified label. Can not be%n" +
                "                               combined with --skip-properties or%n" +
                "                               --keep-only-node-properties.%n" +
                "      --keep-only-node-properties=<label.property>[,<label.property>...]%n" +
                "                             A comma separated list of property keys to keep%n" +
                "                               for nodes with the specified label. Any labels%n" +
                "                               not explicitly mentioned will keep their%n" +
                "                               properties. Can not be combined with%n" +
                "                               --skip-properties or --skip-node-properties.%n" +
                "      --skip-relationship-properties=<relationship.property>[,<relationship.%n" +
                "        property>...]%n" +
                "                             A comma separated list of property keys to ignore%n" +
                "                               for relationships with the specified type. Can%n" +
                "                               not be combined with --skip-properties or%n" +
                "                               --keep-only-relationship-properties.%n" +
                "      --keep-only-relationship-properties=<relationship.property>[,%n" +
                "        <relationship.property>...]%n" +
                "                             A comma separated list of property keys to keep%n" +
                "                               for relationships with the specified type. Any%n" +
                "                               relationship types not explicitly mentioned will%n" +
                "                               keep their properties. Can not be combined with%n" +
                "                               --skip-properties or%n" +
                "                               --skip-relationship-properties.%n" +
                "      --skip-relationships=<relationship>[,<relationship>...]%n" +
                "                             A comma separated list of relationships to ignore.%n" +
                "      --from-pagecache=<size>%n" +
                "                             The size of the page cache to use for reading.%n" +
                "                               Default: 8m%n" +
                "      --to-pagecache=<size>  (Advanced) The size of the page cache to use for%n" +
                "                               writing. If not specified then an optimal size%n" +
                "                               will be automatically selected"
        ) );
    }

    @Test
    void quoteAwareSplitTest()
    {
        assertThat( quoteAwareSplit( "A,B,C", ',', false ) ).containsExactly( "A", "B", "C" );
        assertThat( quoteAwareSplit( "`A,a`,B,C", ',', false ) ).containsExactly( "`A,a`", "B", "C" );
        assertThat( quoteAwareSplit( "`A,a`,`B`.`b`,C", ',', false ) ).containsExactly( "`A,a`", "`B`.`b`", "C" );
        assertThat( quoteAwareSplit( "`A,a`,`B`.`b`,`C`", ',', false ) ).containsExactly( "`A,a`", "`B`.`b`", "`C`" );
        assertThat( quoteAwareSplit( "A,B,C", ',', true ) ).containsExactly( "A", "B", "C" );
        assertThat( quoteAwareSplit( "`A,a`,B,`C`", ',', true ) ).containsExactly( "A,a", "B", "C" );

        assertThrows( CommandLine.TypeConversionException.class, () -> quoteAwareSplit( "A,,C", ',', false ) );
        assertThrows( CommandLine.TypeConversionException.class, () -> quoteAwareSplit( "`A``,B,C", ',', true ) );
        assertThrows( CommandLine.TypeConversionException.class, () -> quoteAwareSplit( "`A,B,C", ',', true ) );
        assertThrows( CommandLine.TypeConversionException.class, () -> quoteAwareSplit( "A,B`B,C", ',', true ) );
        assertThrows( CommandLine.TypeConversionException.class, () -> quoteAwareSplit( "`A`a,B,C", ',', true ) );
        assertThrows( CommandLine.TypeConversionException.class, () -> quoteAwareSplit( "A,``,C", ',', true ) );
        assertThrows( CommandLine.TypeConversionException.class, () -> quoteAwareSplit( "A,B,``", ',', true ) );
        assertThrows( CommandLine.TypeConversionException.class, () -> quoteAwareSplit( "A,B,", ',', true ) );
        assertThrows( CommandLine.TypeConversionException.class, () -> quoteAwareSplit( ",A,B", ',', true ) );
    }
}
