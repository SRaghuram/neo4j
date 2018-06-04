#
# Copyright (c) 2002-2018 "Neo Technology,"
# Network Engine for Objects in Lund AB [http://neotechnology.com]
# This file is a commercial add-on to Neo4j Enterprise Edition.
#

<#
.SYNOPSIS
Writes a string to STDERR

.DESCRIPTION
Writes a string to STDERR.  Will use an appropriate function call depending on the Powershell Host.

.PARAMETER Message
A string to write

.EXAMPLE
Write-StdErr "This is a message"

Outputs the string onto STDERR

.NOTES
This function is private to the powershell module

#>
Function Write-StdErr($Message) {
  if ($Host.Name -eq 'ConsoleHost') { 
    [Console]::Error.WriteLine($Message)
  } else {
    $host.UI.WriteErrorLine($Message)
  }
}
