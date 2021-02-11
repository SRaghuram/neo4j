#
# Copyright (c) "Neo4j"
# Neo4j Sweden AB [http://neo4j.com]
# This file is a commercial add-on to Neo4j Enterprise Edition.
#

<#
.SYNOPSIS
Invokes a command which manages a Neo4j Server

.DESCRIPTION
Invokes a command which manages a Neo4j Server.

Invoke this function with a blank or missing command to list available commands

.PARAMETER CommandArgs
Remaining command line arguments are passed to the admin tool

.EXAMPLE
Invoke-Neo4jAdmin help

Prints the help text

.OUTPUTS
System.Int32
0 = Success
non-zero = an error occured

.NOTES
Only supported on version 4.x Neo4j Community and Enterprise Edition databases

#>
function Invoke-Neo4jAdmin
{
  [CmdletBinding(SupportsShouldProcess = $false,ConfirmImpact = 'Low')]
  param(
    [Parameter(Mandatory = $false,ValueFromRemainingArguments = $true)]
    [Object[]]$CommandArgs = @()
  )

  begin
  {
  }

  process
  {
    try
    {
      return [int](Invoke-Neo4jUtility -Command 'admintool' -CommandArgs $CommandArgs -ErrorAction 'Stop')
    }
    catch {
      Write-Error $_
      return 1
    }
  }

  end
  {
  }
}
