"""Tray client package: the only user-facing entry point of OpenOctopus.

The GUI talks to the Python execution core over the private pipe defined in
:mod:`openoctopus_client.core_channel`.  This package must not import or
initialize MCP, document converters, or exec sessions directly.
"""
