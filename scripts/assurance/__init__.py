"""Commit-bound assurance execution helpers.

The package is deliberately outside the application import graph. Assurance
execution must not initialize runtime services merely to discover or report a
proof result.
"""
