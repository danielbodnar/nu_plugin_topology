#!/usr/bin/env nu
# topology — Nushell module for the topology plugin
#
# Provides data-source adapters, composable pipeline steps,
# summary reports, and export formatters.
#
# Usage:
#   use topology
#   topology adapters github-stars load | topology adapters github-stars normalize | topology pipeline run
#
# Or individual steps:
#   $data | topology pipeline classify --clusters 10
#   $data | topology report stats
#   $data | topology export to-json ./output/report.json

export module adapters/github-stars.nu
export module adapters/chrome-bookmarks.nu
export module adapters/directory.nu
export module adapters/json.nu
export module pipeline.nu
export module report.nu
export module export.nu
