# Changelog
## [v1.1.3](https://github.com/SmithRx/gonymizer/releases/tag/v1.1.3) 7/25/2019
* Rename --exclude-schemas to --exclude-schema mimic pg_dump

## [v1.1.2](https://github.com/SmithRx/gonymizer/releases/tag/v1.1.2) 7/25/2019
* Handled a memory buffer issue mentioned in #44. While taking a pg_dump memory usage would spike to 6GB+ depending on
system and container type.
* Moved to using --exclude-schema instead of --exclude-table and adding a .* to the end of the schema name. This was 
causing confusion between --exclude-table and --exclude-schema.

## [v1.1.1](https://github.com/SmithRx/gonymizer/releases/tag/v1.1.1) 05/07/2019
* Fixes for #31
* Added in script to increment version
* Other fixes were missed during updates. These need to be backfilled. See git history for specifics.

## [v1.0.0](https://github.com/SmithRx/gonymizer/releases/tag/v1.0.0) 03/05/2019
* Initial release
