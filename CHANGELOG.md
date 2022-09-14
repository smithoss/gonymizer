# Changelog

## [v2.0.0](https://github.com/smithoss/gonymizer/releases/tag/v2.0.0) 9/04/2020
* Multithreaded processing is now available. Please see the updated --help flag for new cli arguments.

## [v1.2.0](https://github.com/smithoss/gonymizer/releases/tag/v1.2.0) 7/31/2019
* Create the upload command for S3 uploading
  * was a part of the process command but found that separating uploads to its own command allows for a clear separation of duties
* Add S3 multipart upload support by implementing S3manager for all s3 uploads
* Random lint clean up

## [v1.1.4](https://github.com/smithoss/gonymizer/releases/tag/v1.1.3) 7/30/2019
* Rename command directory to cmd
* Fix ENV variables and logging flags at the root command level
  * log flags and ENV variables were broken in previous versions
  
## [v1.1.3](https://github.com/smithoss/gonymizer/releases/tag/v1.1.3) 7/25/2019
* Rename --exclude-schemas to --exclude-schema mimic pg_dump

## [v1.1.2](https://github.com/smithoss/gonymizer/releases/tag/v1.1.2) 7/25/2019
* Handled a memory buffer issue mentioned in #44. While taking a pg_dump memory usage would spike to 6GB+ depending on
system and container type.
* Moved to using --exclude-schema instead of --exclude-table and adding a .* to the end of the schema name. This was 
causing confusion between --exclude-table and --exclude-schema.

## [v1.1.1](https://github.com/smithoss/gonymizer/releases/tag/v1.1.1) 05/07/2019
* Fixes for #31
* Added in script to increment version
* Other fixes were missed during updates. These need to be backfilled. See git history for specifics.

## [v1.0.0](https://github.com/smithoss/gonymizer/releases/tag/v1.0.0) 03/05/2019
* Initial release
