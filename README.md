Flysystem-SeaweedFS
===================

A **very experimental** Flysystem adapter for SeaweedFS.

This does ***NOT*** use the SeaweedFS Filer, instead it maintains file mappings internally for paths. This is useful for internal storage purposes, and not externally linking files.

Mapping
-------

The `SeaweedFS\Filesystem\Mapping\Mapper` interface can be extended for Databases, Filesystems, etc. It is used to link paths to the file id and metadata.

Mappings should contain the following data:

* fid (volume,file cookie)
* mimeType (mime type, text/plain, etc)
* size (File size as reported by SeaweedFS when storing)

Example
-------

```php
<?php
require_ocne 'vendor/autoload.php';

$cache = new SeaweedFS\Cache\FileCache('./cache');
$client = new SeaweedFS\SeaweedFS('127.0.0.1:9333', $cache);

$adapter = new SeaweedFS\Filesystem\Seaweed($client, new SeaweedFS\Filesystem\Mapping\CacheMapper($cache));

$filesystem = new League\Flysystem\Filesystem($adapter);

$filesystem->put('test.txt', 'test1234');

echo $filesystem->read('test.txt');
```