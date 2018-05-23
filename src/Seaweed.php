<?php

namespace SeaweedFS\Filesystem;

use function GuzzleHttp\Psr7\mimetype_from_filename;
use League\Flysystem\Adapter\AbstractAdapter;
use League\Flysystem\Adapter\CanOverwriteFiles;
use League\Flysystem\Adapter\Polyfill\NotSupportingVisibilityTrait;
use League\Flysystem\Config;
use LogicException;
use SeaweedFS\SeaweedFS;
use SeaweedFS\Filesystem\Mapping\Mapper;
use SeaweedFS\SeaweedFSException;

class Seaweed extends AbstractAdapter implements CanOverwriteFiles {
    use NotSupportingVisibilityTrait;

    /**
     * @var SeaweedFS The SeaweedFS client
     */
    private $client;

    /**
     * @var Mapper The filesystem mapper
     */
    private $mapper;

    /**
     * Construct a new Adapter for SeaweedFS with the given client and mapper.
     *
     * @param SeaweedFS $client
     * @param Mapper $mapper
     */
    public function __construct(SeaweedFS $client, Mapper $mapper) {
        $this->client = $client;
        $this->mapper = $mapper;
    }

    /**
     * Write a new file.
     *
     * @param string $path
     * @param string $contents
     * @param Config $config Config object
     *
     * @return array|false false on failure file meta data on success
     */
    public function write($path, $contents, Config $config) {
        try {
            $mapping = $this->mapper->get($path);

            $file = null;

            if ($mapping) {
                $volume = $this->client->lookup($mapping['fid']);

                $file = new \stdClass();
                $file->url = $volume->url;
                $file->fid = $mapping['fid'];
            }

            $file = $this->client->upload($contents, basename($path), $file);

            if (!$file) {
                return false;
            }

            $this->mapper->store($path, $file->fid, mimetype_from_filename($path), $file->size);

            return $file->toArray();
        } catch (SeaweedFSException $e) {
            return false;
        }
    }

    /**
     * Write a new file using a stream.
     *
     * @param string $path
     * @param resource $resource
     * @param Config $config Config object
     *
     * @return array|false false on failure file meta data on success
     */
    public function writeStream($path, $resource, Config $config) {
        return $this->write($path, $resource, $config);
    }

    /**
     * Update a file.
     *
     * @param string $path
     * @param string $contents
     * @param Config $config Config object
     *
     * @return array|false false on failure file meta data on success
     */
    public function update($path, $contents, Config $config) {
        return $this->write($path, $contents, $config);
    }

    /**
     * Update a file using a stream.
     *
     * @param string $path
     * @param resource $resource
     * @param Config $config Config object
     *
     * @return array|false false on failure file meta data on success
     */
    public function updateStream($path, $resource, Config $config) {
        return $this->write($path, $resource, $config);
    }

    /**
     * Rename a file.
     *
     * @param string $path
     * @param string $newpath
     *
     * @return bool
     */
    public function rename($path, $newpath) {
        throw new LogicException(get_class($this) . ' does not support renaming. Path: ' . $path);
    }

    /**
     * Copy a file.
     *
     * @param string $path
     * @param string $newpath
     *
     * @return bool
     */
    public function copy($path, $newpath) {
        throw new LogicException(get_class($this) . ' does not support copying. Path: ' . $path);
    }

    /**
     * Delete a file.
     *
     * @param string $path
     *
     * @return bool
     */
    public function delete($path) {
        $mapping = $this->mapper->get($path);

        if (!$mapping) {
            return false;
        }

        try {
            $this->client->delete($mapping['fid']);
            $this->mapper->remove($path);

            return true;
        } catch (SeaweedFSException $e) {
            return false;
        }
    }

    /**
     * Delete a directory.
     *
     * @param string $dirname
     *
     * @return bool
     */
    public function deleteDir($dirname) {
        throw new LogicException(get_class($this) . ' does not support directory deletion. Path: ' . $dirname);
    }

    /**
     * Create a directory.
     *
     * @param string $dirname directory name
     * @param Config $config
     *
     * @return array|false
     */
    public function createDir($dirname, Config $config) {
        throw new LogicException(get_class($this) . ' does not support directory creation. Path: ' . $dirname);
    }

    /**
     * Check whether a file exists.
     *
     * @param string $path
     *
     * @return array|bool|null
     */
    public function has($path) {
        $mapping = $this->mapper->get($path);

        if (!$mapping) {
            return false;
        }

        return $this->client->has($mapping['fid']);
    }

    /**
     * Read a file.
     *
     * @param string $path
     *
     * @return array|false
     */
    public function read($path) {
        $mapping = $this->mapper->get($path);

        if (!$mapping) {
            return false;
        }

        try {
            $file = $this->client->get($mapping['fid']);

            if (!$file) {
                return false;
            }

            return [
                'contents' => stream_get_contents($file)
            ];
        } catch (SeaweedFSException $e) {
            return false;
        }
    }

    /**
     * Read a file as a stream.
     *
     * @param string $path
     *
     * @return array|false
     */
    public function readStream($path) {
        $mapping = $this->mapper->get($path);

        if (!$mapping) {
            return false;
        }

        try {
            $file = $this->client->get($mapping['fid']);

            if (!$file) {
                return false;
            }

            return [
                'stream' => $file
            ];
        } catch (SeaweedFSException $e) {
            return false;
        }
    }

    /**
     * List contents of a directory.
     *
     * @param string $directory
     * @param bool $recursive
     *
     * @return array
     */
    public function listContents($directory = '', $recursive = false) {
        throw new LogicException(get_class($this) . ' does not support content listing. Path: ' . $directory);
    }

    /**
     * Get all the meta data of a file or directory.
     *
     * @param string $path
     *
     * @return array|false
     */
    public function getMetadata($path) {
        return array_merge($this->mapper->get($path), [
            'type' => 'file'
        ]);
    }

    /**
     * Get the size of a file.
     *
     * @param string $path
     *
     * @return array|false
     */
    public function getSize($path) {
        return $this->getMetadata($path);
    }

    /**
     * Get the mimetype of a file.
     *
     * @param string $path
     *
     * @return array|false
     */
    public function getMimetype($path) {
        return $this->getMetadata($path);
    }

    /**
     * Get the timestamp of a file.
     *
     * @param string $path
     *
     * @return array|false
     */
    public function getTimestamp($path) {
        return $this->getMetadata($path);
    }

    /**
     * Get the public URL of a path.
     *
     * @param $path
     * @return string|bool
     */
    public function getUrl($path) {
        $mapping = $this->mapper->get($path);

        if (!$mapping) {
            return false;
        }

        try {
            $volume = $this->client->lookup($mapping['fid']);

            return $this->client->buildVolumeUrl($volume->getPublicUrl(), $mapping['fid']);
        } catch (SeaweedFSException $e) {
            return false;
        }
    }
}