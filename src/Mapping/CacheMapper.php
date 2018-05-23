<?php

namespace SeaweedFS\Filesystem\Mapping;

use SeaweedFS\Cache\CacheInterface;

class CacheMapper implements Mapper {

    /**
     * @var CacheInterface
     */
    private $cache;

    /**
     * Construct a new CacheMapper using the given Cache instance.
     *
     * @param $cache
     */
    public function __construct(CacheInterface $cache) {
        $this->cache = $cache;
    }

    /**
     * Store a path to a seaweedfs file id
     *
     * @param $path
     * @param $fileId
     * @param $mimeType
     * @param $size
     * @return mixed
     */
    public function store($path, $fileId, $mimeType, $size) {
        $this->cache->put('seaweedfs.' . md5($path), [
            'fid' => $fileId,
            'mimeType' => $mimeType,
            'size' => $size
        ]);
    }

    /**
     * Map a path to a seaweedfs file id
     *
     * @param $path
     * @return mixed
     */
    public function get($path) {
        return $this->cache->get('seaweedfs.' . md5($path));
    }

    /**
     * Remove a path mapping.
     *
     * @param $path
     * @return mixed
     */
    public function remove($path) {
        $this->cache->remove('seaweedfs.' . md5($path));
    }
}