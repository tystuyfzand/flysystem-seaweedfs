<?php

namespace SeaweedFS\Filesystem\Mapping;

interface Mapper {
    /**
     * Store a path to a seaweedfs file id
     *
     * @param $path
     * @param $fileId
     * @param $mimeType
     * @param $size
     * @return mixed
     */
    public function store($path, $fileId, $mimeType, $size);

    /**
     * Map a path to a seaweedfs file id
     *
     * @param $path
     * @return mixed
     */
    public function get($path);

    /**
     * Remove a path mapping.
     *
     * @param $path
     * @return mixed
     */
    public function remove($path);
}