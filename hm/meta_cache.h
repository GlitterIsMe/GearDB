//
// Created by Zhangyiwen on 2020/7/28.
//

#ifndef GEARDB_META_CACHE_H
#define GEARDB_META_CACHE_H

#include <unordered_map>
#include <iostream>
#include <string>
#include "include/leveldb/slice.h"

namespace leveldb{
    class MetaCache {
    public:
        MetaCache();
        ~MetaCache();

        void Add(uint64_t fname, const Slice meta_data);
        void Evict(uint64_t fname);
        void Get(uint64_t fname, Slice& meta_data);
        uint64_t ApproximateMemoryUsage() const;

    private:
        std::unordered_map<uint64_t, Slice> cache_;
        uint64_t cached_size_;
    };
}

#endif //GEARDB_META_CACHE_H
