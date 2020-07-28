//
// Created by 張藝文 on 2020/7/28.
//

#include "hm/meta_cache.h"

namespace leveldb {
    MetaCache::MetaCache() {
        cached_size_ = 0;
        cache_.clear();
    }

    MetaCache::~MetaCache() {
        for (auto meta : cache_) {
            delete[] meta.second.data();
        }
        cache_.clear();
    }

    void MetaCache::Add(uint64_t fname, const Slice meta_data) {
        auto res = cache_.find(fname);
        if (res != cache_.end()) {
            fprintf(stderr, "Meta cache error : Duplicated entry founded!\n");
            exit(-1);
        }
        uint64_t meta_size = meta_data.size();
        char* data = new char[meta_size];
        memcpy(data, meta_data.data(), meta_size);
        cache_[fname] = Slice(data, meta_size);
        cached_size_ += meta_size;
    }

    void MetaCache::Evict(uint64_t fname) {
        auto res = cache_.find(fname);
        if (res == cache_.end()) {
            fprintf(stderr, "Meta cache error : Entry not found!\n");
            exit(-1);
        }
        Slice evicted = cache_[fname];
        cache_.erase(fname);
        delete[] evicted.data();
    }

    uint64_t MetaCache::ApproximateMemoryUsage() const {
        return cached_size_;
    }

    void MetaCache::Get(uint64_t fname, Slice &meta_data) {
        auto res = cache_.find(fname);
        if (res == cache_.end()) {
            fprintf(stderr, "Meta cache error : [Get] Entry not found!\n");
            exit(-1);
        }
        meta_data = cache_[fname];
    }
}