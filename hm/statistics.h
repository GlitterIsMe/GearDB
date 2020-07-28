//
// Created by 張藝文 on 2020/5/30.
//

#ifndef GEARDB_STATISTICS_H
#define GEARDB_STATISTICS_H

#include <iostream>
#include <fstream>

namespace leveldb{
    enum TimeMetricsType{
        LOG_WRITE,
        GC,
        WRITE_DISK,
        COMPACTION,
        LOCATING,
        READ_DISK,
        INDEX_READ,
        FOOTER_READ,
        BLOCK_READ,
        SUCCESS_READ,
        FAILURE_READ,
    };

    enum SizeMetricsType{
        ACTUAL_WRITE,
        USER_WRITE,
        GC_WRITE,
        COMPACTION_WRITE,

    };

    enum WriteFileMetricsType{
        PER_COMPACTION,
        ZONE_ACCESS,
    };

    class Metrics{
    public:
        Metrics();
        ~Metrics();
        void AddTime(TimeMetricsType metrics_type, uint64_t time);
        void AddSize(SizeMetricsType metrics_type, uint64_t size);
        void RecordFile(WriteFileMetricsType metrics_type, uint64_t arg1, uint64_t arg2);
        void Persist(std::string filename);
        void PrintWA();
        void RecordRange(bool record, std::string smallest, std::string largest);
    private:
        uint64_t delete_zone_num;
        uint64_t all_table_size;
        uint64_t kv_store_sector;
        uint64_t kv_read_sector;
        uint64_t max_zone_num;
        uint64_t move_file_size;
        uint64_t read_time;
        uint64_t write_time;

        // metrics for write
        uint64_t time_log_write;
        uint64_t time_disk_write;
        uint64_t size_disk_write;
        uint64_t size_user_write;

        // metrics for compaction
        uint64_t time_compaction;
        uint64_t size_comapction;
        std::ofstream per_compaction_io;

        // metrics for read
        uint64_t time_locating_sstable;
        uint64_t time_read_disk;
        std::ofstream zone_access_file;
        uint64_t time_index_read;
        uint64_t time_block_read;
        uint64_t time_success_read;
        uint64_t time_failure_read;
        uint64_t time_footer_read;

        // metrics for gc
        uint64_t time_gc;
        uint64_t size_gc_write;

        //
        std::ofstream range_record;

    };

    Metrics& global_metrics();
}
#endif //GEARDB_STATISTICS_H
