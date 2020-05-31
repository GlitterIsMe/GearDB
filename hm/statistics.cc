//
// Created by 張藝文 on 2020/5/30.
//

#include "statistics.h"

namespace leveldb {
    Metrics::Metrics() {
        delete_zone_num = 0;
        all_table_size = 0;
        kv_store_sector = 0;
        kv_read_sector = 0;
        max_zone_num = 0;
        move_file_size = 0;
        read_time = 0;
        write_time = 0;

        // metrics for write
        time_log_write = 0;
        time_gc = 0;
        size_disk_write = 0;
        size_user_write = 0;

        // metrics for compaction
        time_compaction = 0;
        size_comapction = 0;
        per_compaction_io.open("compaction_log.csv", std::ios::out | std::ios::trunc);
        per_compaction_io << "compaction_time(micros), write_data_size(MB)\n";

        // metrics for read
        time_locating_sstable = 0;
        time_read_disk = 0;
        zone_access_file.open("zone_access_log.csv", std::ios::out | std::ios::trunc);
        zone_access_file << "access_time, zone_num\n";
        //metrics for gc
        time_gc = 0;
        size_gc_write = 0;
    }

    Metrics::~Metrics() {
        per_compaction_io.close();
        zone_access_file.close();
    }

    void Metrics::AddTime(leveldb::TimeMetricsType metrics_type, uint64_t time) {
        switch (metrics_type) {
            case LOG_WRITE :
                time_log_write += time;
                break;
            case GC:
                time_gc += time;
                break;
            case WRITE_DISK:
                time_disk_write += time;
                break;
            case COMPACTION:
                time_compaction += time;
                break;
            case LOCATING:
                time_locating_sstable += time;
                break;
            case READ_DISK:
                time_read_disk += time;
                break;
            default:
                break;

        }
    }

    void Metrics::AddSize(leveldb::SizeMetricsType metrics_type, uint64_t size) {
        switch (metrics_type) {
            case ACTUAL_WRITE:
                size_disk_write += size;
                break;
            case USER_WRITE:
                size_user_write += size;
                break;
            case GC_WRITE:
                size_gc_write += size;
                break;
            case COMPACTION_WRITE:
                size_comapction += size;
                break;
            default:
                break;

        }
    }

    void Metrics::RecordFile(leveldb::WriteFileMetricsType metrics_type, uint64_t arg1, uint64_t arg2) {
        switch (metrics_type) {
            case PER_COMPACTION:
                per_compaction_io << arg1 << ", " << arg2 << "\n";
                break;
            case ZONE_ACCESS:
                zone_access_file << arg1 << ", " << arg2 << "\n";
                break;
            default:
                break;
        }
    }

    void Metrics::Persist() {
        std::ofstream output;
        output.open("metrics.csv", std::ios::out | std::ios::trunc);
        output << "total_log_write_time, " << time_log_write << ",\n"
               << "total_disk_write_time, " << time_disk_write << ",\n"
               << "total_compaction_time, " << time_compaction << ",\n"
               << "total_gc_time, " << time_gc << ",\n"
               << "total_user_write(GB), " << size_user_write / 1024.0 / 1024 / 1024 << ",\n"
               << "total_disk_write(GB), " << size_disk_write / 1024.0 / 1024 / 1024 << ",\n"
               << "total_gc_write(GB), " << size_comapction / 1024.0 / 1024 / 1024 << ",\n"
               << "total_locating_time, " << time_locating_sstable << ",\n"
               << "total_read_disk_time, " << time_read_disk << ",\n"
               << "total_gc_time, " << time_gc << ",\n";
        output.close();
    }

    Metrics &global_metrics() {
        static Metrics metrics;
        return metrics;
    }
}
