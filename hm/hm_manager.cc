#include <cstdint>
#include <fcntl.h>
#include <sys/time.h>

#include "hm/hm_manager.h"
#ifdef METRICS_ON
#include "hm/statistics.h"
#endif

namespace leveldb {
    static uint64_t get_now_micros() {
        struct timeval tv;
        gettimeofday(&tv, nullptr);
        return (tv.tv_sec) * 1000000 + tv.tv_usec;
    }

    static size_t random_number(size_t size) {
        return rand() % size;
    }

    HMManager::HMManager(const Comparator *icmp, std::string smr_disk)
            : icmp_(icmp),
              smr_filename_(std::move(smr_disk)) {

        set_all_zonenum_and_first_zonenum(&zonenum_, &first_zonenum_);

        bitmap_ = new BitMap(zonenum_);

        init_log_file();
        MyLog("\n  !!geardb!!  \n");
        MyLog("COM_WINDOW_SEQ:%d Verify_Table:%d Read_Whole_Table:%d Find_Table_Old:%d\n", CompactionWindowSeq,
              Verify_Table, ReadWholeTable, FindTableOld);
        MyLog("the first_zonenum_:%d zone_num:%ld\n", first_zonenum_, zonenum_);
        //////statistics
        delete_zone_num = 0;
        all_table_size = 0;
        kv_store_sector = 0;
        kv_read_sector = 0;
        max_zone_num = 0;
        move_file_size = 0;
        read_time = 0;
        write_time = 0;
        //////end
    }

    HMManager::~HMManager() {
        get_all_info();

        std::map<uint64_t, Ldbfile *>::iterator it = table_map_.begin();
        while (it != table_map_.end()) {
            delete it->second;
            it = table_map_.erase(it);
        }
        table_map_.clear();
        int i;
        for (i = 0; i < config::kNumLevels; i++) {
            std::vector<Zonefile *>::iterator iz = zone_info_[i].begin();
            while (iz != zone_info_[i].end()) {
                delete (*iz);
                iz = zone_info_[i].erase(iz);
            }
            zone_info_[i].clear();
        }

        if (bitmap_) {
            delete bitmap_;
        }

    }

    void HMManager::set_all_zonenum_and_first_zonenum(uint64_t *zonenum, uint64_t *first_zonenum) {
        *zonenum = 55356;
        *first_zonenum = 524;
        //To be improved
    }

    Zonefile *HMManager::hm_alloc_zone() {
        uint64_t i;
        for (i = first_zonenum_; i < zonenum_; i++) {  //Traverse from the first sequential write zone
            if (bitmap_->get(i) == 0) {
                char filenamebuf[100];
                snprintf(filenamebuf, sizeof(filenamebuf), "%s/%d", smr_filename_.c_str(), i);
                //int fd = open(filenamebuf, O_CREAT | O_RDWR | O_TRUNC, 0666);
                int fd = open(filenamebuf, O_RDWR | O_DIRECT | O_TRUNC);  //need O_TRUNC to set write_pointer = 0
                if (fd == -1) {
                    MyLog("error:open failed! path:%s\n", filenamebuf);
                    continue;
                }
                Zonefile *zf = new Zonefile(i, fd, 0);
                bitmap_->set(i);

                return zf;
            }
        }

        printf("error:hm_alloc_zone failed!\n");
        return nullptr;
    }

    ssize_t HMManager::hm_free_zone(uint64_t zone) {

        bitmap_->clr(zone);
        //no need to delete the file, when alloc, it can set file size = 0
        return 1;
    }

    void HMManager::hm_alloc(int level, uint64_t size) {
        uint64_t need_size = (size % PhysicalDiskSize) ? (size / PhysicalDiskSize + 1) * PhysicalDiskSize : size;
        if (zone_info_[level].empty() || ZONESIZE - zone_info_[level].back()->write_pointer() < need_size) {
            Zonefile *zf = hm_alloc_zone();
            zone_info_[level].push_back(zf);

            if (get_zone_num() > max_zone_num) {
                max_zone_num = get_zone_num();
            }

            return;
        }
    }

    Zonefile *HMManager::get_zone(uint64_t zone_id, int level) {
        for (auto zone_file : zone_info_[level]) {
            if (zone_file->zone() == zone_id) {
                return zone_file;
            }
        }
        return nullptr;
    }

    ssize_t HMManager::hm_write(int level, uint64_t filenum, const void *buf, uint64_t count) {
        hm_alloc(level, count);
        void *w_buf = nullptr;
        Zonefile *zf = zone_info_[level].back();
        uint64_t write_size;
        uint64_t write_ofst = zf->write_pointer();
        ssize_t ret;

        uint64_t write_time_begin = get_now_micros();
        if (count % PhysicalDiskSize == 0) {
            write_size = count;
            ret = pwrite(zf->fd(), buf, write_size, write_ofst);
        } else {
            write_size = (count / PhysicalDiskSize + 1) * PhysicalDiskSize;  //Align with physical block
            //w_buf=(void *)malloc(sector_count*512);
            ret = posix_memalign(&w_buf, MemAlignSize, write_size);
            if (ret != 0) {
                printf("error:%ld posix_memalign falid!\n", ret);
                return -1;
            }
            memset(w_buf, 0, write_size);
            memcpy(w_buf, buf, count);
            ret = pwrite(zf->fd(), w_buf, write_size, write_ofst);
            free(w_buf);
        }
        if (ret != write_size) {
            printf("error:%ld hm_write falid! table:%ld write_size:%ld\n", ret, filenum, write_size);
            return -1;
        }
        uint64_t write_time_end = get_now_micros();
        write_time += (write_time_end - write_time_begin);
#ifdef METRICS_ON
        global_metrics().AddSize(ACTUAL_WRITE, ret);
        global_metrics().AddTime(WRITE_DISK, write_time_end - write_time_begin);
#endif
        zf->forward_write_pointer(write_size);
        Ldbfile *ldb = new Ldbfile(filenum, zf->zone(), write_ofst, count, level);
        table_map_[filenum] = ldb;
        zf->add_table(ldb);
        all_table_size += ldb->size;
        kv_store_sector += write_size;

        MyLog("write table:%ld to level-%d zone:%ld of size:%ld bytes ofst:%ld sect:%ld next:%ld\n",
              filenum, level, zf->zone(), count, write_ofst, write_size, write_ofst + write_size);

        return write_size;
    }

    ssize_t HMManager::hm_read(uint64_t filenum, void *buf, uint64_t count, uint64_t offset, ReadType type) {
        void *r_buf = nullptr;
        uint64_t read_size;
        uint64_t read_ofst;
        uint64_t de_ofst;
        ssize_t ret;
        uint64_t read_time_begin = get_now_micros();

        if (table_map_.find(filenum) == table_map_.end()) {
            printf(" table_map_ can't find table:%ld!\n", filenum);
            return -1;
        }
        Zonefile *zf = get_zone(table_map_[filenum]->zone, table_map_[filenum]->level);
        if (zf == nullptr) {
            printf(" get_zone can't find zone:%ld level:%d! \n", table_map_[filenum]->zone, table_map_[filenum]->level);
            return -1;
        }
        read_ofst =
                table_map_[filenum]->offset +
                (offset / LogicalDiskSize) * LogicalDiskSize;    //offset Align with logical block
        de_ofst = offset % LogicalDiskSize;

        read_size = ((count + de_ofst) % LogicalDiskSize) ? ((count + de_ofst) / LogicalDiskSize + 1) * LogicalDiskSize
                                                          : (count + de_ofst);   //size Align with logical block

        //r_buf=(void *)malloc(sector_count*512);
        ret = posix_memalign(&r_buf, MemAlignSize, read_size);
        if (ret != 0) {
            printf("error:%ld posix_memalign falid!\n", ret);
            return -1;
        }
        memset(r_buf, 0, read_size);
        ret = pread(zf->fd(), r_buf, read_size, read_ofst);
        memcpy(buf, ((char *) r_buf) + de_ofst, count);
        free(r_buf);
        if (ret != read_size) {
            printf("error:%ld hm_read falid!\n", ret);
            return -1;
        }

        uint64_t read_time_end = get_now_micros();
        read_time += (read_time_end - read_time_begin);
        kv_read_sector += read_size;
#ifdef METRICS_ON
        if(type = GET_READ){
            global_metrics().AddTime(READ_DISK, read_time_end - read_time_begin);
            global_metrics().RecordFile(ZONE_ACCESS, read_time_begin, zf->zone());
        }
#endif
        return count;
    }

    ssize_t HMManager::hm_delete(uint64_t filenum) {
        if (table_map_.find(filenum) != table_map_.end()) {
            Ldbfile *ldb = table_map_[filenum];
            // remove from table map
            table_map_.erase(filenum);
            int level = ldb->level;
            uint64_t zone_id = ldb->zone;
            std::vector<Zonefile *>::iterator iz = zone_info_[level].begin();
            for (; iz != zone_info_[level].end(); iz++) {
                if (zone_id == (*iz)->zone()) {
                    // remove table from zone file
                    (*iz)->delete_table(ldb);
                    if ((*iz)->ldb().empty() && (*iz)->write_pointer() > 128ul * 1024 * 1024) {
                        //write_size > 128MB,can free it
                        Zonefile *zf = (*iz);
                        zone_info_[level].erase(iz);
                        if (is_com_window(level, zone_id)) {
                            std::vector<Zonefile *>::iterator ic = com_window_[level].begin();
                            for (; ic != com_window_[level].end();
                                   ic++) {
                                if ((*ic)->zone() == zone_id) {
                                    // remove zonefile from compaction window
                                    com_window_[level].erase(ic);
                                    break;
                                }
                            }
                        }
                        close(zf->fd());
                        delete zf;
                        hm_free_zone(zone_id);
                        MyLog("delete zone:%ld from level-%d\n", zone_id, level);
                        delete_zone_num++;

                    }
                    break;
                }
            }
            MyLog("delete table:%ld from level-%d zone:%ld of size:%ld MB\n", filenum, level, zone_id,
                  ldb->size / 1048576);
            all_table_size -= ldb->size;
            delete ldb;
        }
        return 1;
    }

    ssize_t HMManager::move_file(uint64_t filenum, int to_level) {
        void *r_buf = nullptr;
        ssize_t ret;
        if (table_map_.find(filenum) == table_map_.end()) {
            printf("error:move file failed! no find file:%ld\n", filenum);
            return -1;
        }
        Zonefile *zf = get_zone(table_map_[filenum]->zone, table_map_[filenum]->level);
        if (zf == nullptr) {
            printf(" get_zone can't find zone:%ld level:%d! \n", table_map_[filenum]->zone, table_map_[filenum]->level);
            return -1;
        }
        Ldbfile *ldb = table_map_[filenum];
        uint64_t move_size = ((ldb->size + PhysicalDiskSize - 1) / PhysicalDiskSize) *
                             PhysicalDiskSize;  //we write it ,can move directly, need not Align with logical block
        uint64_t read_ofst = ldb->offset;
        uint64_t file_size = ldb->size;
        int old_level = ldb->level;

        uint64_t read_time_begin = get_now_micros();
        //r_buf=(void *)malloc(sector_count*512);
        ret = posix_memalign(&r_buf, MemAlignSize, move_size);
        if (ret != 0) {
            printf("error:%ld posix_memalign falid!\n", ret);
            return -1;
        }
        memset(r_buf, 0, move_size);
        ret = pread(zf->fd(), r_buf, move_size, read_ofst);
        if (ret != move_size) {
            printf("error:%ld move pread falid!\n", ret);
            return -1;
        }
        uint64_t read_time_end = get_now_micros();
        read_time += (read_time_end - read_time_begin);

        hm_delete(filenum);
        kv_read_sector += move_size;

        uint64_t write_time_begin = get_now_micros();

        hm_alloc(to_level, move_size);
        Zonefile *wzf = zone_info_[to_level][zone_info_[to_level].size() - 1];
        uint64_t write_ofst = wzf->write_pointer();
        ret = pwrite(wzf->fd(), r_buf, move_size, write_ofst);
        if (ret != move_size) {
            printf("error:%ld move pwrite falid!\n", ret);
            return -1;
        }
        uint64_t write_time_end = get_now_micros();
        write_time += (write_time_end - write_time_begin);

        wzf->forward_write_pointer(move_size);
        ldb = new Ldbfile(filenum, wzf->zone(), write_ofst, file_size, to_level);
        table_map_[filenum] = ldb;
        wzf->add_table(ldb);

        free(r_buf);
        kv_store_sector += move_size;
        move_file_size += file_size;
        all_table_size += file_size;

        MyLog("move table:%ld from level-%d to level-%d zone:%ld of size:%ld MB\n", filenum, old_level, to_level,
              wzf->zone(), file_size / 1048576);
        return 1;
    }

    Ldbfile *HMManager::get_one_table(uint64_t filenum) {
        if (table_map_.find(filenum) == table_map_.end()) {
            printf("error:no find file:%ld\n", filenum);
            return nullptr;
        }
        return table_map_[filenum];
    }

    void HMManager::get_zone_table(uint64_t filenum, std::vector<Ldbfile *> **zone_table) {
        if (table_map_.find(filenum) == table_map_.end()) {
            printf("error:no find file:%ld\n", filenum);
            return;
        }

        int level = table_map_[filenum]->level;
        uint64_t zone_id = table_map_[filenum]->zone;
        std::vector<Zonefile *>::iterator iz;
        for (iz = zone_info_[level].begin(); iz != zone_info_[level].end(); iz++) {
            if ((*iz)->zone() == zone_id) {
                *zone_table = &((*iz)->ldb());
                return;
            }
        }

    }

    bool HMManager::trivial_zone_size_move(uint64_t filenum) {
        if (table_map_.find(filenum) == table_map_.end()) {
            printf("error:no find file:%ld\n", filenum);
            return false;
        }

        Zonefile *zf = get_zone(table_map_[filenum]->zone, table_map_[filenum]->level);
        if (zf == nullptr) {
            printf(" get_zone can't find zone:%ld level:%d! \n", table_map_[filenum]->zone, table_map_[filenum]->level);
            return -1;
        }
        if (zf->write_pointer() > 192ul * 1024 * 1024) { //The remaining free space is less than 64MB, triggering
            return true;
        } else return false;
    }

    void HMManager::move_zone(uint64_t filenum) {
        if (table_map_.find(filenum) == table_map_.end()) {
            printf("error:no find file:%ld\n", filenum);
            return;
        }

        int level = table_map_[filenum]->level;
        uint64_t zone_id = table_map_[filenum]->zone;
        std::vector<Zonefile *>::iterator iz;
        Zonefile *zf;

        if (is_com_window(level, zone_id)) {
            std::vector<Zonefile *>::iterator ic = com_window_[level].begin();
            for (; ic != com_window_[level].end(); ic++) {
                if ((*ic)->zone() == zone_id) {
                    com_window_[level].erase(ic);
                    break;
                }
            }
        }
        for (iz = zone_info_[level].begin(); iz != zone_info_[level].end(); iz++) {
            if ((*iz)->zone() == zone_id) {
                zf = (*iz);
                zone_info_[level].erase(iz);
                break;
            }
        }
        MyLog("before move zone:[");
        for (int i = 0; i < zone_info_[level + 1].size(); i++) {
            MyLog("%ld ", zone_info_[level + 1][i]->zone());
        }
        MyLog("]\n");

        int size = zone_info_[level + 1].size();
        if (size == 0) size = 1;
        zone_info_[level + 1].insert(zone_info_[level + 1].begin() + (size - 1), zf);

        for (int i = 0; i < zf->ldb().size(); i++) {
            zf->ldb()[i]->level = level + 1;
        }

        MyLog("move zone:%d table:[", zone_id);
        for (int i = 0; i < zf->ldb().size(); i++) {
            MyLog("%ld ", zf->ldb()[i]->table);
        }
        MyLog("] to level:%d\n", level + 1);

        MyLog("end move zone:[");
        for (int i = 0; i < zone_info_[level + 1].size(); i++) {
            MyLog("%ld ", zone_info_[level + 1][i]->zone());
        }
        MyLog("]\n");
    }


    void HMManager::update_com_window(int level) {
        ssize_t window_num = adjust_com_window_num(level);
        if (CompactionWindowSeq) {
            set_com_window_seq(level, window_num);
        } else {
            set_com_window(level, window_num);
        }

    }

    ssize_t HMManager::adjust_com_window_num(int level) {
        ssize_t window_num = 0;
        switch (level) {
            case 0:
            case 1:
            case 2:
                window_num = zone_info_[level].size();   //1,2 level's compaction window number is all the level
                break;
            case 3:
            case 4:
            case 5:
            case 6:
            case 7:
                window_num = zone_info_[level].size() /
                             CompactionWindowSize; //other level compaction window number is 1/COM_WINDOW_SCALE
                break;
            default:
                break;
        }
        return window_num;
    }

    void HMManager::set_com_window(int level, int num) {
        int i;
        if (level == 1 || level == 2) {
            com_window_[level].clear();
            for (i = 0; i < zone_info_[level].size(); i++) {
                com_window_[level].push_back(zone_info_[level][i]);
            }
            return;
        }
        if (com_window_[level].size() >= num) {
            return;
        }
        size_t ran_num;
        for (i = com_window_[level].size(); i < num; i++) {
            while (1) {
                ran_num = random_number(zone_info_[level].size() - 1);
                if (!is_com_window(level, zone_info_[level][ran_num]->zone())) {
                    break;
                }
            }
            com_window_[level].push_back(zone_info_[level][ran_num]);
        }
    }

    void HMManager::set_com_window_seq(int level, int num) {
        int i;
        if (level == 1 || level == 2) {
            com_window_[level].clear();
            for (i = 0; i < zone_info_[level].size(); i++) {
                com_window_[level].push_back(zone_info_[level][i]);
            }
            return;
        }
        if (com_window_[level].size() >= num) {
            return;
        }
        com_window_[level].clear();
        for (i = 0; i < num; i++) {
            com_window_[level].push_back(zone_info_[level][i]);
        }

    }

    bool HMManager::is_com_window(int level, uint64_t zone) {
        std::vector<Zonefile *>::iterator it;
        for (it = com_window_[level].begin(); it != com_window_[level].end(); it++) {
            if ((*it)->zone() == zone) {
                return true;
            }
        }
        return false;
    }

    void HMManager::get_com_window_table(int level, std::vector<Ldbfile *> *window_table) {
        std::vector<Zonefile *>::iterator iz;
        std::vector<Ldbfile *>::iterator it;
        for (iz = com_window_[level].begin(); iz != com_window_[level].end(); iz++) {
            for (it = (*iz)->ldb().begin(); it != (*iz)->ldb().end(); it++) {
                window_table->push_back((*it));
            }
        }

    }


    //////statistics
    uint64_t HMManager::get_zone_num() {
        uint64_t num = 0;
        int i;
        for (i = 0; i < config::kNumLevels; i++) {
            num += zone_info_[i].size();
        }
        return num;
    }

    void HMManager::get_one_level(int level, uint64_t *table_num, uint64_t *table_size) {
        uint64_t num = 0;
        uint64_t size = 0;
        for (auto zone_file : zone_info_[level]) {
            num += zone_file->ldb().size();
            size += zone_file->get_all_file_size();
        }
        *table_num = num;
        *table_size = size;
    }

    void HMManager::get_per_level_info() {
        int i;
        uint64_t table_num = 0;
        uint64_t table_size = 0;
        float percent = 0;
        int zone_num = 0;
        Zonefile *zf;

        for (i = 0; i < config::kNumLevels; i++) {
            get_one_level(i, &table_num, &table_size);
            if (table_size == 0) {
                percent = 0;
            } else {
                zone_num = zone_info_[i].size();
                zf = zone_info_[i][zone_num - 1];
                percent = 100.0 * table_size / ((zone_num - 1) * 256.0 * 1024 * 1024 + zf->write_pointer());
            }
            MyLog("Level-%d zone_num:%d table_num:%ld table_size:%ld MB percent:%.2f %%\n", i, zone_info_[i].size(),
                  table_num, table_size / 1048576, percent);
        }
    }

    void HMManager::get_valid_info() {
        MyLog("write_zone:%ld delete_zone_num:%ld max_zone_num:%ld table_num:%ld table_size:%ld MB\n", get_zone_num(),
              delete_zone_num, max_zone_num, table_map_.size(), all_table_size / 1048576);
        get_per_level_info();
        uint64_t table_num;
        uint64_t table_size;
        uint64_t zone_id;
        float percent;
        int i;
        for (i = 0; i < config::kNumLevels; i++) {
            if (zone_info_[i].size() != 0) {
                for (auto zone_file : zone_info_[i]) {
                    zone_id = zone_file->zone();
                    table_num = zone_file->ldb().size();
                    table_size = zone_file->get_all_file_size();
                    percent = 100.0 * table_size / (256.0 * 1024 * 1024);
                    MyLog("Level-%d zone_id:%ld table_num:%ld valid_size:%ld MB percent:%.2f %% \n",
                          i, zone_id, table_num, table_size / 1048576, percent);
                }
            }
        }
    }

    void HMManager::get_all_info() {
        uint64_t disk_size = (get_zone_num()) * ZONESIZE;

        MyLog("\nget all data!\n");
        MyLog("table_all_size:%ld MB kv_read_sector:%ld MB kv_store_sector:%ld MB disk_size:%ld MB \n",
              all_table_size / (1024 * 1024), \
            kv_read_sector / 1048576, kv_store_sector / 1048576, disk_size / 1048576);
        MyLog("read_time:%.1f s write_time:%.1f s read:%.1f MB/s write:%.1f MB/s\n", 1.0 * read_time * 1e-6,
              1.0 * write_time * 1e-6, \
            (kv_read_sector / 1048576.0) / (read_time * 1e-6), (kv_store_sector / 1048576.0) / (write_time * 1e-6));
        get_valid_info();
        MyLog("\n");

    }

    void HMManager::get_my_info(int num) {
        MyLog6("\nnum:%d table_size:%ld MB kv_read_sector:%ld MB kv_store_sector:%ld MB zone_num:%ld max_zone_num:%ld move_size:%ld MB\n",
               num, all_table_size / (1024 * 1024), \
            kv_read_sector / 1048576, kv_store_sector / 1048576, get_zone_num(), max_zone_num,
               move_file_size / (1024 * 1024));
        MyLog6("read_time:%.1f s write_time:%.1f s read:%.1f MB/s write:%.1f MB/s\n", 1.0 * read_time * 1e-6,
               1.0 * write_time * 1e-6, \
            (kv_read_sector / 1048576.0) / (read_time * 1e-6), (kv_store_sector / 1048576.0) / (write_time * 1e-6));
        get_valid_all_data(num);
    }

    void HMManager::get_valid_all_data(int num) {
        uint64_t disk_size = (get_zone_num()) * ZONESIZE;

        MyLog3("\nnum:%d\n", num);
        MyLog3("table_all_size:%ld MB kv_read_sector:%ld MB kv_store_sector:%ld MB disk_size:%ld MB \n",
               all_table_size / (1024 * 1024), \
            kv_read_sector / 1048576, kv_store_sector / 1048576, disk_size / 1048576);
        MyLog3("read_time:%.1f s write_time:%.1f s read:%.1f MB/s write:%.1f MB/s\n", 1.0 * read_time * 1e-6,
               1.0 * write_time * 1e-6, \
            (kv_read_sector / 1048576.0) / (read_time * 1e-6), (kv_store_sector / 1048576.0) / (write_time * 1e-6));
        MyLog3("write_zone:%ld delete_zone_num:%ld max_zone_num:%ld table_num:%ld table_size:%ld MB\n", get_zone_num(),
               delete_zone_num, max_zone_num, table_map_.size(), all_table_size / 1048576);
        uint64_t table_num;
        uint64_t table_size;
        int zone_num = 0;
        uint64_t zone_id;
        Zonefile *zf;
        float percent;
        int i;
        for (i = 0; i < config::kNumLevels; i++) {
            get_one_level(i, &table_num, &table_size);
            if (table_size == 0) {
                percent = 0;
            } else {
                zone_num = zone_info_[i].size();
                zf = zone_info_[i][zone_num - 1];
                percent = 100.0 * table_size / ((zone_num - 1) * 256.0 * 1024 * 1024 + zf->write_pointer());
            }
            MyLog3("Level-%d zone_num:%d table_num:%ld table_size:%ld MB percent:%.2f %% \n", i, zone_info_[i].size(),
                   table_num, table_size / 1048576, percent);
        }
        MyLog3("level,zone_id,table_num,valid_size(MB),percent(%%)\n");
        for (i = 0; i < config::kNumLevels; i++) {
            if (zone_info_[i].size() != 0) {
                for (auto zone_file : zone_info_[i]) {
                    zone_id = zone_file->zone();
                    table_num = zone_file->ldb().size();
                    table_size = zone_file->get_all_file_size();
                    percent = 100.0 * table_size / (256.0 * 1024 * 1024);
                    MyLog3("%d,%ld,%ld,%ld,%.2f\n", i, zone_id, table_num, table_size / 1048576, percent);
                }
            }
        }
    }




    //////end









}