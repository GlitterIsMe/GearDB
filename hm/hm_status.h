#ifndef LEVELDB_HM_STATUS_H
#define LEVELDB_HM_STATUS_H

//////
//Module function: Some variables and structures
//////
#include <unistd.h>
#include <vector>

#define Verify_Table 1        //To confirm whether the SSTable is useful, every time an SSTable is written to the disk, \
                             //it will read the handle of the file and add it to the leveldb's table cache. This is the leveldb's own mechanism;
// 1 means that there is this mechanism; 0 means no such mechanism.

const unsigned int PhysicalDiskSize = 4096;  //Disk physical block size, write operation may align with it; get it maybe can accord to the environment in some way
const unsigned int LogicalDiskSize = 4096; //Disk logical block size, read operation may align with it; get it maybe can accord to the environment in some way
const unsigned int CompactionWindowSize = 4;  //The proportion of Compaction window to the total number of zone numbers in the level
const unsigned int SetCompactionWindow = 4;  //The level's data reaches the level threshold * 1/HAVE_WINDOW_SCALE ,then have compaction window
const bool CompactionWindowSeq = true;  //0 means the compaction window selects zone random; 1 means the compaction window selects zone compaction
const long MemAlignSize = sysconf(
        _SC_PAGESIZE);  //The size of the alignment when applying for memory using posix_memalign
const bool ReadWholeTable = true;   //1 means that the entire file is read together when reading the SSTable; 0 means that a block is read during the read operation.
const bool FindTableOld = true; //1 means that when finding key-value, it is the same as the original, a block is read; 0 means that the search key-value is the same as the Read_Whole_Table parameter.



namespace leveldb {

    static const char smr_filename[] = "/mnt/seqwrite";   //e.g. smr_filename[]="/mnt/seqwrite"; mount smr seqwrite dir

    struct Ldbfile {     //file = SSTable ,file Metadata struct
        uint64_t table;  //file name = fiel serial number
        uint64_t zone;   //file's zone number
        uint64_t offset; //file's offset in the zone
        uint64_t size;   //file's size
        int level;  //file in the level number

        Ldbfile(uint64_t a, uint64_t b, uint64_t c, uint64_t d, int e) : table(a), zone(b), offset(c), size(d),
                                                                         level(e) {};

        ~Ldbfile() {};
    };

    struct Zonefile {    //zone struct
        uint64_t zone; //zone num 
        int fd;    //zone open file's fd  
        uint64_t write_pointer; //zone's write_pointer, also offset

        std::vector<struct Ldbfile *> ldb; //SSTable pointers
        Zonefile(uint64_t a, int b, uint64_t c) : zone(a), fd(b), write_pointer(c) {};

        ~Zonefile() {};

        void add_table(struct Ldbfile *file) {
            ldb.push_back(file);
        }

        void delete_table(struct Ldbfile *file) {
            std::vector<struct Ldbfile *>::iterator it;
            for (it = ldb.begin(); it != ldb.end();) {
                if ((*it) == file) {
                    ldb.erase(it);
                    return;
                } else it++;
            }
        }

        uint64_t get_all_file_size() {
            uint64_t size = 0;
            std::vector<struct Ldbfile *>::iterator it;
            for (it = ldb.begin(); it != ldb.end(); it++) {
                size += (*it)->size;
            }
            return size;
        }
    };


}


#endif