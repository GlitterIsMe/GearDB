//
// Created by 張藝文 on 2020/5/28.
//

#ifndef GEARDB_ENV_HM_H
#define GEARDB_ENV_HM_H

#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/resource.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <sys/types.h>
#include <time.h>
#include <unistd.h>
#include <deque>
#include <limits>
#include <set>
#include "leveldb/env.h"
#include "leveldb/slice.h"
#include "port/port.h"
#include "util/logging.h"
#include "util/mutexlock.h"
#include "util/posix_logger.h"
#include "util/env_posix_test_helper.h"
#include "leveldb/options.h"

#include "include/leveldb/env.h"
#include "hm/hm_manager.h"

namespace hm {
    using leveldb::Status;

// Set of locked files.  We keep a separate set instead of just
// relying on fcntrl(F_SETLK) since fcntl(F_SETLK) does not provide
// any protection against multiple uses from the same process.
    class PosixLockTable {
    private:
        leveldb::port::Mutex mu_;
        std::set <std::string> locked_files_;
    public:
        bool Insert(const std::string &fname);

        void Remove(const std::string &fname);
    };

// Helper class to limit resource usage to avoid exhaustion.
// Currently used to limit read-only file descriptors and mmap file usage
// so that we do not end up running out of file descriptors, virtual memory,
// or running into kernel performance problems for very large databases.
    class Limiter {
    public:
        // Limit maximum number of resources to |n|.
        Limiter(intptr_t n);

        // If another resource is available, acquire it and return true.
        // Else return false.
        bool Acquire();

        // Release a resource acquired by a previous call to Acquire() that returned
        // true.
        void Release();

    private:
        leveldb::port::Mutex mu_;
        leveldb::port::AtomicPointer allowed_;

        intptr_t GetAllowed() const;

        // REQUIRES: mu_ must be held
        void SetAllowed(intptr_t v);

        Limiter(const Limiter &);

        void operator=(const Limiter &);
    };

    class HMEnv : public leveldb::Env {
    public:
        explicit HMEnv(leveldb::HMManager *hm_manager);

        ~HMEnv() override;

        Status NewSequentialFile(const std::string &fname,
                                 leveldb::SequentialFile **result);

        Status NewRandomAccessFile(const std::string &fname,
                                   leveldb::RandomAccessFile **result, int flag = 0, const char *buf_file = nullptr);

        Status NewWritableFile(const std::string &fname,
                               leveldb::WritableFile **result,
                               int level = -1);

        Status NewAppendableFile(const std::string &fname,
                                 leveldb::WritableFile **result);

        bool FileExists(const std::string &fname);

        Status GetChildren(const std::string &dir,
                           std::vector <std::string> *result);

        Status DeleteFile(const std::string &fname);

        Status CreateDir(const std::string &name);

        Status DeleteDir(const std::string &name);

        Status GetFileSize(const std::string &fname, uint64_t *size);

        Status RenameFile(const std::string &src, const std::string &target);

        Status LockFile(const std::string &fname, leveldb::FileLock **lock);

        Status UnlockFile(leveldb::FileLock *lock) override;

        void Schedule(void (*function)(void *), void *arg) override;

        void StartThread(void (*function)(void *arg), void *arg) override;

        Status GetTestDirectory(std::string *result);

        static uint64_t gettid();

        Status NewLogger(const std::string &fname, leveldb::Logger **result);

        uint64_t NowMicros() override;

        void SleepForMicroseconds(int micros) override;

        static Env *Default();

    private:
        void PthreadCall(const char *label, int result);

        // BGThread() is the body of the background thread
        void BGThread();

        static void *BGThreadWrapper(void *arg);

        pthread_mutex_t mu_;
        pthread_cond_t bgsignal_;
        pthread_t bgthread_;
        bool started_bgthread_;

        // Entry per Schedule() call
        struct BGItem {
            void *arg;

            void (*function)(void *);
        };

        typedef std::deque <BGItem> BGQueue;
        BGQueue queue_;

        PosixLockTable locks_;
        Limiter mmap_limit_;
        Limiter fd_limit_;

        leveldb::HMManager *hm_manager_;
    };
}

#endif //GEARDB_ENV_HM_H
