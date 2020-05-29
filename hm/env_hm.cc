// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
#include "env_hm.h"

namespace hm {

    using leveldb::HMManager;
    using leveldb::Slice;

    static int open_read_only_file_limit = -1;
    static int mmap_limit = -1;

    static const size_t kBufSize = 65536;

    static Status PosixError(const std::string &context, int err_number) {
        if (err_number == ENOENT) {
            return Status::NotFound(context, strerror(err_number));
        } else {
            return Status::IOError(context, strerror(err_number));
        }
    }

    static bool isSSTableName(const std::string &fname) {
        size_t pos = fname.find(".ldb");
        return (pos != std::string::npos);
    }

    static uint64_t Parsefname(std::string fname) {
        int pos = fname.find_last_of('.');
        int pos2 = fname.find_last_of('/');
        int num = pos - pos2 - 1;
        std::string raw = fname.substr(pos2 + 1, num);
        uint64_t result = 0;
        int step = 1, start = raw.size() - 1;
        while (start >= 0) {
            result += (raw[start] - '0') * step;
            start--;
            step *= 10;
        }
        return result;
    }

    static int LockOrUnlock(int fd, bool lock) {
        errno = 0;
        struct flock f;
        memset(&f, 0, sizeof(f));
        f.l_type = (lock ? F_WRLCK : F_UNLCK);
        f.l_whence = SEEK_SET;
        f.l_start = 0;
        f.l_len = 0;        // Lock/unlock entire file
        return fcntl(fd, F_SETLK, &f);
    }

    // Return the maximum number of concurrent mmaps.
    static int MaxMmaps() {
        if (mmap_limit >= 0) {
            return mmap_limit;
        }
        // Up to 1000 mmaps for 64-bit binaries; none for smaller pointer sizes.
        mmap_limit = sizeof(void *) >= 8 ? 1000 : 0;
        return mmap_limit;
    }

    // Return the maximum number of read-only files to keep open.
    static intptr_t MaxOpenFiles() {
        if (open_read_only_file_limit >= 0) {
            return open_read_only_file_limit;
        }
        struct rlimit rlim;
        if (getrlimit(RLIMIT_NOFILE, &rlim)) {
            // getrlimit failed, fallback to hard-coded default.
            open_read_only_file_limit = 50;
        } else if (rlim.rlim_cur == RLIM_INFINITY) {
            open_read_only_file_limit = std::numeric_limits<int>::max();
        } else {
            // Allow use of 20% of available file descriptors for read-only files.
            open_read_only_file_limit = rlim.rlim_cur / 5;
        }
        return open_read_only_file_limit;
    }


    class HMRamdomAccessFile : public leveldb::RandomAccessFile {  //hm read file
    private:
        HMManager *hm_manager_;
        const std::string filename_;
        uint64_t filenum;

    public:
        HMRamdomAccessFile(const std::string &fname, HMManager *hm_manager)
                : filename_(fname), hm_manager_(hm_manager) {

            filenum = Parsefname(fname);
        }

        virtual ~HMRamdomAccessFile() {}

        virtual Status Read(uint64_t offset, size_t n, Slice *result, char *scratch) const {
            Status s;
            ssize_t r = -1;
            r = hm_manager_->hm_read(filenum, scratch, n, offset);
            if (r < 0) {
                s = PosixError(filename_, errno);
                return s;
            }
            *result = Slice(scratch, (r < 0) ? 0 : r);
            return s;
        }
    };

    class HMComRamdomAccessFile
            : public leveldb::RandomAccessFile {  //when compaction,we can read the entire file at once
    private:
        HMManager *hm_manager_;
        const std::string filename_;
        uint64_t filenum;
        struct leveldb::Ldbfile *ldb;
        char *buf_;
        Status st;

    public:
        HMComRamdomAccessFile(const std::string &fname, HMManager *hm_manager, const char *buf_file)
                : filename_(fname), hm_manager_(hm_manager), buf_(nullptr) {
            filenum = Parsefname(fname);
            ldb = hm_manager_->get_one_table(filenum);
            //buf_ = new char[ldb->size];
            int ret = posix_memalign((void **) &buf_, MemAlignSize, ldb->size);
            if (ret != 0) {
                printf("error:%d posix_memalign falid!\n", ret);
            }
            ssize_t r = -1;
            st = Status::OK();
            if (buf_file != NULL) {
                memcpy(buf_, buf_file, ldb->size);
            } else {
                r = hm_manager_->hm_read(filenum, buf_, ldb->size, 0);
                if (r < 0) {
                    st = PosixError(filename_, errno);
                }
            }
        }

        virtual ~HMComRamdomAccessFile() {
            if (buf_ != nullptr) {
                free(buf_);
            }
            //MyLog("free table:%ld\n",filenum);
        }

        virtual Status Read(uint64_t offset, size_t n, Slice *result, char *scratch) const {
            if (st.ok()) {
                *result = Slice(buf_ + offset, n);
                return Status::OK();
            } else {
                return st;
            }
        }
    };

    class HMWritableFile : public leveldb::WritableFile {    //hm write file except L0 level
    private:
        HMManager *hm_manager_;
        std::string fname_;
        int level_;
        char *buf_; //Fixed buffer size
        uint64_t total_size_;

    public:
        HMWritableFile(const std::string &fname, HMManager *hm_manager, int level)
                : fname_(fname), total_size_(0), hm_manager_(hm_manager), level_(level), buf_(nullptr) {
            if (level_ == -1) {
                printf("ldb file have error level!table:%ld\n", Parsefname(fname_));
            }
            //buf_ = new char[Options().max_file_size + 1*1024*1024];
            uint64_t size = (leveldb::Options().max_file_size + 1 * 1024 * 1024);
            int ret = posix_memalign((void **) &buf_, MemAlignSize, size);
            if (ret != 0) {
                printf("error:%d posix_memalign falid!\n", ret);
            }
        }

        ~HMWritableFile() {
            if (buf_ != nullptr) {
                free(buf_);
            }
        }

        virtual Status Append(const Slice &data) {
            memcpy(buf_ + total_size_, data.data(), data.size());
            total_size_ += data.size();
            return Status::OK();
        }

        virtual Status Close() {
            return Status::OK();
        }

        virtual Status Flush() {
            return Status::OK();
        }

        virtual Status Sync() {
            ssize_t ret = hm_manager_->hm_write(level_, Parsefname(fname_), buf_, total_size_);
            if (ret > 0) {
                return Status::OK();
            }
            return Status::IOError("hm_write error");
        }

        virtual Status Setlevel(int level = 0) { return Status::OK(); }

        virtual const char *Getbuf() { return buf_; }

    };

    class HMWritableFileL0 : public leveldb::WritableFile {    //hm write L0 level file
    private:
        HMManager *hm_manager_;
        std::string fname_;
        int level_;
        char *buf_;
        uint64_t total_size_;

    public:
        HMWritableFileL0(const std::string &fname, HMManager *hm_manager, int level)
                : fname_(fname), total_size_(0), hm_manager_(hm_manager), level_(level), buf_(nullptr) {
            if (level_ == -1) {
                printf("ldb file have error level!table:%ld\n", Parsefname(fname_));
            }
            //buf_ = new char[Options().write_buffer_size + 1*1024*1024];
            uint64_t size = (leveldb::Options().write_buffer_size + 1 * 1024 * 1024);
            int ret = posix_memalign((void **) &buf_, MemAlignSize, size);
            if (ret != 0) {
                printf("error:%d posix_memalign falid!\n", ret);
            }
        }

        ~HMWritableFileL0() {
            if (buf_ != nullptr) {
                free(buf_);
            }
        }

        virtual Status Append(const Slice &data) {
            memcpy(buf_ + total_size_, data.data(), data.size());
            total_size_ += data.size();
            return Status::OK();
        }

        virtual Status Close() {
            return Status::OK();
        }

        virtual Status Flush() {
            return Status::OK();
        }

        virtual Status Sync() {
            ssize_t ret = hm_manager_->hm_write(level_, Parsefname(fname_), buf_, total_size_);
            if (ret > 0) {
                return Status::OK();
            }
            return Status::IOError("hm_write error");
        }

        virtual Status Setlevel(int level = 0) {
            level_ = level;
            return Status::OK();
        }

        virtual const char *Getbuf() { return buf_; }

    };

// mmap() based random-access
    class PosixMmapReadableFile : public leveldb::RandomAccessFile {
    private:
        std::string filename_;
        void *mmapped_region_;
        size_t length_;
        Limiter *limiter_;

    public:
        // base[0,length-1] contains the mmapped contents of the file.
        PosixMmapReadableFile(const std::string &fname, void *base, size_t length,
                              Limiter *limiter)
                : filename_(fname), mmapped_region_(base), length_(length),
                  limiter_(limiter) {
        }

        virtual ~PosixMmapReadableFile() {
            munmap(mmapped_region_, length_);
            limiter_->Release();
        }

        virtual Status Read(uint64_t offset, size_t n, Slice *result,
                            char *scratch) const {
            Status s;
            if (offset + n > length_) {
                *result = Slice();
                s = PosixError(filename_, EINVAL);
            } else {
                *result = Slice(reinterpret_cast<char *>(mmapped_region_) + offset, n);
            }
            return s;
        }
    };

// pread() based random-access
    class PosixRandomAccessFile : public leveldb::RandomAccessFile {
    private:
        std::string filename_;
        bool temporary_fd_;  // If true, fd_ is -1 and we open on every read.
        int fd_;
        Limiter *limiter_;

    public:
        PosixRandomAccessFile(const std::string &fname, int fd, Limiter *limiter)
                : filename_(fname), fd_(fd), limiter_(limiter) {
            temporary_fd_ = !limiter->Acquire();
            if (temporary_fd_) {
                // Open file on every access.
                close(fd_);
                fd_ = -1;
            }
        }

        virtual ~PosixRandomAccessFile() {
            if (!temporary_fd_) {
                close(fd_);
                limiter_->Release();
            }
        }

        virtual Status Read(uint64_t offset, size_t n, Slice *result,
                            char *scratch) const {
            int fd = fd_;
            if (temporary_fd_) {
                fd = open(filename_.c_str(), O_RDONLY);
                if (fd < 0) {
                    return PosixError(filename_, errno);
                }
            }

            Status s;
            ssize_t r = pread(fd, scratch, n, static_cast<off_t>(offset));
            *result = Slice(scratch, (r < 0) ? 0 : r);
            if (r < 0) {
                // An error: return a non-ok status
                s = PosixError(filename_, errno);
            }
            if (temporary_fd_) {
                // Close the temporary file descriptor opened earlier.
                close(fd);
            }
            return s;
        }
    };

    class PosixWritableFile : public leveldb::WritableFile {
    private:
        // buf_[0, pos_-1] contains data to be written to fd_.
        std::string filename_;
        int fd_;
        char buf_[kBufSize];
        size_t pos_;

    public:
        PosixWritableFile(const std::string &fname, int fd)
                : filename_(fname), fd_(fd), pos_(0) {}

        ~PosixWritableFile() {
            if (fd_ >= 0) {
                // Ignoring any potential errors
                Close();
            }
        }

        virtual Status Append(const Slice &data) {
            size_t n = data.size();
            const char *p = data.data();

            // Fit as much as possible into buffer.
            size_t copy = std::min(n, kBufSize - pos_);
            memcpy(buf_ + pos_, p, copy);
            p += copy;
            n -= copy;
            pos_ += copy;
            if (n == 0) {
                return Status::OK();
            }

            // Can't fit in buffer, so need to do at least one write.
            Status s = FlushBuffered();
            if (!s.ok()) {
                return s;
            }

            // Small writes go to buffer, large writes are written directly.
            if (n < kBufSize) {
                memcpy(buf_, p, n);
                pos_ = n;
                return Status::OK();
            }
            return WriteRaw(p, n);
        }

        virtual Status Close() {
            Status result = FlushBuffered();
            const int r = close(fd_);
            if (r < 0 && result.ok()) {
                result = PosixError(filename_, errno);
            }
            fd_ = -1;
            return result;
        }

        virtual Status Flush() {
            return FlushBuffered();
        }

        virtual Status Setlevel(int level = 0) { return Status::OK(); }

        virtual const char *Getbuf() { return NULL; }

        Status SyncDirIfManifest() {
            const char *f = filename_.c_str();
            const char *sep = strrchr(f, '/');
            Slice basename;
            std::string dir;
            if (sep == NULL) {
                dir = ".";
                basename = f;
            } else {
                dir = std::string(f, sep - f);
                basename = sep + 1;
            }
            Status s;
            if (basename.starts_with("MANIFEST")) {
                int fd = open(dir.c_str(), O_RDONLY);
                if (fd < 0) {
                    s = PosixError(dir, errno);
                } else {
                    if (fsync(fd) < 0) {
                        s = PosixError(dir, errno);
                    }
                    close(fd);
                }
            }
            return s;
        }

        virtual Status Sync() {
            // Ensure new files referred to by the manifest are in the filesystem.
            Status s = SyncDirIfManifest();
            if (!s.ok()) {
                return s;
            }
            s = FlushBuffered();
            if (s.ok()) {
                if (fdatasync(fd_) != 0) {
                    s = PosixError(filename_, errno);
                }
            }
            return s;
        }

    private:
        Status FlushBuffered() {
            Status s = WriteRaw(buf_, pos_);
            pos_ = 0;
            return s;
        }

        Status WriteRaw(const char *p, size_t n) {
            while (n > 0) {
                ssize_t r = write(fd_, p, n);
                if (r < 0) {
                    if (errno == EINTR) {
                        continue;  // Retry
                    }
                    return PosixError(filename_, errno);
                }
                p += r;
                n -= r;
            }
            return Status::OK();
        }
    };

    class PosixSequentialFile : public leveldb::SequentialFile {
    private:
        std::string filename_;
        int fd_;

    public:
        PosixSequentialFile(const std::string &fname, int fd)
                : filename_(fname), fd_(fd) {}

        virtual ~PosixSequentialFile() { close(fd_); }

        virtual Status Read(size_t n, Slice *result, char *scratch) {
            Status s;
            while (true) {
                ssize_t r = read(fd_, scratch, n);
                if (r < 0) {
                    if (errno == EINTR) {
                        continue;  // Retry
                    }
                    s = PosixError(filename_, errno);
                    break;
                }
                *result = Slice(scratch, r);
                break;
            }
            return s;
        }

        virtual Status Skip(uint64_t n) {
            if (lseek(fd_, n, SEEK_CUR) == static_cast<off_t>(-1)) {
                return PosixError(filename_, errno);
            }
            return Status::OK();
        }
    };


    class PosixFileLock : public leveldb::FileLock {
    public:
        int fd_;
        std::string name_;
    };

    bool PosixLockTable::Insert(const std::string &fname) {
        leveldb::MutexLock l(&mu_);
        return locked_files_.insert(fname).second;
    }

    void PosixLockTable::Remove(const std::string &fname) {
        leveldb::MutexLock l(&mu_);
        locked_files_.erase(fname);
    }

    Limiter::Limiter(intptr_t n) {
        SetAllowed(n);
    }

    Limiter::Limiter(const hm::Limiter &) {

    }

    void Limiter::operator=(const hm::Limiter &) {

    }

    bool Limiter::Acquire() {
        if (GetAllowed() <= 0) {
            return false;
        }
        leveldb::MutexLock l(&mu_);
        intptr_t x = GetAllowed();
        if (x <= 0) {
            return false;
        } else {
            SetAllowed(x - 1);
            return true;
        }
    }

    void Limiter::Release() {
        leveldb::MutexLock l(&mu_);
        SetAllowed(GetAllowed() + 1);
    }

    intptr_t Limiter::GetAllowed() const {
        return reinterpret_cast<intptr_t>(allowed_.Acquire_Load());
    }

    void Limiter::SetAllowed(intptr_t v) {
        allowed_.Release_Store(reinterpret_cast<void *>(v));
    }

    HMEnv::HMEnv(HMManager *hm_manager)
            : started_bgthread_(false),
              mmap_limit_(MaxMmaps()),
              fd_limit_(MaxOpenFiles()),
              hm_manager_(hm_manager) {
        PthreadCall("mutex_init", pthread_mutex_init(&mu_, NULL));
        PthreadCall("cvar_init", pthread_cond_init(&bgsignal_, NULL));
    }

    HMEnv::~HMEnv() {
        char msg[] = "Destroying Env::Default()\n";
        fwrite(msg, 1, sizeof(msg), stderr);
        abort();
    }

    Status HMEnv::NewSequentialFile(const std::string &fname,
                                    leveldb::SequentialFile **result) {
        int fd = open(fname.c_str(), O_RDONLY);
        if (fd < 0) {
            *result = NULL;
            return PosixError(fname, errno);
        } else {
            *result = new PosixSequentialFile(fname, fd);
            return Status::OK();
        }
    }

    Status HMEnv::NewRandomAccessFile(const std::string &fname,
                                      leveldb::RandomAccessFile **result, int flag, const char *buf_file) {
        *result = NULL;
        Status s;
        if (isSSTableName(fname)) {
            if (flag && FindTableOld) {
                *result = new HMRamdomAccessFile(fname, hm_manager_);
                return Status::OK();
            }

            if (ReadWholeTable) {
                *result = new HMComRamdomAccessFile(fname, hm_manager_, buf_file);
                return Status::OK();
            } else {
                *result = new HMRamdomAccessFile(fname, hm_manager_);
                return Status::OK();
            }
        }
        int fd = open(fname.c_str(), O_RDONLY);
        if (fd < 0) {
            s = PosixError(fname, errno);
        } else if (mmap_limit_.Acquire()) {
            uint64_t size;
            s = GetFileSize(fname, &size);
            if (s.ok()) {
                void *base = mmap(NULL, size, PROT_READ, MAP_SHARED, fd, 0);
                if (base != MAP_FAILED) {
                    *result = new PosixMmapReadableFile(fname, base, size, &mmap_limit_);
                } else {
                    s = PosixError(fname, errno);
                }
            }
            close(fd);
            if (!s.ok()) {
                mmap_limit_.Release();
            }
        } else {
            *result = new PosixRandomAccessFile(fname, fd, &fd_limit_);
        }
        return s;
    }

    Status HMEnv::NewWritableFile(const std::string &fname,
                                  leveldb::WritableFile **result,
                                  int level) {
        Status s;
        if (isSSTableName(fname)) {
            if (level == 0) {
                *result = new HMWritableFileL0(fname, hm_manager_, level);
                return Status::OK();
            } else {
                *result = new HMWritableFile(fname, hm_manager_, level);
                return Status::OK();
            }

        }
        int fd = open(fname.c_str(), O_TRUNC | O_WRONLY | O_CREAT, 0644);
        if (fd < 0) {
            *result = NULL;
            s = PosixError(fname, errno);
        } else {
            *result = new PosixWritableFile(fname, fd);
        }
        return s;
    }

    Status HMEnv::NewAppendableFile(const std::string &fname,
                                    leveldb::WritableFile **result) {
        Status s;
        int fd = open(fname.c_str(), O_APPEND | O_WRONLY | O_CREAT, 0644);
        if (fd < 0) {
            *result = NULL;
            s = PosixError(fname, errno);
        } else {
            *result = new PosixWritableFile(fname, fd);
        }
        return s;
    }

    bool HMEnv::FileExists(const std::string &fname) {
        return access(fname.c_str(), F_OK) == 0;
    }

    Status HMEnv::GetChildren(const std::string &dir,
                              std::vector <std::string> *result) {
        result->clear();
        DIR *d = opendir(dir.c_str());
        if (d == NULL) {
            return PosixError(dir, errno);
        }
        struct dirent *entry;
        while ((entry = readdir(d)) != NULL) {
            result->push_back(entry->d_name);
        }
        closedir(d);
        return Status::OK();
    }

    Status HMEnv::DeleteFile(const std::string &fname) {
        Status result;
        if (isSSTableName(fname)) {
            hm_manager_->hm_delete(Parsefname(fname));
            return Status::OK();
        }
        if (unlink(fname.c_str()) != 0) {
            result = PosixError(fname, errno);
        }
        return result;
    }

    Status HMEnv::CreateDir(const std::string &name) {
        Status result;
        if (mkdir(name.c_str(), 0755) != 0) {
            result = PosixError(name, errno);
        }
        return result;
    }

    Status HMEnv::DeleteDir(const std::string &name) {
        Status result;
        if (rmdir(name.c_str()) != 0) {
            result = PosixError(name, errno);
        }
        return result;
    }

    Status HMEnv::GetFileSize(const std::string &fname, uint64_t *size) {
        Status s;
        struct stat sbuf;
        if (stat(fname.c_str(), &sbuf) != 0) {
            *size = 0;
            s = PosixError(fname, errno);
        } else {
            *size = sbuf.st_size;
        }
        return s;
    }

    Status HMEnv::RenameFile(const std::string &src, const std::string &target) {
        Status result;
        if (rename(src.c_str(), target.c_str()) != 0) {
            result = PosixError(src, errno);
        }
        return result;
    }

    Status HMEnv::LockFile(const std::string &fname, leveldb::FileLock **lock) {
        *lock = NULL;
        Status result;
        int fd = open(fname.c_str(), O_RDWR | O_CREAT, 0644);
        if (fd < 0) {
            result = PosixError(fname, errno);
        } else if (!locks_.Insert(fname)) {
            close(fd);
            result = Status::IOError("lock " + fname, "already held by process");
        } else if (LockOrUnlock(fd, true) == -1) {
            result = PosixError("lock " + fname, errno);
            close(fd);
            locks_.Remove(fname);
        } else {
            PosixFileLock *my_lock = new PosixFileLock;
            my_lock->fd_ = fd;
            my_lock->name_ = fname;
            *lock = my_lock;
        }
        return result;
    }

    Status HMEnv::UnlockFile(leveldb::FileLock *lock) {
        PosixFileLock *my_lock = reinterpret_cast<PosixFileLock *>(lock);
        Status result;
        if (LockOrUnlock(my_lock->fd_, false) == -1) {
            result = PosixError("unlock", errno);
        }
        locks_.Remove(my_lock->name_);
        close(my_lock->fd_);
        delete my_lock;
        return result;
    }

    Status HMEnv::GetTestDirectory(std::string *result) {
        const char *env = getenv("TEST_TMPDIR");
        if (env && env[0] != '\0') {
            *result = env;
        } else {
            char buf[100];
            snprintf(buf, sizeof(buf), "/tmp/leveldbtest-%d", int(geteuid()));
            *result = buf;
        }
        // Directory may already exist
        CreateDir(*result);
        return Status::OK();
    }

    uint64_t HMEnv::gettid() {
        pthread_t tid = pthread_self();
        uint64_t thread_id = 0;
        memcpy(&thread_id, &tid, std::min(sizeof(thread_id), sizeof(tid)));
        return thread_id;
    }

    Status HMEnv::NewLogger(const std::string &fname, leveldb::Logger **result) {
        FILE *f = fopen(fname.c_str(), "w");
        if (f == NULL) {
            *result = NULL;
            return PosixError(fname, errno);
        } else {
            *result = new leveldb::PosixLogger(f, &HMEnv::gettid);
            return Status::OK();
        }
    }

    uint64_t HMEnv::NowMicros() {
        struct timeval tv;
        gettimeofday(&tv, NULL);
        return static_cast<uint64_t>(tv.tv_sec) * 1000000 + tv.tv_usec;
    }

    void HMEnv::SleepForMicroseconds(int micros) {
        usleep(micros);
    }


    void HMEnv::Schedule(void (*function)(void *), void *arg) {
        PthreadCall("lock", pthread_mutex_lock(&mu_));

        // Start background thread if necessary
        if (!started_bgthread_) {
            started_bgthread_ = true;
            PthreadCall(
                    "create thread",
                    pthread_create(&bgthread_, NULL, &HMEnv::BGThreadWrapper, this));
        }

        // If the queue is currently empty, the background thread may currently be
        // waiting.
        if (queue_.empty()) {
            PthreadCall("signal", pthread_cond_signal(&bgsignal_));
        }

        // Add to priority queue
        queue_.push_back(BGItem());
        queue_.back().function = function;
        queue_.back().arg = arg;

        PthreadCall("unlock", pthread_mutex_unlock(&mu_));
    }

    void HMEnv::BGThread() {
        while (true) {
            // Wait until there is an item that is ready to run
            PthreadCall("lock", pthread_mutex_lock(&mu_));
            while (queue_.empty()) {
                PthreadCall("wait", pthread_cond_wait(&bgsignal_, &mu_));
            }

            void (*function)(void *) = queue_.front().function;
            void *arg = queue_.front().arg;
            queue_.pop_front();

            PthreadCall("unlock", pthread_mutex_unlock(&mu_));
            (*function)(arg);
        }
    }

    namespace {
        struct StartThreadState {
            void (*user_function)(void *);

            void *arg;
        };
    }

    static void *StartThreadWrapper(void *arg) {
        StartThreadState *state = reinterpret_cast<StartThreadState *>(arg);
        state->user_function(state->arg);
        delete state;
        return NULL;
    }

    void HMEnv::StartThread(void (*function)(void *arg), void *arg) {
        pthread_t t;
        StartThreadState *state = new StartThreadState;
        state->user_function = function;
        state->arg = arg;
        PthreadCall("start thread",
                    pthread_create(&t, NULL, &StartThreadWrapper, state));
    }

    void HMEnv::PthreadCall(const char *label, int result) {
        if (result != 0) {
            fprintf(stderr, "pthread %s: %s\n", label, strerror(result));
            abort();
        }
    }

    void *HMEnv::BGThreadWrapper(void *arg) {
        reinterpret_cast<HMEnv *>(arg)->BGThread();
        return NULL;
    }

    static leveldb::Env *default_env;

    static void InitDefaultEnv(leveldb::HMManager* hm_manager) {
        default_env = new HMEnv(hm_manager);
    }


    leveldb::Env *HMEnv::Default(leveldb::HMManager *hm_manager) {
        InitDefaultEnv(hm_manager);
        return default_env;
    }
}