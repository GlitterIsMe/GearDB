#include "../hm/my_log.h"

namespace leveldb{

    FILE *fp1 = nullptr;
    FILE *fp2 = nullptr;
    FILE *fp3 = nullptr;
    FILE *fp4 = nullptr;
    FILE *fp5 = nullptr;
    FILE *fp6 = nullptr;
    FILE *fp7 = nullptr;
    FILE *fp8 = nullptr;

    void init_log_file(){
#if GEARDB_DBBUG
        fp1 = fopen(log_name.c_str(), "w");
        if(fp1 == nullptr) printf("log failed\n");

#if 0
        fp2 = fopen(log_name2.c_str(), "w");
        if(fp2 == nullptr) printf("log failed\n");

#endif
        fp3 = fopen(log_name3.c_str(), "w");
        if(fp3 == nullptr) printf("log failed\n");

        fp4 = fopen(log_name4.c_str(), "w");
        if(fp4 == nullptr) printf("log failed\n");
        MyLog4("compaction,read(MB),write(MB),time(s)\n");

        fp5 = fopen(log_name5.c_str(), "w");
        if(fp5 == nullptr) printf("log failed\n");

        fp6 = fopen(log_name6.c_str(), "w");
        if(fp6 == nullptr) printf("log failed\n");
#endif

    }

    void MyLog(const char* format, ...){
#if GEARDB_DBBUG
        va_list ap;
        va_start(ap, format);
        char buf[1024];
        vsprintf(buf, format, ap);
        va_end(ap);

        fprintf(fp1, "%s", buf);

#endif
    }

    void MyLog2(const char* format, ...){
#if 0
        va_list ap;
        va_start(ap, format);
        char buf[1024];

        vsprintf(buf, format, ap);

        va_end(ap);

        fprintf(fp2, "%s", buf);
#endif
    }

    void MyLog3(const char* format, ...){
#if GEARDB_DBBUG
        va_list ap;
        va_start(ap, format);
        char buf[1024];

        vsprintf(buf, format, ap);

        va_end(ap);

        fprintf(fp3, "%s", buf);
#endif
    }

    void MyLog4(const char* format, ...){
#if GEARDB_DBBUG
        va_list ap;
        va_start(ap, format);
        char buf[1024];

        vsprintf(buf, format, ap);

        va_end(ap);

        fprintf(fp4, "%s", buf);
#endif
    }

    void MyLog5(const char* format, ...){
#if GEARDB_DBBUG
        va_list ap;
        va_start(ap, format);
        char buf[1024];

        vsprintf(buf, format, ap);

        va_end(ap);

        fprintf(fp5, "%s", buf);
#endif
    }

    void MyLog6(const char* format, ...){
#if GEARDB_DBBUG
        va_list ap;
        va_start(ap, format);
        char buf[1024];

        vsprintf(buf, format, ap);

        va_end(ap);

        fprintf(fp6, "%s", buf);
#endif
    }

    void close_log() {
#if GEARDB_DBBUG
        fclose(fp1);
        //fclose(fp2);
        fclose(fp3);
        fclose(fp4);
        fclose(fp5);
        fclose(fp6);
#endif
    }

}