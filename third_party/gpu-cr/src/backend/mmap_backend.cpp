#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#include <cerrno>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <string>

#include "../common.h"
#include "backend.h"

Backend::Backend(int id) {
}

Backend::~Backend() {
}

void Backend::setup() {
}

ShareMem::ShareMem(int id) : Backend(id), id(id) {
}

ShareMem::~ShareMem() {
}

// Map the dump buffer. fatal=true keeps the historical exit() behavior
// for eager setup; fatal=false returns nullptr so a caller can fail the
// op cleanly instead of killing the workload.
void* ShareMem::map_dump_buffer(bool fatal) {
    char shm_name[512];
    const char* export_file_path = std::getenv("EXPORT_FILE_PATH");
    bool use_file_backend = (export_file_path != nullptr);
    size_t size = SHM_SIZE;

    if (use_file_backend) {
        snprintf(shm_name, sizeof(shm_name), "%s/ckpt-%d.data", export_file_path, id);
        fprintf(stderr, "[ShareMem] Using File Backend: %s (%zu MiB)\n", shm_name, size >> 20);
    } else {
        snprintf(shm_name, sizeof(shm_name), "/mnt/huge-ckpt/%d", id);
    }

    int fd = open(shm_name, O_CREAT | O_RDWR, use_file_backend ? 0644 : 0755);
    if (fd < 0) {
        perror("open() dump buffer");
        if (fatal) exit(EXIT_FAILURE);
        return nullptr;
    }
    if (ftruncate(fd, size) < 0) {
        perror("ftruncate() dump buffer");
        close(fd);
        if (fatal) exit(EXIT_FAILURE);
        return nullptr;
    }
    if (use_file_backend) {
        // Reserve the full configured size up front: tmpfs/disk reserve
        // nothing at ftruncate or mmap, so an over-committed store would
        // SIGBUS mid-dump; full fallocate turns that into a synchronous
        // ENOSPC here. Partial reservation would only move the fault,
        // since the dump extent grows toward the configured size.
        int rc = posix_fallocate(fd, 0, static_cast<off_t>(size));
        if (rc != 0) {
            fprintf(stderr, "[ShareMem] posix_fallocate(%s, %zu MiB): %s\n", shm_name, size >> 20, strerror(rc));
            close(fd);
            if (fatal) exit(EXIT_FAILURE);
            return nullptr;
        }
    }

    void* buf = mmap(nullptr, size, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
    close(fd);
    if (buf == MAP_FAILED) {
        fprintf(stderr, "%s failed: %s\n",
                use_file_backend ? "mmap (file backend)" : "mmap with hugepages", strerror(errno));
        fprintf(stderr, "[ShareMem] configured dump-buffer size is %zu MiB (build-time SHM_SIZE_GB); "
                        "check it against the pod's hugepages-2Mi request and the node pool size\n",
                        size >> 20);
        if (fatal) exit(EXIT_FAILURE);
        return nullptr;
    }
    if (!use_file_backend)
        fprintf(stderr, "Hugepage shared memory mapped at %p (%zu MiB)\n", buf, size >> 20);

    shared_mem_fs* fs = static_cast<shared_mem_fs*>(buf);
    fs->file_num = 0;
    fs->current_offset = ROUND_UP_2MB(sizeof(shared_mem_fs));
    return buf;
}

void ShareMem::setup() {
    char shm_name[512];
    const char* export_file_path = std::getenv("EXPORT_FILE_PATH");
    bool use_file_backend = (export_file_path != nullptr);

    // tmp_buf
    fs_mutex.lock();
    tmp_buf = map_dump_buffer(/*fatal=*/true);
    fs_mutex.unlock();

    // host_buf — follow same backend selection as the main staging buffer.
    if (use_file_backend) {
        snprintf(shm_name, sizeof(shm_name), "%s/ckpt-%d-host.data", export_file_path, id);
    } else {
        snprintf(shm_name, sizeof(shm_name), "/mnt/huge-ckpt/%d-host", id);
    }
    fd_host = open(shm_name, O_CREAT | O_RDWR, 0755);
    if (fd_host < 0) {
        perror("open()");
        exit(EXIT_FAILURE);
    }
    
    size_t host_buf_total_size = STAGING_BUF_SIZE * STAGING_BUF_NUM;
    if (ftruncate(fd_host, host_buf_total_size) < 0) {
        perror("ftruncate() host mem");
        exit(EXIT_FAILURE);
    }

    host_buf_ptr = mmap(NULL, host_buf_total_size, PROT_READ | PROT_WRITE, MAP_SHARED, fd_host, 0);
    if (host_buf_ptr == MAP_FAILED) {
        perror("mmap host mem failed");
        exit(EXIT_FAILURE);
    }
    fprintf(stderr, "[ShareMem] Host memory mapped at %p (size: %zu)\n", host_buf_ptr, host_buf_total_size);

    
}

void* ShareMem::get_tmp_buf() {
    return tmp_buf;
}

void* ShareMem::get_host_buffer() {
        return host_buf_ptr;
    }