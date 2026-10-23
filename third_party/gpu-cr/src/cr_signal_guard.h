#ifndef GPU_CR_SRC_CR_SIGNAL_GUARD_H_
#define GPU_CR_SRC_CR_SIGNAL_GUARD_H_

// Masking helper for the CR control signals, shared by the GPU layer and the
// signal handler in vGPU.cpp.
//
// Two distinct hazards need the same mask. A process-directed CR signal may
// be delivered to whichever thread happens to hold gpu_mem_mutex, which
// self-deadlocks when the handler tries to take it; masking the holding
// thread routes delivery elsewhere. And a handler auto-blocks only its own
// signum, so a *different* CR signal can nest on the handler's own thread
// while it owns the mutex — an operation must therefore widen the mask for
// its whole scope, not just around the lock.

#include <pthread.h>

#include <csignal>

#include "common.h"

namespace gpu_cr {

// Blocks every CR control signal for the guard's lifetime. Construct
// immediately before the lock so reverse destruction unlocks before
// unmasking.
class ScopedBlockCrSignals {
public:
    ScopedBlockCrSignals() {
        sigset_t block;
        sigemptyset(&block);
        sigaddset(&block, CR_INIT_SIGNAL);
        sigaddset(&block, CR_CKPT_SIGNAL);
        sigaddset(&block, CR_RESTORE_SIGNAL);
        sigaddset(&block, CR_IPC_TEARDOWN_SIGNAL);
        sigaddset(&block, CR_IPC_REBUILD_SIGNAL);
        sigaddset(&block, CR_IPC_VALIDATE_SIGNAL);
        pthread_sigmask(SIG_BLOCK, &block, &old_mask_);
    }
    ~ScopedBlockCrSignals() {
        // SIG_SETMASK with the saved set: SIG_UNBLOCK would wrongly unmask
        // signals the caller had already blocked.
        pthread_sigmask(SIG_SETMASK, &old_mask_, nullptr);
    }
    ScopedBlockCrSignals(const ScopedBlockCrSignals&) = delete;
    ScopedBlockCrSignals& operator=(const ScopedBlockCrSignals&) = delete;

private:
    sigset_t old_mask_;
};

}  // namespace gpu_cr

#endif  // GPU_CR_SRC_CR_SIGNAL_GUARD_H_
