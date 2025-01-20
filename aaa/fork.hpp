//
//  fork.hpp
//  aaa
//
//  Created by Antony Searle on 19/1/2025.
//

#ifndef fork_hpp
#define fork_hpp

#include "awaitable.hpp"

namespace aaa {
    
    // inspired by libfork
    //
    // https://arxiv.org/abs/2402.18480
    
    
    // In our use case of each task spawning 64 tasks in a loop, it can't be
    // good to suspend the loop every iteration, enqueue it and run it on a new
    // core with attendant cache miss, can it?
    //
    // However, using work stacks rather than queues seems to yield some of the
    // same goodness; each thread runs its youngest job but steals other
    // thread's oldest job
    
    struct co_fork {
        
        std::coroutine_handle<> _handle;
        
        constexpr bool await_ready() const noexcept {
            return false;
        }
        
        std::coroutine_handle<> await_suspend(std::coroutine_handle<> outer) noexcept {
            schedule_coroutine_handle(outer);
            return std::exchange(_handle, nullptr);
        }
        
        void await_resume() const noexcept {
        }
        
        ~co_fork() {
            assert(_handle == nullptr);
        }
        
    };
    
    struct co_join {
        
    };
    
    
}

#endif /* fork_hpp */
