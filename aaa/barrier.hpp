//
//  barrier.hpp
//  aaa
//
//  Created by Antony Searle on 11/1/2025.
//

#ifndef barrier_hpp
#define barrier_hpp

#include "awaitable.hpp"

namespace aaa {
    
    // single use barrier, pending need for multi-use
    struct barrier {
        
        struct awaitable {
            barrier* _barrier;
            awaitable* _next = nullptr;
            std::coroutine_handle<> _handle = nullptr;
            
            constexpr bool await_ready() const noexcept { return false; }
            void await_suspend(std::coroutine_handle<> handle) noexcept {
                // install ourself on the stack as an awaiter
                _handle = handle;
                _next = _barrier->_awaiters.load(std::memory_order_relaxed);
                while (!_barrier->_awaiters.compare_exchange_weak(_next,
                                                                 this,
                                                                 std::memory_order_relaxed,
                                                                 std::memory_order_relaxed))
                    ;
                _barrier->arrive();
            }
            constexpr void await_resume() const noexcept {}
            
        };
        
        std::atomic<ptrdiff_t> _count;
        std::atomic<awaitable*> _awaiters;
        
        explicit barrier(ptrdiff_t count)
        : _count(count)
        , _awaiters(nullptr) {
        }
        
        ~barrier() {
            // assert barrier completion happens-before destruction
            assert(_count.load(std::memory_order_relaxed) == 0);
        }
        
        void arrive() {
            // decrement the barrier but do not wait
            ptrdiff_t old = _count.fetch_sub(1, std::memory_order_release);
            // negative numbers mean that more coroutines than allowed have
            // waited on the barrier
            assert(old > 0);
            if (old == 1) {
                // we completed the barrier; establish ordering with other
                // decrementers
                (void) _count.load(std::memory_order_acquire);
                // we can now traverse the stack
                awaitable* current = _awaiters.load(std::memory_order_relaxed);
                assert(current); // at least our node will be present
                while (current) {
                    schedule_coroutine_handle(current->_handle);
                    current = current->_next;
                }
            }
        }
        
        auto operator co_await() noexcept {
            return awaitable{this};
        }
        
    };
    
    
}

#endif /* barrier_hpp */
