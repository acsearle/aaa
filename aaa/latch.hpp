//
//  latch.hpp
//  aaa
//
//  Created by Antony Searle on 11/1/2025.
//

#ifndef latch_hpp
#define latch_hpp

#include <cassert>
#include <cstdint>

#include <atomic>
#include <coroutine>

#include "awaitable.hpp"
#include "allocator.hpp"

namespace aaa {
    
    
    
    // coroutine -> co_latch
    //
    // coroutine must take a latch as its first argument
    // the latch will be decremented at the end of the coroutine
    // if the decrement results in zero, the
    //
    // also fork-join
    //      countdown-latch
    //
    // supplies a coroutine return type and promise type that signals the latch
    // on completion and resumes the latch continuation when appropriate
    
    // TODO: seems like signal-object-on-completion-and-resume is a generic
    // pattern
    
    // TODO: this is turning into a forkjoin context or something rather than
    // a latch; rename it
    
    struct latch {
        
        enum : intptr_t {
            NONSIGNALED = 0,
            SIGNALED = 1,
        };
        
        std::atomic<ptrdiff_t> _count;
        std::atomic<intptr_t> _continuation;
        
        ptrdiff_t _dependents;
        
        
        latch()
        : _count(0)
        , _continuation(NONSIGNALED)
        , _dependents(0) {
        }
        
        std::coroutine_handle<> _signal_and_get_continuation() {
            intptr_t observed = _continuation.exchange(SIGNALED, std::memory_order_release);
            if (observed != NONSIGNALED) {
                (void) _continuation.load(std::memory_order_acquire);
                return std::coroutine_handle<>::from_address((void*)observed);
            } else {
                return std::noop_coroutine();
            }
        }
        
        
        void decrement() {
            subtract(1);
        }
        
        void subtract(ptrdiff_t count) {
            assert(count > 0);
            ptrdiff_t n = _count.fetch_sub((ptrdiff_t)count, std::memory_order_release);
            assert(n > 0);
            if (n != count)
                return;
            (void) _count.load(std::memory_order_acquire);
            intptr_t observed = _continuation.exchange(SIGNALED, std::memory_order_release);
            assert(observed != SIGNALED);
            if (observed == NONSIGNALED)
                return;
            (void) _continuation.load(std::memory_order_acquire);
            schedule_coroutine_handle_from_address((void*)observed);
        }
                
        // Awaitable
        
        /*
        constexpr bool await_ready() const noexcept {
            return false;
        }
        
        bool await_suspend(std::coroutine_handle<> handle) noexcept {
            intptr_t expected = NONSIGNALED;
            return _continuation.compare_exchange_strong(expected,
                                                         (intptr_t)handle.address(),
                                                         std::memory_order_release,
                                                         std::memory_order_acquire);
        }
        constexpr void await_resume() const noexcept {
        }
         */
                
        bool _signalling_coroutine_decrement() {
            ptrdiff_t n = _count.fetch_sub((ptrdiff_t)1, std::memory_order_release);
            // assert(n > 0);
            if (n == 1)
                (void) _count.load(std::memory_order_acquire);
            return n == 1;
        }
        
        
        
        //auto with(ptrdiff_t count) {
        //  assert(count > 0);
        //struct awaitable {
        //  latch* _latch;
        //ptrdiff_t _addend;
        //};
        //return awaitable{this, count};
        //}

        bool await_ready() noexcept {
            if (_dependents == 0)
                // there are no jobs to wait for
                return true;
            // TODO: atomic::add_fetch
            ptrdiff_t n = _count.fetch_add(_dependents, std::memory_order_relaxed) + _dependents;
            // TODO: verify this is actually an optimization
            if (n == 0)
                // all the jobs already finished
                (void) _count.load(std::memory_order_acquire);
            // some jobs were not yet finished
            return n == 0;
        }

        bool await_suspend(std::coroutine_handle<> handle) noexcept {
            intptr_t expected = NONSIGNALED;
            // install the handler; failure means jobs completed and we resume immediately
            return _continuation.compare_exchange_strong(expected,
                                                         (intptr_t)handle.address(),
                                                         std::memory_order_release,
                                                         std::memory_order_acquire);
        }

        void await_resume() {}
        
        // usage: latch::signalling_coroutine my_coroutine(&my_latch, my_arguments...) { ... }
        //
        // on completion of the coroutine, it will signal the latch and, if
        // complete, transfer control to the latch's continuation
        struct signalling_coroutine {
            struct promise_type {
                
                static void* operator new(std::size_t count) {
                    return arena_allocate(count);
                }
                
                static void operator delete(void*) {
                    // no-op
                }
                
                
                latch& _latch;
                promise_type() = delete;
                explicit promise_type(latch& p, auto&&...)
                : _latch(p) {
                    // this is invoked immediately on the spawning thread and
                    // does not need to be atomic **for the intended used case**
                    ++p._dependents;
                }
                
                constexpr signalling_coroutine get_return_object() const noexcept {
                    return signalling_coroutine{};
                }
                
                auto initial_suspend() const noexcept {
                    return suspend_always_and_schedule{};
                }
                
                auto final_suspend() const noexcept {
                    struct awaitable {
                        latch& _latch;
                        bool await_ready() const noexcept {
                            return !_latch._signalling_coroutine_decrement();
                        }
                        std::coroutine_handle<> await_suspend(std::coroutine_handle<promise_type> handle) const noexcept {
                            latch& target = _latch;
                            handle.destroy();
                            return target._signal_and_get_continuation();
                        }
                        void await_resume() const noexcept {
                        }
                    };
                    return awaitable{_latch};
                }
                constexpr void return_void() const noexcept { }
                void unhandled_exception() const noexcept { std::terminate(); }
            };
        };
    };
    
} // namespace aaa


#endif /* latch_hpp */
