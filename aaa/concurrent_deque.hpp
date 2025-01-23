//
//  concurrent_queue.hpp
//  aaa
//
//  Created by Antony Searle on 31/12/2024.
//

#ifndef concurrent_deque_hpp
#define concurrent_deque_hpp

#include <os/lock.h>
#include <os/os_sync_wait_on_address.h>

#include <condition_variable>
#include <deque>
#include <mutex>

namespace aaa {

    // A straightforward concurrent queue built with STL
    
    template<typename T>
    struct concurrent_deque_stl {
        
        // TODO: bad interface.  Enum?
        struct done_exception : std::exception {
            virtual const char* what() const noexcept {
                return "ConcurrentQueue2 is done\n";
            }
        };
        
        std::mutex _mutex;
        std::condition_variable _condition_variable;
        std::deque<T> _queue;
        ptrdiff_t _waiting;
        bool _done;
        
        concurrent_deque_stl()
        : _waiting(0)
        , _done(false) {
        }
                
        void _locked_emplace(auto&&... args) {
            _queue.emplace_back(std::forward<decltype(args)>(args)...);
            if (_waiting)
                _condition_variable.notify_one();
        }
        
        bool _locked_try_pop(T& item) {
            if (_queue.empty())
                return false;
            item = _queue.front();
            _queue.pop_front();
            return true;
        }
        
        void emplace(auto&&... args) {
            std::unique_lock lock{_mutex};
            _locked_emplace(std::forward<decltype(args)>(args)...);
        }
             
        bool try_pop_weak(T& victim) {
            std::unique_lock lock{_mutex, std::try_to_lock};
            if (!lock.owns_lock())
                return false;
            return _locked_try_pop(victim);
        }
        
        bool try_pop_stong(T& victim) {
            std::unique_lock lock{_mutex};
            return _locked_try_pop(victim);
        }
                
        T pop_wait() {
            std::unique_lock lock{_mutex};
            for (;;) {
                if (!_queue.empty()) {
                    T item = _queue.front();
                    _queue.pop_front();
                    return item;
                }
                if (_done)
                    throw done_exception{};
                ++_waiting;
                _condition_variable.wait(lock);
                --_waiting;
            }
        }
        
        void mark_done() {
            std::unique_lock lock{_mutex};
            if (!_done) {
                _done = true;
                if (_waiting)
                    _condition_variable.notify_all();
            }
        }
        
    }; // concurrent_deque_stl
    
    
    // A better-performing concurrent queue built with Apple-specific
    // os_unfair_lock and os_sync_wait_on_address
    
    template<typename T>
    struct concurrent_deque_apple {
                
        // TODO: bad interface.  Enum?
        struct done_exception : std::exception {
            virtual const char* what() const noexcept {
                return "ConcurrentQueue2 is done\n";
            }
        };

        os_unfair_lock _mutex = OS_UNFAIR_LOCK_INIT;
        std::atomic<uint64_t> _generation = 0;
        
        std::deque<T> _queue;
        ptrdiff_t _waiting = 0;
        bool _done = false;
        
        void _locked_emplace_back(auto&&... args) {
            os_unfair_lock_assert_owner(&_mutex);
            _queue.emplace_back(std::forward<decltype(args)>(args)...);
            ptrdiff_t waiting = _waiting;
            if (waiting)
                _generation.fetch_add(1, std::memory_order_relaxed);
            os_unfair_lock_unlock(&_mutex);
            if (waiting)
                os_sync_wake_by_address_any(&_generation, sizeof(_generation), OS_SYNC_WAKE_BY_ADDRESS_NONE);
        }
        
        void emplace_back(auto&&... args) {
            os_unfair_lock_lock(&_mutex);
            _locked_emplace(std::forward<decltype(args)>(args)...);
        }
        
        bool _locked_try_pop_front(T& item) {
            os_unfair_lock_assert_owner(&_mutex);
            bool done = _done;
            bool empty = _queue.empty();
            if (!done && !empty) {
                item = _queue.front();
                _queue.pop_front();
            }
            os_unfair_lock_unlock(&_mutex);
            if (done)
                throw done_exception{};
            return !empty;
        }
                
        // may fail spuriously under contention
        bool try_pop_front_weak(T& victim) {
            return (os_unfair_lock_trylock(&_mutex)
                    && _locked_try_pop_front(victim));
        }
        
        bool try_pop_front_stong(T& victim) {
            os_unfair_lock_lock(&_mutex);
            return _locked_try_pop_front(victim);
        }
        
        bool _locked_try_pop_back(T& item) {
            os_unfair_lock_assert_owner(&_mutex);
            bool done = _done;
            bool empty = _queue.empty();
            if (!done && !empty) {
                item = _queue.back();
                _queue.pop_back();
            }
            os_unfair_lock_unlock(&_mutex);
            if (done)
                throw done_exception{};
            return !empty;
        }
        
        // may fail spuriously under contention
        bool try_pop_back_weak(T& victim) {
            return os_unfair_lock_trylock(&_mutex) && _locked_try_pop_back(victim);
        }
        
        bool try_pop_back_stong(T& victim) {
            os_unfair_lock_lock(&_mutex);
            return _locked_try_pop_back(victim);
        }
                
        T pop_front_wait() {
            T result;
            uint64_t expected = 0;
            os_unfair_lock_lock(&_mutex);
            for (;;) {
                bool done = _done;
                bool empty = _queue.empty();
                if (!done) {
                    if (!empty) {
                        result = std::move(_queue.front());
                        _queue.pop_front();
                    } else {
                        expected = _generation.load(std::memory_order_relaxed);
                        ++_waiting;
                    }
                }
                os_unfair_lock_unlock(&_mutex);
                if (done)
                    throw done_exception{};
                if (!empty)
                    return result;
                os_sync_wait_on_address(&_generation, expected, sizeof(_generation), OS_SYNC_WAIT_ON_ADDRESS_NONE);
                os_unfair_lock_lock(&_mutex);
                --_waiting;
            }
        }
        
        T pop_back_wait() {
            T result;
            uint64_t expected = 0;
            os_unfair_lock_lock(&_mutex);
            for (;;) {
                bool done = _done;
                bool empty = _queue.empty();
                if (!done) {
                    if (!empty) {
                        result = std::move(_queue.front());
                        _queue.pop_front();
                    } else {
                        expected = _generation.load(std::memory_order_relaxed);
                        ++_waiting;
                    }
                }
                os_unfair_lock_unlock(&_mutex);
                if (done)
                    throw done_exception{};
                if (!empty)
                    return result;
                os_sync_wait_on_address(&_generation, expected, sizeof(_generation), OS_SYNC_WAIT_ON_ADDRESS_NONE);
                os_unfair_lock_lock(&_mutex);
                --_waiting;
            }
        }

        void mark_done() {
            // std::unique_lock lock{_mutex};
            // ptrdiff_t waiting;
            os_unfair_lock_lock(&_mutex);
            _done = true;
            _generation.fetch_add(1, std::memory_order_relaxed);
            os_unfair_lock_unlock(&_mutex);
            os_sync_wake_by_address_all(&_generation, sizeof(_generation), OS_SYNC_WAKE_BY_ADDRESS_NONE);
        }
        
    }; // concurrent_deque_apple
    
    
    

    
    
    
} // namespace aaa

#endif /* concurrent_queue_hpp */
