//
//  work_stealing_queue.hpp
//  aaa
//
//  Created by Antony Searle on 16/1/2025.
//

#ifndef work_stealing_queue_hpp
#define work_stealing_queue_hpp

#include <cassert>

#include <atomic>
#include <bit>

#include <sanitizer/tsan_interface.h>

namespace aaa {
        
    namespace _work_stealing_deque {
        
        // A lock-free, unbounded SPMC deque suitable for work-stealing
        //
        // This design seems to be widely used despite the relative complexity
        // of the required memory orderings, which are even then not ideal in
        // this platform-agnostic implementation.
        //
        // D. Chase and Y. Lev. Dynamic circular work-stealing deque. In SPAA,
        // 2005
        //
        // Nhat Minh Lê, Antoniu Pop, Albert Cohen, Francesco Zappa Nardelli.
        // Correct and Efficient WorkStealing for Weak Memory Models.
        // PPoPP ’13 - Proceedings of the 18th ACM SIGPLAN symposium on
        // Principles and practice of parallel programming, Feb 2013, Shenzhen,
        // China. pp.69-80, ff10.1145/2442516.2442524ff. ffhal-00802885f
    
                    
        template<typename T>
        concept AlwaysLockFreeAtomic = std::atomic<T>::is_always_lock_free;
                
        template<AlwaysLockFreeAtomic T>
        struct work_stealing_deque {

            constexpr static std::size_t CACHE_LINE_SIZE = 128;
            constexpr static std::size_t INITIAL_CAPACITY = 16;

            struct circular_array {
                
                std::size_t _mask;
                mutable std::atomic<T> _data[0];
                
                std::size_t capacity() const { return _mask + 1; }
                
                explicit circular_array(std::size_t mask)
                : _mask(mask) {
                    assert(std::has_single_bit(_mask + 1));
                }
                
                static circular_array* make(std::size_t capacity) {
                    void* raw = calloc(sizeof(circular_array) + sizeof(T) * capacity, 1);
                    std::size_t mask = capacity - 1;
                    return new(raw) circular_array(mask);
                }
                
                std::atomic<T>& operator[](size_t i) const {
                    return _data[i & _mask];
                }
                
            };
            
            // written by owner
            alignas(CACHE_LINE_SIZE) struct {
                mutable std::atomic<const circular_array*> _array;
                mutable std::atomic<std::ptrdiff_t> _bottom;
                mutable std::ptrdiff_t _cached_top;
            };

            // written by owner and thief
            alignas(CACHE_LINE_SIZE) struct {
                mutable std::atomic<std::ptrdiff_t> _top;
            };
            
            work_stealing_deque()
            : _array(circular_array::make(INITIAL_CAPACITY))
            , _bottom(0)
            , _cached_top(0)
            , _top(0) {
            }
            
            // called by owner thread
            bool pop(T& item) const {
                ptrdiff_t bottom = this->_bottom.load(std::memory_order_relaxed);
                const circular_array* array = this->_array.load(std::memory_order_relaxed);
                ptrdiff_t new_bottom = bottom - 1;
                _bottom.store(new_bottom, std::memory_order_relaxed);
                // TODO: Clang produces suboptimal code on x86 for seq_cst fence
                std::atomic_thread_fence(std::memory_order_seq_cst);
                ptrdiff_t _cached_top = _top.load(std::memory_order_relaxed);
                assert(_cached_top <= bottom);
                ptrdiff_t new_top = _cached_top + 1;
                ptrdiff_t new_size = new_bottom - _cached_top;
                if (new_size < 0) {
                    _bottom.store(bottom, std::memory_order_relaxed);
                    return false;
                }
                item = (*array)[new_bottom].load(std::memory_order_relaxed);
                if (new_size > 0)
                    return true;
                assert(new_size == 0);
                bool success = this->_top.compare_exchange_strong(_cached_top,
                                                                  new_top,
                                                                  std::memory_order_seq_cst,
                                                                  std::memory_order_relaxed);
                assert(bottom == new_top);
                _bottom.store(bottom, std::memory_order_relaxed);
                return success;
            }

            // called by owner thread
            void push(T item) const {
                ptrdiff_t bottom = this->_bottom.load(std::memory_order_relaxed);
                const circular_array* array = this->_array.load(std::memory_order_relaxed);
                ptrdiff_t capacity = array->capacity();
                assert(bottom - _cached_top <= capacity);
                if (bottom - _cached_top == capacity) {
                    _cached_top = this->_top.load(std::memory_order_acquire);
                    assert(bottom - _cached_top <= capacity);
                    if (bottom - _cached_top == capacity) [[unlikely]] {
                        circular_array* new_array = circular_array::make(capacity << 1);
                        for (std::ptrdiff_t i = _cached_top; i != bottom; ++i) {
                            T jtem = (*array)[i].load(std::memory_order_relaxed);
                            (*new_array)[i].store(jtem, std::memory_order_relaxed);
                        }
                        // TODO: Garbage collection write barrier
                        // Currently we are leaking the old array
                        _array.store(new_array, std::memory_order_release);
                        array = new_array;
                    }
                }
                (*array)[bottom].store(item, std::memory_order_relaxed);
                std::atomic_thread_fence(std::memory_order_release);
#if defined(__has_feature)
#    if __has_feature(thread_sanitizer)
                // thread sanitizer requires that release (and acquire) fences
                // are annotated with their associated variable(s)
                __tsan_release(&_top);
#    endif
#endif
                _bottom.store(bottom + 1, std::memory_order_relaxed);
            }

            // called by any thief thread
            bool steal(T& item) const {
                std::ptrdiff_t top = _top.load(std::memory_order_acquire);
                // TODO: Clang produces suboptimal code on x86 for seq_cst fence
                std::atomic_thread_fence(std::memory_order_seq_cst);
                std::ptrdiff_t bottom = _bottom.load(std::memory_order_acquire);
                if (!(top < bottom))
                    return false;
                // TODO: Current status of std::memory_order_consume
                // This likely maps to std::memory_order_acquire
                const circular_array* array = _array.load(std::memory_order_consume);
                item = (*array)[top].load(std::memory_order_relaxed);
                std::ptrdiff_t new_top = top + 1;
                return _top.compare_exchange_strong(top,
                                                    new_top,
                                                    std::memory_order_seq_cst,
                                                    std::memory_order_relaxed);
            }
            
            
        }; // work_stealing_deque
        
        
    } // namespace _work_stealing_deque
    
    using _work_stealing_deque::work_stealing_deque;
    
}

#endif /* work_stealing_queue_hpp */
