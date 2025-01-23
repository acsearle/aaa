//
//  termination_detection_barrier.hpp
//  aaa
//
//  Created by Antony Searle on 21/1/2025.
//

#ifndef termination_detection_barrier_hpp
#define termination_detection_barrier_hpp

#include <atomic>

namespace aaa {
    
    struct termination_detection_barrier {
        
        std::atomic<std::ptrdiff_t> _count;
        
        termination_detection_barrier(size_t count)
        : _count(count) {
        }
        
        void set_active() {
            _count.fetch_add(1, std::memory_order_relaxed);
        }
        
        void set_inactive() {
            _count.fetch_sub(1, std::memory_order_release);
        }
        
        bool is_terminated() const {
            return _count.load(std::memory_order_acquire) == 0;
        }
        
    };
    
    
} // namespace aaa

#endif /* termination_detection_barrier_hpp */
