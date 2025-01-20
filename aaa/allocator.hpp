//
//  allocator.hpp
//  aaa
//
//  Created by Antony Searle on 15/1/2025.
//

#ifndef allocator_hpp
#define allocator_hpp

#include <cassert>
#include <cstdio>
#include <cstdlib>
#include <cstddef>

namespace aaa {
    
    // Thread-local arena allocator
    //
    // Objects that will live only until some consensus time (typically end of
    // frame) can be bump-allocated from a per-thread slab, and then simply
    // overwritten next frame.  This reduces work for both mutator and
    // collector, and should be faster than malloc/free which must do more work.
    //
    // Objects so allocated will be not be destructed; they should be
    // TriviallyDestructible
    
    struct _arena_t {
        unsigned char* begin; // next allocation
        unsigned char* end; // first unavailable byte
        _arena_t* predecessor; // earlier, smaller arena
        void* _padding;
        unsigned char data[0]; // the bytes following
    };
    
    inline thread_local _arena_t* _tl_arena = nullptr;
    void* _arena_allocate_cold(size_t n);
    

    // this is the critical hot function
    inline void* arena_allocate(size_t n) {
        // thread-local read
        _arena_t* p = _tl_arena;
        // branch
        if ((p->end - p->begin) >= n) [[likely]] {
            void* q = p->begin;
            p->begin += n;
            // printf("%p\n", q);
            return q;
        } else [[unlikely]] {
            return _arena_allocate_cold(n);
        }
    }
    
    void arena_initialize();
    void arena_finalize();
    void arena_advance();
        
} // namespace aaa

#endif /* allocator_hpp */
