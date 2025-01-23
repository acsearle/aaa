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
    //
    // We don't enforce alignment, which is equivalent to requiring that all
    // objects allocated must have a size divisible by the alignment of the
    // most-aligned object
    
    struct _arena_t {
        unsigned char* begin;  // next allocation
        unsigned char* end;    // first unavailable byte
        _arena_t* predecessor; // previous allocation
        void* _padding;        // put data[] at a 16 byte offset
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
        
    
    
    // TODO: Put all metadata in a struct that lives in the thread_local
    // service object that is cache hot, and then when the slab is exhausted
    // copy this metadata as the first allocation of the new slab?
    
    struct BumpAllocator {
        intptr_t address;
        intptr_t lower_bound;
        size_t size;
        BumpAllocator* predecessor;
    };
    
    inline thread_local BumpAllocator* thread_local_bump_allocator = nullptr;
    
    void* _bump_allocator_grow(BumpAllocator* allocator, size_t alignment, size_t size);
    
    inline void* bump_allocator_aligned_alloc(std::size_t alignment, std::size_t size) {
        BumpAllocator* allocator = thread_local_bump_allocator;
        intptr_t address = allocator->address;
        intptr_t lower_bound = allocator->lower_bound;
        intptr_t new_address = address - size;
        intptr_t aligned_address = new_address & (alignment - 1);
        bool success = !(aligned_address < lower_bound);
        if (success) [[likely]] {
            allocator->address = aligned_address;
            return (void*)aligned_address;
        } else [[unlikely]] {
            return _bump_allocator_grow(allocator, alignment, size);
        }
    }
    
    
    
} // namespace aaa

#endif /* allocator_hpp */
