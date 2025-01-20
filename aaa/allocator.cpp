//
//  allocator.cpp
//  aaa
//
//  Created by Antony Searle on 15/1/2025.
//

#include "allocator.hpp"

namespace aaa {
    
    void* _arena_allocate_cold(size_t n) {
        _arena_t* p = _tl_arena;
        assert(p);
        size_t m = (p->end - (unsigned char*) p) << 1;
        _arena_t* q = (_arena_t*)malloc(m);
        q->begin = q->data + n;
        q->end = (unsigned char*)q + m;
        q->predecessor = p;
        _tl_arena = q;
        return q->data;
    }
    
    void arena_initialize() {
        // allocate 1 Mb
        size_t m = 1 << 20;
        _arena_t* p = (_arena_t*)malloc(m);
        p->begin = p->data;
        p->end = (unsigned char*)p + m;
        p->predecessor = nullptr;
        assert(_tl_arena == nullptr);
        _tl_arena = p;
    }
    
    void area_advance() {
        _arena_t* p = _tl_arena;
        // reset the largest arena
        p->begin = p->data;
        // free the other regions
        p = p->predecessor;
        while (p) {
            _arena_t* q = p->predecessor;
            free(p);
            p = q;
        }
    }
    
    void arena_finalize() {
        _arena_t* p = _tl_arena;
        assert(p);
        ptrdiff_t n = 0;
        while (p) {
            n += p->end - (unsigned char*)p;
            _arena_t* q = p->predecessor;
            free(p);
            p = q;
        }
        _tl_arena = nullptr;
        printf("thread allocated %g Mb\n", n / (1024.0 * 1024.0));
    }
    
}
