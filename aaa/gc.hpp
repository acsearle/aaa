//
//  gc.hpp
//  aaa
//
//  Created by Antony Searle on 21/1/2025.
//

#ifndef gc_hpp
#define gc_hpp

#include <cstddef>

namespace aaa::gc {
    
    void collector_start();
    bool collector_this_thread_is_collector_thread();
    void collector_stop();
    
    void mutator_enter();
    bool mutator_is_entered();
    
    enum class HandshakeResult {
        OK,
        COLLECTOR_DID_REQUEST_MUTATOR_LEAVES,
    };
    
    void mutator_handshake();
    void mutator_leave();
    
    void* allocate(std::size_t bytes);
    void deallocate(void* ptr, std::size_t bytes);
    
} // namespace gc

#endif /* gc_hpp */
