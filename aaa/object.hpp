//
//  object.hpp
//  client
//
//  Created by Antony Searle on 16/6/2024.
//

#ifndef object_hpp
#define object_hpp



#include "atomic.hpp"
// #include "concepts.hpp"
// #include "typeinfo.hpp"
// #include "type_traits.hpp"

#include <cstdio>

#include <atomic>
#include <concepts>
#include <string_view>

namespace aaa::gc {
    
    template<typename T>
    constexpr std::string_view type_name() {
        std::string_view p = __PRETTY_FUNCTION__;
        return std::string_view(p.data() + 39, p.size() - 39 - 1);
    }
    
    template<typename T>
    constexpr std::string_view name_of = type_name<T>();
    
    // Tricolor abstraction color
    enum class Color {
        WHITE = 0,
        BLACK = 1,
        GRAY  = 2,
        RED   = 3,
    };
    
    struct AtomicEncodedColor {
        
        Atomic<std::underlying_type_t<Color>> _encoded;
        
        AtomicEncodedColor();
        Color load() const;
        bool compare_exchange(Color& expected, Color desired);
        
    };
    
    // TODO: VirtualObject?
    struct Object {
        
        static void* operator new(size_t number_of_bytes);
        static void* operator new(size_t number_of_bytes, void* placement) {
            return placement;
        }
        static void operator delete(void*);
        
        static void* operator new[](size_t number_of_bytes) = delete;
        static void operator delete[](void*) = delete;
        
        // TODO: is it useful to have a base class above tricolored + sweep?
        mutable AtomicEncodedColor color;
        
        Object();
        Object(const Object&);
        Object(Object&&);
        virtual ~Object() = default;
        Object& operator=(const Object&);
        Object& operator=(Object&&);
                
        virtual void _object_debug() const;
        virtual void _object_shade() const;
        virtual void _object_trace() const;
        virtual void _object_trace_weak() const;
        
        virtual void _object_scan() const = 0;
        virtual Color _object_sweep() const;
        
    }; // struct Object
    
    template<std::derived_from<Object> T> void object_debug(T*const& self);
    template<std::derived_from<Object> T> void object_passivate(T*& self);
    template<std::derived_from<Object> T> void object_shade(T*const& self);
    template<std::derived_from<Object> T> void object_trace(T*const& self);
    template<std::derived_from<Object> T> void object_trace_weak(T*const& self);
    
    template<typename T> void any_debug(T const& self) {
        std::string_view sv = name_of<T>;
        printf("(%.*s)\n", (int) sv.size(), sv.data());
    }
    
    template<typename T> auto any_read(T const& self) {
        return self;
    }
    
    template<typename T>
    T any_none;
    
    template<typename T>
    inline constexpr T* any_none<T*> = nullptr;
    
} // namespace aaa::gc


namespace aaa::gc {
    
    // useful defaults
    
    inline void Object::_object_trace_weak() const {
        _object_trace();
    }
    
    inline Color Object::_object_sweep() const {
        return color.load();
    }
    
    
    // implementation of
    
    template<std::derived_from<Object> T>
    void object_debug(T*const& self) {
        if (self) {
            self->_object_debug();
        } else {
            printf("(const Object*)nullptr\n");
        }
    }
    
    template<std::derived_from<Object> T>
    void object_passivate(T*& self) {
        self = nullptr;
    }
    
    template<std::derived_from<Object> T>
    void object_shade(T*const& self) {
        if (self)
            self->_object_shade();
    }
    
    template<std::derived_from<Object> T>
    void object_trace(T*const& self) {
        if (self)
            self->_object_trace();
    }
    
    template<std::derived_from<Object> T>
    void object_trace_weak(T*const& self) {
        if (self)
            self->_object_trace_weak();
    }
    
} // namespace aaa::gc

#endif /* object_hpp */
