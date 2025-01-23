//
//  skiplist.hpp
//  aaa
//
//  Created by Antony Searle on 5/1/2025.
//

#ifndef skiplist_hpp
#define skiplist_hpp

#include <cassert>
#include <cstddef>
#include <cstdio>

#include <atomic>
#include <new>
#include <random>
#include <utility>

#include "allocator.hpp"

namespace aaa {
    
    // A concurrent skiplist supporting insert but not lookup or erase, and a
    // corresponding frozen alias supporting lookup but not mutation
    
    // It is tailored to our two-phase process where we first insert-or-modify
    // elements, take a barrier (or equivalent), and then lookup those elements
    
    // Successor pointers are inline in the nodes using a flexible array member,
    // and the nodes are right-size allocated
    
    // We use a node height distribution of P(n) \propto 4^{-n} which gives
    // very close to the optimal expected runtime (vs e^{-n}), and reduced
    // storage (vs 2^{-n}).  The downside wrt e and 2 is increased runtime
    // variance.  Sampling an e^{-n} distribution would be relatively
    // expensive.
    
    
    //
    // https://epaperpress.com/sortsearch/download/skiplist.pdf
    
    // Skip
    // https://ticki.github.io/blog/skip-lists-done-right/
    
    using std::pair;
    
    inline thread_local std::ranlux24_base* thread_local_random_number_generator = nullptr;
    
    /*
    namespace _distribution {
        
        // experiments to generate the ideal (asterisk) geometric distribution
        // with p = 1 - 1 / e
        
        uint_fast32_t generate_iterative() {
            int count = 1;
            while ((*thread_local_random_number_generator)() < (uint_fast32_t)6171993)
                ++count;
            return count;
        };
        
        uint32_t generate_constant_time() {
            std::uniform_real_distribution<double> a;
            // this is actually going to call ranlux24 3 times to get the
            // required 53 bits, and then do a quite naive job of using them;
            // notably if the range includes a variety of exponents
            double b = a(*thread_local_random_number_generator);
            return (uint32_t)std::ceil(std::log(b) * -2.18019225602);
        }
        
        
    } // namespace _distribution
    */
    
    
    
    template<typename Key, typename Compare = std::less<Key>>
    struct frozen_skiplist {

        struct _node_t;
        struct _array_t;
        
        struct _array_t {
            
            size_t _size;
            const _node_t* _data[0];
            
            size_t size() const { return _size; }
            const _node_t* operator[](size_t index) const {
                assert(index < _size);
                return _data[index];
            }
                        
        };
        
        struct _node_t {
            
            Key _key;
            _array_t _next;
            
            size_t size() const {
                return _next.size();
            }
            
        };
        
        struct _head_t {
            
            size_t _top;
            _array_t _next;
                        
        };
        
        const _head_t* _head;
        
        struct iterator {
            
            const _node_t* _pointer;
            
            void _advance() {
                assert(_pointer);
                _pointer = _pointer->_next[0];
            }
            
            const Key* operator->() const {
                assert(_pointer);
                return &(_pointer->_key);
            }
            
            iterator operator++(int) {
                iterator old{_pointer};
                _advance();
                return old;
            }
            
            iterator& operator++() {
                _advance();
                return *this;
            }
            
            explicit operator bool() const {
                return (bool)_pointer;
            }
            
            bool operator!() const {
                return !_pointer;
            }
            
            const Key& operator*() const {
                assert(_pointer);
                return _pointer->_key;
            }
            
            bool operator==(const iterator&) const = default;
            
        }; // struct iterator
        
        iterator begin() const {
            return iterator{_head->_next[0]};
        }
        
        iterator end() const {
            return iterator{nullptr};
        }
        
        
        
        struct cursor {
            
            // points to the common member of nodes and the (keyless) head
            const _array_t* _next;
            size_t _level;
            
            bool is_bottom() const {
                return _level == 0;
            }
            
            const _node_t* _load() const {
                return (*_next)[_level];
            }

            /*
            const Key& operator*() const {
                return _load()->_key;
            }
            
            const Key* operator->() const {
                return &(_load()->_key);
            }
             */
            
            void advance() {
                const _node_t* a = (*_next)[_level];
                _next = &(a->_next);
            }
            
            void descend() {
                assert(_level);
                --_level;
            }
            
            iterator as_iterator() {
                return iterator{(*_next)[0]};
            }
            
            template<typename Query>
            bool refine_closed_range(const Query& a, const Query& b) {
                for (;;) {
                    const _node_t* d = _load();
                    if (!d || Compare()(b, d->_key)) {
                        // pointee is to the right of range; descend to finer level
                        if (is_bottom())
                            return false;
                        descend();
                    } else if (Compare()(d->_key, a)) {
                        // pointee is to the left of range; advance along current level
                        _next = &(d->_next);
                    } else {
                        // pointee is into the range; we are done
                        return true;
                    }
                }
            }
            
            template<typename Query>
            iterator lower_bound(const Query& a) {
                for (;;) {
                    const _node_t* d = _load();
                    if (!d || Compare()(a, d->_key)) {
                        if (is_bottom())
                            // authoritative
                            return iterator{d};
                        descend();
                    } else if (Compare()(d->_key, a)) {
                        _next = &(d->_next);
                    } else {
                        // exact match
                        return iterator{d};
                    }
                }
            }
            
            template<typename Query>
            iterator reverse_lower_bound(const Query& a) {
                for (;;) {
                    const _node_t* d = _load();
                    if (d && Compare()(d->_key, a)) {
                        // advance
                        _next = &(d->_next);
                    } else if (d && !Compare()(a, d->_key)) {
                        // exact match
                        return iterator{d};
                    } else {
                        if (is_bottom())
                            return iterator{d};
                        descend();
                    }
                }
            }
            
            
            template<typename Query>
            iterator find(const Query& query) {
                for (;;) {
                    const _node_t* candidate = _load();
                    if (!candidate || Compare()(query, candidate->_key)) {
                        if (is_bottom())
                            return iterator{nullptr};
                        descend();
                    } else if (Compare()(candidate->_key, query)) {
                        _next = &(candidate->_next);
                    } else {
                        return iterator{candidate};
                    }
                }
            }
            
            
        };
        
        cursor top() const {
            return cursor{
                &(_head->_next),
                _head->_top - 1
            };
        }
        
        template<typename Query>
        iterator find(const Query& query) const {
            size_t level = _head->_top - 1;
            const _array_t* array = &(_head->_next);
            for (;;) {
                const _node_t* candidate = (*array)[level];
                if (!candidate || Compare()(query, candidate->_key)) {
                    if (level == 0)
                        return iterator{nullptr};
                    --level;
                } else if (Compare()(candidate->_key, query)) {
                    array = &(candidate->_next);
                } else {
                    return iterator{candidate};
                }
            }
        }
        
        // Returns the lowest level cursor on the search path of all keys in
        // the given keyrange.  This is a useful starting point for many
        // algorithms.  The pointee of the cursor will be a node inside the
        // range, if such a node exists, else to a node to the right of the
        // range,if such node a nodes exists, else to .end().
        template<typename Query>
        pair<cursor, bool> cursor_for_closed_range(const Query& a, const Query& b) const {
            cursor c = top();
            bool flag = c.refine_closed_range(a, b);
            return {c, flag};
        }
        
        // Tests if any entries exist in the given keyrange.  This is a trivial
        // specialization of cursor_for_closed_range.
        template<typename Query>
        bool intersects_closed_range(const Query& a, const Query& b) const {
            cursor c = top();
            return c.refine_closed_range(a, b);
        }
        
        
        template<typename Query>
        iterator lower_bound(const Query& query) const {
            cursor c = this->top();
            for (;;) {
                const _node_t* candidate = c._load(std::memory_order_acquire);
                if (!candidate || Compare()(query, candidate->_key)) {
                    // candidate is to the right of the entry
                    if (c.is_bottom())
                        // first element not less than query
                        return iterator{candidate};
                    // descend to a lower level
                    c.descend();
                } else if (Compare()(candidate->_key, query)) {
                    // candiate is to the left of the entry
                    // advance to along current level
                    c._next = candidate->_next;
                } else {
                    // we found the exact entry
                    return iterator{candidate};
                }
            }
        }
        
        template<typename Query>
        iterator upper_bound(const Query& query) const {
            cursor c = this->top();
            for (;;) {
                const _node_t* candidate = c._load(std::memory_order_acquire);
                if (!candidate || Compare()(query, candidate->_key)) {
                    // candidate is to the right of the entry
                    if (c.is_bottom())
                        // first element not less than query
                        return iterator{candidate};
                    // descend to a lower level
                    c.descend();
                } else {
                    // candidate is not to right of the entry
                    // (it may actually be an exact match, but we don't care)
                    // advance along current level
                    c._next = candidate->_next;
                }
            }
        }
        
    };
    
    
    
    // Concurrent skiplist supporting only find-or-emplace
    //
    // To read back the contents, irrevocably convert to an immutable
    // frozen_skiplist

    template<typename Key, typename Compare = std::less<Key>>
    struct concurrent_skiplist {
        
        struct _node_t;
        struct _array_t;
        
        struct _array_t {
            
            size_t _size;
            mutable std::atomic<const _node_t*> _data[0];
            
            explicit _array_t(size_t n) : _size(n) {}
            size_t size() const { return _size; }
            std::atomic<const _node_t*>& operator[](size_t index) const {
                assert(index < _size);
                return _data[index];
            }
            
            std::atomic<const _node_t*>* operator+(size_t index) const {
                assert(index < _size);
                return _data + index;
            }
            
        };
        
        struct _node_t {
            
            Key _key;
            _array_t _next;
            
            explicit _node_t(size_t n, auto&&... args)
            : _key(std::forward<decltype(args)>(args)...)
            , _next(n) {
            }
            
            size_t size() const {
                return _next.size();
            }
                        
            static _node_t* with_size_emplace(size_t n, auto&&... args) {
                void* raw = arena_allocate(sizeof(_node_t) + sizeof(std::atomic<_node_t*>) * n);
                return new(raw) _node_t(n, std::forward<decltype(args)>(args)...);
            }
                        
        };
        
        struct _head_t {
            
            mutable std::atomic<size_t> _top;
            _array_t _next;
            
            _head_t() : _top(1), _next(33) {
                std::memset(_next._data, 0, 33*88);
            }
            
            static _head_t* make() {
                size_t n = 33;
                void* raw = arena_allocate(sizeof(_head_t) + n * sizeof(std::atomic<const _node_t*>));
                return new(raw) _head_t;
            }
            
        };
        
        const _head_t* _head;
        

        concurrent_skiplist() : _head(_head_t::make()) {}
        
        static pair<const _node_t*, bool> _link_level(size_t level,
                                                      const _array_t* array,
                                                      const _node_t* expected,
                                                      const _node_t* desired) {
        alpha:
            assert(array && desired);
            assert(!expected || (desired->_key < expected->_key));
            desired->_next[level].store(expected, std::memory_order_release);
            if ((*array)[level].compare_exchange_strong(expected,
                                                        desired,
                                                        std::memory_order_release,
                                                        std::memory_order_acquire))
                return {desired, true};
        beta:
            if (!expected || (Compare()(desired->_key, expected->_key)))
                goto alpha;
            if (!(Compare()(expected->_key, desired->_key)))
                return {expected, false};
            array = &(expected->_next);
            expected = (*array)[level].load(std::memory_order_acquire);
            goto beta;
        }
        
        template<typename Query, typename... Args>
        static pair<const _node_t*, bool> _emplace(size_t max_level, size_t level,
                                                   const _array_t* array,
                                                   const Query& query,
                                                   Args&&... args) {
        alpha:
            const _node_t* candidate = (*array)[level].load(std::memory_order_acquire);
            if (!candidate || Compare()(query, candidate->_key))
                goto beta;
            if (!(Compare()(candidate->_key, query)))
                return std::pair(candidate, false);
            array = &(candidate->_next);
            goto alpha;
        beta:
            assert(!candidate || Compare()(query, candidate->_key));
            pair<const _node_t*, bool> result;
            if (level == 0) {
                // get (24) random bits
                uint_fast32_t x = (*thread_local_random_number_generator)();
                x |= x >> 12;
                x |= 1 << max_level;
                // the bottom 12 bits are now set with probability 0.75
                size_t n = 1 + __builtin_ctz(x);
                // each bit is zero with probability 0.25;
                // ctz counts the length of the run of such outcomes
                // this is an exponential distribution with p(k) = 3 * 4^{-n})
                // valid up to n = 12, which corresponds to a skiplists of order
                // 4^{12} = 2^{24} = 16 million elements
                
                // Both p=1/2 and 1/4 are close to 1/e, the value that
                // minimizes the expected number of comparisons.  1/4 will
                // use less memory than 1/2 at the cost of higher variance in
                // operation time.   As 1/4 uses less memory, it may benefit
                // from memory locality more.
                
                // printf("%zu\n", n);
                _node_t* p = _node_t::with_size_emplace(n, query, std::forward<decltype(args)>(args)...);
                // Link the lowest level
                result = _link_level(0, array, candidate, p);
                if (!result.second)
                    free(p);
            } else {
                // Recurse down to emplace
                result = _emplace(max_level, level - 1, array, query, std::forward<decltype(args)>(args)...);
                // On the way back up, link this level if appropriate
                if (result.second && (level < result.first->_next.size())) {
                    result = _link_level(level, array, candidate, result.first);
                    assert(result.second);
                }
            }
            return result;
        }
        
        template<typename Query, typename... Args>
        pair<const Key&, bool> emplace(const Query& query, Args&&... args) {
            assert(_head);
            size_t level = _head->_top.load(std::memory_order_relaxed);
            assert(level > 0);
            pair<const _node_t*, bool> result = _emplace(level, level - 1, &(_head->_next), query, std::forward<decltype(args)>(args)...);
            // max_level only increases so no need to re-read top
            if (result.second && result.first->size() > level) {
                // TODO: limit growth here to one level at a time
                // Needs to pass "top" down all the way (or read it again)
                __atomic_fetch_max((size_t*)&(_head->_top), result.first->size(), __ATOMIC_RELAXED);
                while (level < result.first->size()) {
                    _link_level(level, &(_head->_next), nullptr, result.first);
                    ++level;
                }
            }
            return { result.first->_key, result.second };
        }
        
        
        frozen_skiplist<Key, Compare> freeze() && {
            return frozen_skiplist<Key, Compare>{
                (const typename frozen_skiplist<Key, Compare>::_head_t*)_head
            };
        }
       
        
        
    }; // concurrent_skiplist<Key, Compare>
    
    
    template<typename T>
    struct _is_pair : std::integral_constant<bool, false> {};
    
    template<typename A, typename B>
    struct _is_pair<std::pair<A, B>> : std::integral_constant<bool, true> {};
    
    template<typename T>
    constexpr bool _is_pair_v = _is_pair<T>::value;
        
    
    template<typename Compare>
    struct CompareFirst {
        
        static decltype(auto) key_if_pair(auto&& query) {
            // TODO: make this a kind(?) comparison
            if constexpr (_is_pair_v<std::decay_t<decltype(query)>>) {
                return std::forward_like<decltype(query)>(query.first);
            } else {
                return std::forward<decltype(query)>(query);
            }
        }
        
        bool operator()(auto&& a, auto&& b) const {
            return Compare()(key_if_pair(std::forward<decltype(a)>(a)),
                             key_if_pair(std::forward<decltype(b)>(b)));
        }
        
    };

    
    
    template<typename Key, typename T, typename Compare = std::less<>>
    struct frozen_skiplist_map {
        
        using P = std::pair<Key, T>;
                
        using S = frozen_skiplist<P, CompareFirst<Compare>>;
        using iterator = S::iterator;
        using cursor = S::cursor;
        
        S _set;
        
        iterator begin() const {
            return _set.begin();
        }
        
        iterator end() const {
            return _set.end();
        }
        
        cursor top() const {
            return _set.top();
        }
        
        iterator find(auto&& query) const {
            return _set.find(std::forward<decltype(query)>(query));
        }
        
        iterator lower_bound(auto&& query) const {
            return _set.lower_bound(std::forward<decltype(query)>(query));
        }
        
        bool intersects_closed_range(const auto& a, const auto& b) const {
            return _set.intersects_closed_range(a, b);
        }
        
        
        std::pair<iterator, bool> emplace(auto&&... args) {
            return _set.emplace(std::forward<decltype(args)>(args)...);
        }
        
        const T& operator[](auto&& query) const {
            return _set.emplace(std::forward<decltype(query)>(query)).first->second;
        }
        
        T& operator[](auto&& query) {
            return _set.emplace(std::forward<decltype(query)>(query)).first->second;
        }
        
    }; // struct frozen_skiplist_map
    
    
    template<typename Key, typename T, typename Compare = std::less<>>
    struct concurrent_skiplist_map {
        
        using P = std::pair<Key, T>;
        
        using S = concurrent_skiplist<P, CompareFirst<Compare>>;
        S _set;
        
        std::pair<const P&, bool> emplace(auto&&... args) {
            return _set.emplace(std::forward<decltype(args)>(args)...);
        }
        
        const T& operator[](auto&& query) const {
            return _set.emplace(std::forward<decltype(query)>(query)).first->second;
        }
        
        T& operator[](auto&& query) {
            return _set.emplace(std::forward<decltype(query)>(query)).first->second;
        }
        
        frozen_skiplist_map<Key, T, Compare> freeze() && {
            frozen_skiplist_map<Key, T, Compare> a;
            using H = typename frozen_skiplist<P, CompareFirst<Compare>>::_head_t;
            a._set._head = (const H*) (_set._head);
            return a;
        }
        
    }; // struct concurrent_skiplist_map
    
} // namespace aaa

#endif /* skiplist_hpp */
