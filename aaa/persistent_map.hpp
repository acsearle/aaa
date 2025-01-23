//
//  persistent_map.hpp
//  aaa
//
//  Created by Antony Searle on 31/12/2024.
//

#ifndef persistent_map_hpp
#define persistent_map_hpp

#include <cassert>
#include <cstdint>
#include <cstdio>

#include <algorithm>
#include <bit>
#include <utility>

#include "object.hpp"
#include "latch.hpp"

namespace aaa {
    
    template<typename T>
    struct PersistentIntMap {
        
        struct Node  : gc::Object {
            uint64_t _prefix;
            int _shift;
            uint64_t _bitmap;
            union {
                const Node* _children[0];
                T _values[0];
            };
                                    
            void assert_invariant() const {
                assert(_shift >= 0);
                assert(_shift < 64);
                assert(~(_shift % 6));
                assert(!(_prefix & ((uint64_t)63 << _shift)));
                assert(_bitmap);
                if (_shift) {
                    for (uint64_t i = 0; i != 64; ++i) {
                        uint64_t j = (uint64_t)1 << i;
                        uint64_t expected_prefix = (_prefix >> _shift) | i;
                        if (j & _bitmap) {
                            int k = __builtin_popcountll((j - 1) & _bitmap);
                            const Node* p = _children[k];
                            assert((p->_prefix >> _shift) == expected_prefix);
                            p->assert_invariant();
                        }
                    }
                }
            }
            
            
            virtual void _object_scan() const override {
                if (_shift) {
                    int n = __builtin_popcountll(_bitmap);
                    for (int i = 0; i != n; ++i) {
                        _children[i]->_object_trace();
                    }
                }
            }
            
            Node(uint64_t prefix, int shift, uint64_t bitmap)
            : gc::Object()
            , _prefix(prefix)
            , _shift(shift)
            , _bitmap(bitmap) {
            }
            
            static Node* make(uint64_t prefix, int shift, uint64_t bitmap) {
                assert((shift >= 0) && (shift < 64) && !(shift % 6));
                assert(!(prefix & ~(~(uint64_t)63 << shift )));
                assert(bitmap);
                size_t n = (sizeof(Node)
                            + ((shift
                                ? sizeof(const Node*)
                                : sizeof(T))
                               * __builtin_popcountll(bitmap)));
                void* p = operator new(n);
                return new(p) Node(prefix, shift, bitmap);
            }
            
            static const Node* make_from_array(uint64_t prefix, int shift, uint64_t bitmap, const Node* const* array) {
                if (!bitmap)
                    // empty node
                    return nullptr;
                if (__builtin_popcountll(bitmap) == 1)
                    // only one child, use it directly
                    return array[__builtin_ctzll(bitmap)];
                Node* a = make(prefix, shift, bitmap);
                for (int k = 0; bitmap != 0; ++k) {
                    int i = __builtin_ctzll(bitmap);
                    a->_children[k] = array[i];
                    bitmap &= (bitmap - 1);
                }
                return a;
            }

            static Node* make_from_array(uint64_t prefix, uint64_t bitmap, const T* array) {
                if (!bitmap)
                    // empty node
                    return nullptr;
                Node* a = make(prefix, 0, bitmap);
                for (int k = 0; bitmap != 0; ++k) {
                    int i = __builtin_ctzll(bitmap);
                    a->_values[k] = array[i];
                    bitmap &= (bitmap - 1);
                }
                return a;
            }
            
            static const Node* make_from_nullable_array(uint64_t prefix, int shift, const Node* const* array) {
                uint64_t bitmap = 0;
                for (int i = 0; i != 64; ++i) {
                    if (array[i]) {
                        bitmap |= (uint64_t)1 << i;
                    }
                }
                return make_from_array(prefix, shift, bitmap, array);
            }

            static Node* make(uint64_t key, T value) {
                void* a = operator new(sizeof(Node) + sizeof(T));
                Node* p = new(a) Node{
                    key & ~(uint64_t)63,
                    0,
                    (uint64_t)1 << (key & 63)
                };
                p->_values[0] = std::move(value);
                return p;
            }
            
            static Node* make_with_two_children(const Node* p, const Node* q) {
                uint64_t delta = p->_prefix ^ q->_prefix;
                assert(delta);
                int new_shift = ((63 - __builtin_clzll(delta)) / 6) * 6;
                assert((new_shift >= 0) && (new_shift < 64) && !(new_shift % 6));
                assert(new_shift > p->_shift);
                assert(new_shift > q->_shift);
                assert(delta >> new_shift);
                assert(!(delta >> new_shift >> 6));
                uint64_t new_prefix = p->_prefix & (~(uint64_t)63 << new_shift);
                uint64_t i_p  = (p->_prefix >> new_shift) & 63;
                uint64_t i_q = ( q->_prefix >> new_shift) & 63;
                uint64_t j_p  = (uint64_t)1 << i_p ;
                uint64_t j_q = (uint64_t)1 << i_q;
                uint64_t new_bitmap = j_p | j_q;
                Node* b = Node::make(new_prefix, new_shift, new_bitmap);
                int k_p = __builtin_popcountll((j_p - 1) & new_bitmap);
                int k_q = __builtin_popcountll((j_q - 1) & new_bitmap);
                b->_children[k_p] = p;
                b->_children[k_q] = q;
                return b;
            }
            
            Node* clone_and_insert_or_replace_child(const Node* child) const {
                assert(_shift);
                assert(child->_shift < _shift);
                assert(!((child->_prefix ^ _prefix) >> _shift >> 6));
                uint64_t i = (child->_prefix >> _shift) & (uint64_t)63;
                uint64_t j = (uint64_t)1 << i;
                int k = __builtin_popcountll((j - 1) & _bitmap);
                uint64_t new_bitmap = _bitmap | j;
                Node* b = Node::make(_prefix, _shift, new_bitmap);
                int c = 0, d = 0;
                int old_count = __builtin_popcountll(_bitmap);
                for (; c != k;)
                    b->_children[d++] = _children[c++];
                if (_bitmap & j)
                    c++;
                b->_children[d++] = child;
                for (; c != old_count;)
                    b->_children[d++] = _children[c++];
                return b;
            }
            
            Node* clone_and_insert_or_replace_value(uint64_t key, T value) const {
                assert(_shift == 0);
                assert(!((key ^ _prefix) >> 6));
                uint64_t i = key & (uint64_t)63;
                uint64_t j = (uint64_t)1 << i;
                int k = __builtin_popcountll((j - 1) & _bitmap);
                uint64_t new_bitmap = _bitmap | j;
                Node* b = Node::make(_prefix, _shift, new_bitmap);
                int c = 0, d = 0;
                int old_count = __builtin_popcountll(_bitmap);
                for (; c != k;)
                    b->_values[d++] = _values[c++];
                if (_bitmap & j)
                    c++;
                b->_values[d++] = std::move(value);
                for (; c != old_count;)
                    b->_values[d++] = _values[c++];
                return b;
            }
            
            const Node* clone_and_erase_prefix(uint64_t prefix) const {
                uint64_t i = (prefix >> _shift) & (uint64_t)63;
                uint64_t j = (uint64_t)1 << i;
                if (!(_bitmap & j))
                    // prefix is not present
                    return this;
                uint64_t new_bitmap = _bitmap ^ j;
                if (!new_bitmap)
                    // erased last entry
                    return nullptr;
                int k = __builtin_popcount((j - 1) & _bitmap);
                if (_shift && std::has_single_bit(new_bitmap)) {
                    // erased last sibling, collapse this level
                    return _children[k ^ 1];
                }
                Node* b = Node::make(_prefix, _shift, new_bitmap);
                int old_count = __builtin_popcountll(_bitmap);
                assert(old_count);
                assert(k < old_count);
                int c = 0, d = 0;
                if (_shift) {
                    for (; c != k;)
                        b->_children[d++] = _children[c++];
                    c++;
                    for (; c != old_count;)
                        b->_children[d++] = _children[c++];
                } else {
                    for (; c != k;)
                        b->_values[d++] = _values[c++];
                    c++;
                    for (; c != old_count;)
                        b->_values[d++] = _values[c++];
                }
            }
            
            bool contains(uint64_t key) const {
                if ((_prefix ^ key) >> _shift >> 6)
                    return false; // prefix excludes the key
                uint64_t i = (key >> _shift) & (uint64_t)63;
                uint64_t j = (uint64_t)1 << i;
                if (!(_bitmap & j))
                    return false; // bitmap excludes the key
                int k = __builtin_popcountll((j - 1) & _bitmap);
                return !_shift || _children[k]->contains(key);
            }
            
            bool try_find(uint64_t key, T& victim) const {
                if ((_prefix ^ key) >> _shift >> 6)
                    return false; // prefix excludes the key
                uint64_t i = (key >> _shift) & (uint64_t)63;
                uint64_t j = (uint64_t)1 << i;
                if (!(_bitmap & j))
                    return false; // bitmap excludes the key
                int k = __builtin_popcountll((j - 1) & _bitmap);
                if (_shift) {
                    return _children[k]->try_find(key, victim);
                } else {
                    victim = _values[k];
                    return true;
                }
            }
            
            
            
            const Node* insert_or_replace(uint64_t key, T value) const {
                if (!((_prefix ^ key) >> _shift >> 6)) {
                    // the prefix matches
                    // we need to return an altered version of this Node
                    uint64_t i = (key >> _shift) & 63;
                    uint64_t j = (uint64_t)1 << i;
                    int k = __builtin_popcountll((j - 1) & _bitmap);
                    if (_shift) {
                        return clone_and_insert_or_replace_child(_bitmap & j
                                                      ? _children[k]->insert_or_replace(key, value)
                                                      : make(key, std::move(value)));
                    } else {
                        return clone_and_insert_or_replace_value(key, value);
                    }
                } else {
                    return make_with_two_children(this, make(key, value));
                }
            }
            
            /*
            const Node* erase(uint64_t key) const {
                if ((_prefix ^ key) >> _shift >> 6)
                    return false; // prefix excludes the key
                uint64_t i = (key >> _shift) & 63;
                uint64_t j = (uint64_t)1 << i;
                if (!(_bitmap & j))
                    return false; // bitmap excludes the key
                int k = __builtin_popcountll((j - 1) & _bitmap);
                if (_shift) {
                    const Node* b = _children[k]->erase(key);
                    if (b == this) {
                        return this;
                    }
                    return
                }


                if (_shift) {
                    return _children[k]->try_find(key, victim);
                } else {
                    victim = _values[k];
                    return true;
                }

            }
             */
            
            
            
            static const Node* merge_left(const Node* a, const Node* b) {
                // handle the trivial cases
                if (!b)
                    return a;
                if (!a)
                    return b;
                assert(a && b);
                
                // form the difference of the prefixes
                uint64_t delta = a->_prefix ^ b->_prefix;
                if (delta >> std::max(a->_shift, b->_shift) >> 6) {
                    // the prefixes differ above the level covered by the nodes
                    // this means the trees cover disjoint ranges of keys
                    // to merge them we just need to make a new parent
                    return Node::make_with_two_children(a, b);
                }
                
                // the prefixes are consistent above at least one of the nodes
                // the merge is either parent-adopts-child or merge-siblings

                if (a->_shift != b->_shift) {
                    // parent-adopts-child
                    // we need to handle the two cases separately maintain the left-right ordering
                    if (a->_shift > b->_shift) {
                        // a is a parent of b
                        uint64_t i = (b->_prefix >> a->_shift) & (uint64_t)63;
                        uint64_t j = (uint64_t)1 << i;
                        int k = __builtin_popcountll((j - 1) & a->_bitmap);
                        if (a->_bitmap & j)
                            b = merge_left(a->_children[k], b);
                        return a->clone_and_insert_or_replace_child(b);
                    } else {
                        assert(b->_shift > a->_shift);
                        // b is a parent of a
                        uint64_t i = (a->_prefix >> b->_shift) & (uint64_t)63;
                        uint64_t j = (uint64_t)1 << i;
                        int k = __builtin_popcountll((j - 1) & b->_bitmap);
                        if (b->_bitmap & j)
                            a = merge_left(a, b->_children[k]);
                        return b->clone_and_insert_or_replace_child(a);
                    }
                } else {
                    // merge-siblings
                    assert(a->_shift == b->_shift);
                    assert(a->_prefix == b->_prefix);
                    
                    uint64_t new_bitmap = a->_bitmap | b->_bitmap;
                    Node* c = Node::make(a->_prefix, a->_shift, new_bitmap);
                    // now form the new children
                    int k_a = 0, k_b = 0, k_c = 0;
                    for (; new_bitmap;) {
                        uint64_t j = new_bitmap & -new_bitmap;
                        new_bitmap ^= j;

                        assert(k_a == __builtin_popcountll((j-1) & a->_bitmap));
                        assert(k_b == __builtin_popcountll((j-1) & b->_bitmap));
                        assert(k_c == __builtin_popcountll((j-1) & c->_bitmap));
                        int i = __builtin_ctzll(j);
                        assert(j == (uint64_t)1 << i);
                        if (a->_shift) {
                            if (a->_bitmap & j) {
                                assert(((a->_children[k_a]->_prefix >> c->_shift) & 63) == i);
                            }
                            if (b->_bitmap & j) {
                                assert(((b->_children[k_b]->_prefix >> c->_shift) & 63) == i);
                            }
                        }
                        // TODO: DRY
                        if (a->_shift) {
                            if (a->_bitmap & b->_bitmap & j) {
                                c->_children[k_c++] = merge_left(a->_children[k_a++], b->_children[k_b++]);
                            } else if (a->_bitmap & j) {
                                c->_children[k_c++] = a->_children[k_a++];
                            } else if (b->_bitmap & j) {
                                c->_children[k_c++] = b->_children[k_b++];
                            }
                        } else {
                            if (a->_bitmap & b->_bitmap & j) {
                                c->_values[k_c++] = a->_values[k_a++]; // take-left
                                k_b++;
                            } else if (a->_bitmap & j) {
                                c->_values[k_c++] = a->_values[k_a++];
                            } else if (b->_bitmap & j) {
                                c->_values[k_c++] = b->_values[k_b++];
                            }
                        }
                    }
                    return c;
                }
            }
            
            static const Node* node_for_closed_range(const Node* node, uint64_t key_low, uint64_t key_high) {
                assert(key_low <= key_high);
                for (;;) {
                    assert(node);
                    uint64_t a = key_low >> node->_shift;
                    uint64_t b = key_high >> node->_shift;
                    if ((a != b) || !node->_shift) {
                        uint64_t ia = a & (uint64_t)63;
                        uint64_t ib = b & (uint64_t)63;
                        uint64_t j = (~(uint64_t)0 << ia) ^ (~(uint64_t)1 << ib);
                        return node->_bitmap & j ? node : nullptr;
                    } else {
                        assert(a == b);
                        assert(node->_shift);
                        uint64_t i = a & (uint64_t)63;
                        uint64_t j = (uint64_t)1 << i;
                        if (!(node->_bitmap & j))
                            return nullptr;
                        int k = __builtin_popcountll((j - 1) & node->_bitmap);
                        node = node->_children[k];
                    }
                }
            }
            
            void print() const {
                printf("{\n");
                printf("  _prefix:%llx,\n", _prefix);
                printf("  _shift:%d,\n", _shift);
                int n = __builtin_popcountll(_bitmap);
                printf("  _bitmap:%llx (%d),\n", _bitmap, n);
                if (_shift) {
                    printf("  _children:[");
                } else {
                    printf("  _values:[");
                }
                int k = 0;
                for (uint64_t i = 0; i != 64; ++i) {
                    uint64_t j = (uint64_t)1 << i;
                    uint64_t key = _prefix | (i << _shift);
                    if (_bitmap & j) {
                        if (_shift) {
                            printf(" %llx:%p,", key, _children[k++]);
                        } else {
                            printf(" %llx:%llx,", key, _values[k++]);
                        }
                    }
                }
                printf("]\n");
                printf("}\n");
            }
            
            virtual void _object_debug() const override final {
                print();
            }
            
        };
        
        static_assert(sizeof(Node) == 40);
        
        const Node* _root = nullptr;
        
        bool try_find(uint64_t key, T& victim) {
            return _root && _root->try_find(key, victim);
        }
        
        void insert_or_replace(uint64_t key, T value) {
            _root = (_root
                     ? _root->insert_or_replace(key, std::move(value))
                     : Node::make(key, std::move(value))
                     );
        }
        
        PersistentIntMap submap_for_closed_range(uint64_t key_low, uint64_t key_high) {
            return PersistentIntMap{
                _root
                ? Node::node_for_closed_range(_root, key_low, key_high)
                : nullptr
            };
        }
                
    }; // PersistentMap
    
    
    
    template<typename T>
    PersistentIntMap<T> merge_left(PersistentIntMap<T> a, PersistentIntMap<T> b) {
        return PersistentIntMap<T>{PersistentIntMap<T>::Node::merge_left(a._root, b._root)};
    }

    
    
    
    
    
    template<typename T>
    latch::signalling_coroutine
    parallel_merge_left(latch& outer, // <-- used by coroutine promise
                        const typename PersistentIntMap<T>::Node* a,
                        const typename PersistentIntMap<T>::Node* b,
                        const typename PersistentIntMap<T>::Node** target
                        ) {
        using U = PersistentIntMap<T>::Node;
        
        if (!b) {
            // trivial-left
            *target = a;
        } else if (!a) {
            // trivial-right
            *target = b;
        } else if ((a->_prefix ^ b->_prefix) >> std::max(a->_shift, b->_shift) >> 6) {
            // trivially-disjoint
            *target = U::make_with_two_children(a, b);
        } else if (a->_shift == b->_shift) {
            // merge-siblings
            if (a->_shift == 0) {
                // leaf-merge - not parallel
                assert(b->_shift == 0);
                *target = U::merge_left(a, b);
            } else {
                // branch-merge - spawn tasks to resolve each prefix collision
                uint64_t common = a->_bitmap & b->_bitmap;
                // SingleConsumerCountdownEvent event{__builtin_popcountll(common)};
                latch inner;
                const U* results[64] = {};
                int k_a = 0; int k_b = 0;
                int n_a = __builtin_popcountll(a->_bitmap);
                int n_b = __builtin_popcountll(b->_bitmap);
                for (int i = 0; i != 64; ++i) {
                    uint64_t j = (uint64_t)1 << i;
                    assert(k_a <= n_a);
                    assert(k_b <= n_b);
                    if (j & common) {
                        assert(k_a < n_a);
                        assert(k_b < n_b);
                        parallel_merge_left<T>(inner,
                                               a->_children[k_a++],
                                               b->_children[k_b++],
                                               results + i);
                    } else if (j & a->_bitmap) {
                        assert(k_a < n_a);
                        assert(!(j & b->_bitmap));
                        results[i] = a->_children[k_a++];
                    } else if (j & b->_bitmap) {
                        assert(k_b < n_b);
                        assert(!(j & a->_bitmap));
                        results[i] = b->_children[k_b++];
                    }
                }
                co_await inner;
                // gather results
                assert(a->_prefix == b->_prefix);
                assert(a->_shift == b->_shift);
                uint64_t new_bitmap = a->_bitmap | b->_bitmap;
                *target = U::make_from_array(a->_prefix, a->_shift, new_bitmap, results);
            }
        } else if (b->_shift < a->_shift) {
            // adopt-parent-child
            uintptr_t i = (b->_prefix >> a->_shift) & (uint64_t)63;
            uintptr_t j = (uint64_t)1 << i;
            uintptr_t k = __builtin_popcountll((j - 1) & a->_bitmap);
            const U* d = nullptr;
            if (j & a->_bitmap) {
                // we must merge
                // TODO: this is a trivial fork-join that should be done
                // inline, but to do so we need to make parallel_merge_left
                // generic over its execution policies
                latch inner;
                parallel_merge_left<T>(inner, a->_children[k], b, &d); // <-- respect order
                co_await inner;
            } else {
                d = b;
            }
            *target = a->clone_and_insert_or_replace_child(d);
        } else {
            // adopt-child-parent
            assert(a->_shift < b->_shift);
            uintptr_t i = (a->_prefix >> b->_shift) & (uint64_t)63;
            uintptr_t j = (uint64_t)1 << i;
            uintptr_t k = __builtin_popcountll((j - 1) & b->_bitmap);
            const U* d = nullptr;
            if (j & b->_bitmap) {
                // we must merge
                latch inner;
                parallel_merge_left<T>(inner, a, b->_children[k], &d); // <-- respect order
                co_await inner;
            } else {
                d = a;
            }
            *target = b->clone_and_insert_or_replace_child(d);
        }
        // outer latch is signaled when coroutine completes
    }
    

    inline PersistentIntMap<uint64_t> sneaky;
    
    template<typename T>
    co_void parallel_merge_left(PersistentIntMap<T> a, PersistentIntMap<T> b, PersistentIntMap<T>& c) {
        printf("%s\n", __PRETTY_FUNCTION__);
        latch inner;
        parallel_merge_left<T>(inner, a._root, b._root, &c._root);
        co_await inner;
        for (int i = 0; i != 10; ++i) {
            // work_queues[i].mark_done();
        }
    }
    
    
    
    
    

    
    
} // namespace aaa


#endif /* persistent_map_hpp */
