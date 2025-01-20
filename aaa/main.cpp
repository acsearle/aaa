//
//  main.cpp
//  aaa
//
//  Created by Antony Searle on 29/12/2024.
//

// C
#include <cassert>
#include <cstdlib>

// C++
#include <atomic>
#include <map>
#include <random>
#include <thread>
#include <utility>
#include <vector>
#include <algorithm>


#include "awaitable.hpp"
#include "bag.hpp"
#include "concurrent_queue.hpp"
#include "latch.hpp"
#include "persistent_map.hpp"
#include "skiplist.hpp"

namespace aaa {
    
    std::atomic<bool> q_done = false;
            
    void worker_entry(int index) {
        int count = 0;
        tlq_index = index;
        arena_initialize();
        try {
            for (;;) {
                std::coroutine_handle<> work = nullptr;
                // try the back of our own queue
                for (int i = 0;; ++i) {
                    int j = (index + i) % 10;
                    auto& q = work_queues[j];
                    if ((j == index) ? q.pop(work) : q.steal(work))
                        break;
                    if (q_done.load(std::memory_order_acquire))
                        throw ConcurrentQueue2<std::coroutine_handle<>>::done_exception{};
                }
                work.resume();
                ++count;
            }
        } catch(ConcurrentQueue2<std::coroutine_handle<>>::done_exception) {
            printf("thread did %d jobs\n", count);
        };
        arena_finalize();
    }
        
    
    // basic/slow/simple/serial skiplist to trie
    template<typename T>
    PersistentIntMap<T> sync_persist_skiplist(typename frozen_skiplist_map<uint64_t, T>::cursor a,
                          uint64_t key_low,
                          uint64_t key_high) {
        PersistentIntMap<T> c;
        auto aa = a.as_iterator();
        assert(aa);
        for (;; ++aa) {
            if (!aa)
                break;
            const std::pair<uint64_t, T>& kv = *aa;
            if (kv.first > key_high)
                break;
            if (kv.first < key_low)
                continue;
            printf("emplacing %llx:%llx\n", kv.first, kv.second);
            c.insert_or_replace(kv.first, kv.second);
        }
        return c;
    }
    
    
    // async/parallel skiplist to trie
    template<typename T>
    latch::signalling_coroutine
    async_persist_skiplist(latch&, // <-- signalled by coroutine promise
                     typename frozen_skiplist_map<uint64_t, T>::cursor a,
                     const typename PersistentIntMap<T>::Node** target,
                     uint64_t outer_key_low,
                     uint64_t outer_key_high) {
        
        using U = PersistentIntMap<T>::Node;
        
        assert(target);
        assert(outer_key_low <= outer_key_high);

        // find the common prefix
        uint64_t delta = outer_key_low ^ outer_key_high;
        assert(delta);
        int new_shift = ((63 - __builtin_clzll(delta)) / 6) * 6;
        uint64_t new_prefix = outer_key_low & (~(uint64_t)63 << new_shift);
        assert(!((outer_key_low ^ new_prefix) >> new_shift >> 6));
        assert(!((outer_key_high ^ new_prefix) >> new_shift >> 6));
                
        // handle the situation where the chunks don't neatly divide the word
        uint64_t imax = ((uint64_t)63 << new_shift >> new_shift) + 1;
                
        if (new_shift) {
            const U* results[64] = {};
            latch inner;
            for (uint64_t i = 0; i != imax; ++i) {
                uint64_t key_low = new_prefix | (i << new_shift);
                uint64_t key_high = new_prefix | ~(~i << new_shift);
                assert(key_low <= key_high);
                assert(key_low >= outer_key_low);
                assert(key_high <= outer_key_high);
                auto c = a;
                bool in_b = c.refine_closed_range(key_low, key_high);
                if (in_b)
                    async_persist_skiplist<T>(inner, c, results + i, key_low, key_high);
            }
            co_await inner;
            *target = U::make_from_nullable_array(new_prefix, new_shift, results);
        } else {
            assert(new_shift == 0);
            T results[64] = {};
            uint64_t new_bitmap = 0;
            for (uint64_t i = 0; i != imax; ++i) {
                uint64_t key = new_prefix | i;
                assert(key >= outer_key_low);
                assert(key <= outer_key_high);
                auto b = a.find(key);
                if (b) {
                    assert(b->first == key);
                    results[i] = b->second;
                    new_bitmap |= (uint64_t)1 << i;
                }
            }
            *target = U::make_from_array(new_prefix, new_bitmap, results);
        }
    }
    
    
    
    
    
    template<typename T>
    latch::signalling_coroutine
    parallel_merge_right(latch&, // <-- signalled by coroutine promise
                        const typename PersistentIntMap<T>::Node* a,
                        typename frozen_skiplist_map<uint64_t, T>::cursor b, // <-- points before the key range
                        const typename PersistentIntMap<T>::Node** target,
                        uint64_t outer_key_low, // <-- inclusive range of keys to handle
                        uint64_t outer_key_high
                        ) {
        using U = PersistentIntMap<T>::Node;
        using C = frozen_skiplist_map<uint64_t, T>::cursor;
        
        //printf("keyrange: [%llx, %llx]\n", outer_key_low, outer_key_high);
        
        {
            auto p = (*b._next)[0];
            auto q = (*b._next)[b._level];
          //  printf("b-cursor points to %llx, %llx\n", p->_key.first, q->_key.second);
        }


        {
            // process the skiplist cursor to match the desired range
            // TODO: this is probably already guaranteed by the caller
            bool nonempty = b.refine_closed_range(outer_key_low, outer_key_high);
            if (!nonempty) {
               // printf("skiplist empty over [%llx, %llx]\n", outer_key_low, outer_key_high);
                // nothing in the skiplist, reuse the trie
                *target = a;
                co_return;
            }
        }
        
        if (!a) {
            // TODO: this is probably already guaranteed by the caller
            // nothing in the trie, process the skiplist
            // TODO: this should be a parallel job
            //printf("PersistentIntMap empty over  [%llx, %llx]\n", outer_key_low, outer_key_high);
            // *target = persistent_int_map_from_frozen_skiplist_map_cursor_range<T>(b, outer_key_low, outer_key_high)._root;
            latch inner;
            async_persist_skiplist<T>(inner, b, target, outer_key_low, outer_key_high);
            co_await inner;
            co_return;
        }
        
        // the skiplist and the trie both have some elements in the keyrange
        
        // TODO: handle the case that the keyrange does not match the range
        // implied by the prefix of a
        
        assert(a);
        //printf("prefix  :  %llx\n", a->_prefix);
        //printf("prefix^ :  %llx\n", a->_prefix + ~(~(uint64_t)63 << a->_shift));

        uint64_t a_low = a->_prefix;
        uint64_t a_high = a->_prefix + ~(~(uint64_t)63 << a->_shift);
        assert(outer_key_low <= a_low);
        assert(outer_key_high >= a_high);

        if ((outer_key_low < a_low) || (outer_key_high > a_high)) {
            
            // printf("sinister considers    %llx-%llx\n", outer_key_low, outer_key_high);

            // TODO: most of this code is shared with the shift != 0 path
                        
            uint64_t delta = outer_key_low ^ outer_key_high;
            assert(delta);
            int new_shift = ((63 - __builtin_clzll(delta)) / 6) * 6;
            assert(new_shift > a->_shift);
            uint64_t new_prefix = outer_key_low & (~(uint64_t)63 << new_shift);
            assert(!((outer_key_low ^ new_prefix) >> new_shift >> 6));
            assert(!((outer_key_high ^ new_prefix) >> new_shift >> 6));
            assert(!((a->_prefix ^ new_prefix) >> new_shift >> 6));

            uint64_t ia = (a->_prefix >> new_shift) & 63;
            
            uint64_t imax = ((uint64_t)63 << new_shift >> new_shift) + 1;
            
            latch inner;
            const U* results[64] = {};
                                    
            
            //printf("before loop\n");
            
            for (uint64_t i = 0; i != imax; ++i) {
                                
                uint64_t key_low = new_prefix | (i << new_shift);
                uint64_t key_high = new_prefix | ~(~i << new_shift);
                
               //  printf("sinister iterate      %llx-%llx with %llu\n", key_low, key_high, i);


                bool in_a = (i == ia);
                
                auto c = b;
                bool in_b = c.refine_closed_range(key_low, key_high);

                if (in_a && !in_b) {
                    //printf("sinister persist  for %llx-%llx\n", key_low, key_high);
                    results[i] = a;
                } else if (!in_a && in_b) {
                    //printf("sinister skiplist for %llx-%llx\n", key_low, key_high);
                    // results[i] = persistent_int_map_from_frozen_skiplist_map_cursor_range<T>(c, key_low, key_high)._root;
                    async_persist_skiplist<T>(inner, c, results + i, key_low, key_high);
                } else if (in_a && in_b) {
                    //printf("sinister common   for %llx-%llx\n", key_low, key_high);
                    parallel_merge_right<T>(inner, a, c, results + i, key_low, key_high);
                } else {
                    assert(!in_a && !in_b);
                    // printf("sinister nothing  for %llx-%llx\n", key_low, key_high);
                }

            }
            
            co_await inner;
            *target = U::make_from_nullable_array(new_prefix, new_shift, results);

        } else if (a->_shift) {
            // not at bottom
            uint64_t imax = ((uint64_t)63 << a->_shift >> a->_shift) + 1;

            latch inner;
            const U* results[64] = {};
            uint64_t k = 0;
            for (uint64_t i = 0; i != imax; ++i) {
                uint64_t j = (uint64_t)1 << i;
                uint64_t key_low = a->_prefix | (i << a->_shift);
                uint64_t key_high = a->_prefix | ~(~i << a->_shift);
                //printf("%llx-%llx under consideration\n", key_low, key_high);

                bool in_a = j & a->_bitmap;
                auto it_b = b.as_iterator();
                //if (it_b)
                  //  printf("it_b has key %llx\n", it_b->first);
                
                auto c = b;
                bool in_b = c.refine_closed_range(key_low, key_high);
                auto it_c = c.as_iterator();
                //if (it_c)
                  //  printf("it_c has key %llx\n", it_c->first);
                if (in_a && !in_b) {
                    //printf("%llx-%llx from pim\n", key_low, key_high);
                    results[i] = a->_children[k++];
                } else if (!in_a && in_b) {
                    //printf("%llx-%llx from fsm\n", key_low, key_high);
                    // results[i] = persistent_int_map_from_frozen_skiplist_map_cursor_range<T>(c, key_low, key_high)._root;
                    async_persist_skiplist<T>(inner, c, results + i, key_low, key_high);

                } else if (in_a && in_b) {
                    //printf("%llx-%llx from merge_right\n", key_low, key_high);
                    parallel_merge_right<T>(inner, a->_children[k++], c, results + i, key_low, key_high);
                }
            }

            
            co_await inner;
            *target = U::make_from_nullable_array(a->_prefix, a->_shift, results);

        } else {
            assert(a->_shift == 0);
            uint64_t new_bitmap = 0;
            T results[64] = {};
            uint64_t k = 0;
            for (uint64_t i = 0; i != 64; ++i) {
                uint64_t j = (uint64_t)1 << i;
                uint64_t key = a->_prefix | i;
                bool in_a = j & a->_bitmap;
                // TODO: clumsy
                auto p = b.lower_bound(key);
                bool in_b = p && (p->first == key);
                if (in_a) {
                    results[i] = a->_values[k++];
                    new_bitmap |= j;
                }
                if (in_b) {
                    results[i] = p->second;
                    new_bitmap |= j;
                }
            }
            
            *target = U::make_from_array(a->_prefix, new_bitmap, results);
        }
    }
    
    template<typename T>
    co_void parallel_merge_right(PersistentIntMap<T> a, frozen_skiplist_map<uint64_t, T> b, PersistentIntMap<T>& c) {
        printf("%s\n", __PRETTY_FUNCTION__);
        latch inner;
        parallel_merge_right<T>(inner,
                                a._root,
                                b.top(),
                                &c._root,
                                (uint64_t)0,
                                ~(uint64_t)0);
        co_await inner;
        //for (int i = 0; i != 10; ++i) {
            // work_queues[i].mark_done();
        //}
        q_done.store(true, std::memory_order_release);
    }
    
    
    
    template<typename T, typename F>
    latch::signalling_coroutine parallel_persist_generate(latch& outer,
                                                          const typename PersistentIntMap<T>::Node** target,
                                                          uint64_t outer_key_low,
                                                          uint64_t outer_key_high,
                                                          const F& f)
                                                          
    {
        using U = PersistentIntMap<T>::Node;
        
        assert(target);
        assert(outer_key_low <= outer_key_high);
        
        // find the common prefix
        uint64_t delta = outer_key_low ^ outer_key_high;
        assert(delta);
        int new_shift = ((63 - __builtin_clzll(delta)) / 6) * 6;
        uint64_t new_prefix = outer_key_low & (~(uint64_t)63 << new_shift);
        assert(!((outer_key_low ^ new_prefix) >> new_shift >> 6));
        assert(!((outer_key_high ^ new_prefix) >> new_shift >> 6));
        
        // handle the situation where the chunks don't neatly divide the word
        uint64_t imax = ((uint64_t)63 << new_shift >> new_shift) + 1;
        
        latch inner;
        
        if (new_shift) {
            const U* results[64] = {};
            for (uint64_t i = 0; i != imax; ++i) {
                
                uint64_t key_low = new_prefix | (i << new_shift);
                uint64_t key_high = new_prefix | ~(~i << new_shift);
                
                assert(key_low <= key_high);
                // assert(key_low >= outer_key_low);
                if (key_low > outer_key_high)
                    continue;
                // assert(key_high <= outer_key_high);
                if (key_high < outer_key_low)
                    continue;
                parallel_persist_generate<T, F>(inner, results + i,
                                                std::max(key_low, outer_key_low),
                                                std::min(key_high, outer_key_high), f);
            }
            
            co_await inner;
            *target = U::make_from_nullable_array(new_prefix, new_shift, results);
            co_return;

        } else {
            assert(new_shift == 0);
            T results[64] = {};
            uint64_t new_bitmap = 0;
            for (uint64_t i = 0; i != imax; ++i) {
                uint64_t key = new_prefix | i;
                assert(key >= outer_key_low);
                assert(key <= outer_key_high);
                results[i] = f(key);
                new_bitmap |= (uint64_t)1 << i;
            }
            *target = U::make_from_array(new_prefix, new_bitmap, results);
            co_return;
        }
        
        
    }
    
    
    template<typename T, typename F>
    co_void parallel_persist_generate_outer(PersistentIntMap<T>* target,
                                            uint64_t outer_key_low,
                                            uint64_t outer_key_high,
                                            const F& f) {
        latch inner;
        parallel_persist_generate<T, F>(inner, &(target->_root), outer_key_low, outer_key_high, f);
        co_await inner;
        //for (int i = 0; i != 10; ++i) {
            // work_queues[i].mark_done();
        //}
        q_done.store(true, std::memory_order_release);

    }
    
    
    
    void test3() {
        
        // manual initialization of global services
        tlq_index = 0; // work queue
        arena_initialize(); // bump allocator
        thread_local_random_number_generator = new std::ranlux24_base;
        
        
        // rudimentary thread pool
        std::vector<std::thread> threads;
        
        int thread_count = 10; // std::thread::hardware_concurrency();
        for (int i = 1; i != thread_count; ++i)
            threads.emplace_back(worker_entry, i);
        
        PersistentIntMap<uint64_t> a;
        
        parallel_persist_generate_outer(&a, 0, 1024 * 1024 * 32 - 1, [](uint64_t x) {
            return x + 1;
        });
                
        for (auto&& t : threads)
            t.join();
        

        // a._root->print();

        
//        for (uint64_t key = 0; key != 1024 * 1024 * 16; ++key) {
//            uint64_t value;
//            bool flag = a.try_find(key, value);
//            printf("\"%llx\" : %llx,\n", key, value);
//            assert(flag);
//            assert(value == key + 1);
//            
            // 308.5 Mb
            
            // We have stored 2**24 * 8 bytes
            // which is 134,217,728 bytes
            // Overhead on these full nodes is only 3 / 64
            
            // However, each coroutine is at least as big as the node it creates
            // (which is actually unnecessary, derp), which gets us up to "lots"
        
        
            
            
//        }
        
    }
    
        
    void test() {
        
        tlq_index = 0;
        
        arena_initialize();
        thread_local_random_number_generator = new std::ranlux24_base;
        
        
       

        

        uint64_t N = 100000000 / 1000;
        uint64_t M = 1000000 / 1000; // <-- sparseifier
        PersistentIntMap<uint64_t> a;
        PersistentIntMap<uint64_t> b;
        PersistentIntMap<uint64_t> c;
        
        concurrent_skiplist_map<uint64_t, uint64_t> z;
        frozen_skiplist_map<uint64_t, uint64_t> y;
                
            
        {
            std::mt19937 prng{std::random_device{}()};
            std::uniform_int_distribution<uint64_t> p{0, N-1};
            
            for (uint64_t i = 0; i != M; ++i) {
                uint64_t j = p(prng);
                uint64_t k = p(prng);
                a.insert_or_replace(j, k);
                b.insert_or_replace(k, j);
            }

            // copy a into z
            for (uint64_t key = 0; key != N; ++key) {
                uint64_t value_a = 0;
                bool flag_a = a.try_find(key, value_a);
                if (flag_a)
                    z.emplace(key, value_a);
            }

            
            a._root->assert_invariant();
            b._root->assert_invariant();

            c = merge_left(a, b);
            
            y = std::move(z).freeze();
            
            // validate
            for (uint64_t key = 0; key != N; ++key) {
                uint64_t value_a = 0;
                uint64_t value_b = 0;
                uint64_t value_c = 0;
                bool flag_a = a.try_find(key, value_a);
                bool flag_b = b.try_find(key, value_b);
                bool flag_c = c.try_find(key, value_c);
                auto it_y = y.find(key);
                if (flag_a) {
                    assert(flag_c);
                }
                if (flag_b) {
                    assert(flag_c);
                }
                if (flag_c) {
                    if (!flag_a) {
                        assert(flag_b);
                        assert(value_c == value_b);
                    } else {
                        assert(value_c == value_a);
                    }
                        
                }
                assert(!!it_y == flag_a);
                if (it_y) {
                    assert(it_y->second == value_a);
                }

            }
            
        }
        
        std::vector<std::thread> threads;
        
        int thread_count = 10; // std::thread::hardware_concurrency();
        for (int i = 1; i != thread_count; ++i)
            threads.emplace_back(worker_entry, i);
        
        PersistentIntMap<uint64_t> d;
        {
            // parallel_merge_left(a, b);
            //printf("parallel merge_right\n");
            parallel_merge_right<uint64_t>(b, y, d);
        }
        
        // main thread blocks until all threads join
        for (auto&& t : threads)
            t.join();
    
        // d should now contain merge_right(b, y) (aka y wins collisions)
    
        {
            
            // validate the parallel merge is the same as the serial merge
            for (uint64_t key = 0; key != N; ++key) {
                uint64_t value_a = 0;
                uint64_t value_b = 0;
                uint64_t value_c = 0;
                uint64_t value_d = 0;
                uint64_t value_y = 0;
                bool flag_a = a.try_find(key, value_a);
                bool flag_b = b.try_find(key, value_b);
                bool flag_c = c.try_find(key, value_c);
                bool flag_d = d.try_find(key, value_d);
                auto it_y = y.find(key);
                bool flag_y = it_y && (it_y->first == key);
                if (flag_y) value_y = it_y->second;
                bool f = flag_a || flag_b || flag_c || flag_d || flag_y;
//                if (f) printf("%llx :", key);
//                if (flag_a) printf(" (a : %llx),", value_a);
//                if (flag_b) printf(" (b : %llx),", value_b);
//                if (flag_c) printf(" (c : %llx),", value_c);
//                if (flag_d) printf(" (d : %llx),", value_d);
//                if (flag_y) printf(" (y : %llx),", value_y);
//                if (f) printf("\n");
                assert(flag_c == flag_d);
                if (flag_d) {
                    assert(value_c == value_d);
                }
            }
            printf("parallel merge == sequential merge\n");

            
        }

        

      
        
        /*
                
        int target[64] = {};
        
        work_queue.emplace([](int* target) -> Task {
            
            SingleConsumerCountdownEvent remaining{64};
            
            printf("enqueuing tasks\n");
            for (int i = 0; i != 64; ++i) {
                work_queue.emplace(coroutine_from_lambda([=,&remaining]() {
                    target[i] = i;
                    remaining.decrement();
                }));
            }
            
            printf("waiting for all tasks\n");
            co_await remaining;
            
            printf("show work\n");
            for (int i = 0; i != 64; ++i)
                printf("[%d] ?= %d\n", target[i], i);
            
            printf("ahutting down\n");
            work_queue.mark_done();
            
        } (target).release());
        */

      
        
        
        /*
        {
            PersistentIntMap<uint64_t> a;
            uint64_t N = 10000;
            for (uint64_t i = 0; i != N; ++i) {
                a.insert_or_replace(i, i + 10);
            }
            for (uint64_t i = 0; i != 2 * N; ++i) {
                uint64_t j;
                bool flag = a.try_find(i, j);
                assert(flag == (i < N));
                if (flag) {
                    assert(j == i + 10);
                }
            }
            PersistentIntMap<uint64_t> b;
            for (uint64_t i = 1000; i != N + 1000; ++i) {
                b.insert_or_replace(i, i + 100);
            }
            PersistentIntMap<uint64_t> c = PersistentIntMap<uint64_t>::merge(a, b);
            for (uint64_t i = 0; i != 2 * N; ++i) {
                uint64_t j;
                bool flag = c.try_find(i, j);
                assert(flag == (i < N + 1000));
                if (flag) {
                    if (i < N) {
                        assert(j == i + 10);
                    } else {
                        assert(j == i + 100);
                    }
                }
            }

            
            
            

        }
        
     
        std::vector<std::thread> threads;
        
        int thread_count = std::thread::hardware_concurrency();
        
        threads.emplace_back(collector_entry);
        for (int i = 1; i != thread_count; ++i) {
            threads.emplace_back(mutator_entry);
        }
        
        for (auto&& t : threads) {
            t.join();
        }
         */
        
    }
    
    
    
    
    /*
     // collector thread needs:
     //
     // scan: grey->black, record grey
     // sweep: dispose white
     //
     // mutator thread needs:
     // shade: roots and barrier
     
     struct Colored {
     std::atomic<uint64_t> color;
     };
     
     struct Globals {
     std::atomic<uint64_t> alloc;
     std::atomic<uint64_t> white;
     };
     
     struct Locals {
     // bag
     };
     
     struct Report {
     
     };
     
     
     Globals* globals = nullptr;
     thread_local Locals* locals = nullptr;
     
     
     
     
     
     
     void collector_entry() {
     printf("collector\n");
     }
     
     void mutator_entry() {
     printf("mutator\n");
     }
     */
    
} // namespace aaa

int main(int argc, char** argv) {
    aaa::test();
    return EXIT_SUCCESS;
}








#if 0


/*
// POSIX

#include <unistd.h>

// C++
#include <cstdlib>
#include <thread>

namespace aaa {
    



    struct ThreadLocalState {
    };
    
    thread_local ThreadLocalState* thread_local_state = nullptr;
    
    
    
    
    
    
}

int main(int argc, char** argv) {
        
    return EXIT_SUCCESS;
}

*/


#include <time.h>

#include <chrono>
#include <cstdint>
#include <vector>
#include <random>
#include <cstdio>

struct Trial {
    void* p;
    size_t n;
    uint64_t t;
};

int main(int argc, char** argv) {
    
    std::random_device rd;
    std::mt19937 gen(rd());
    std::exponential_distribution<> d(1.0 / 4096.0);
    
    size_t N = 65536;
    
    std::vector<size_t> a(N);
    for (auto& b : a) {
        b = (size_t)ceil(d(gen));
    }
    
    std::vector<Trial> b(N);
    
    
    // uint64_t t0 = clock_gettime_nsec_np(CLOCK_UPTIME_RAW);
    auto t0 = std::chrono::high_resolution_clock::now();
    
    for (size_t i = 0; i != N; ++i) {
        size_t n = a[i];
        void* p = calloc(n, 1);
        //void* p = malloc(n);
        // uint64_t t1 = clock_gettime_nsec_np(CLOCK_UPTIME_RAW);
        auto t1 = std::chrono::high_resolution_clock::now();
        uint64_t t = (t1 - t0).count();
        b[i] = Trial{p, n, t};
        t0 = t1;
    }
    
    
    auto f = fopen("/Users/antony/Desktop/trials.csv", "w");
    for (auto [p, n, t] : b) {
        fprintf(f, "%p, %zu, %llu\n", p, n, t);
    }
    fclose(f);
    
    return EXIT_SUCCESS;
    
    
}

#include <cstdint>
#include <cstdio>
#include <deque>
#include <functional>
#include <variant>



namespace aaa {
    
    struct CoroutineFrameHeader {
        void (*resume)(void*);
        void (*destroy)(void*);
    };
    
    /*
    struct {
        void (*__r)(); // function pointer to the `resume` function
        void (*__d)(); // function pointer to the `destroy` function
        promise_type; // the corresponding `promise_type`
        ... // Any other needed information (args, stack)
    }
     */
    
    struct Header {
        intptr_t color;
    };

    struct Garbage {
        void* a;
    };
    
    
    std::deque<Garbage> deque_garbage;
    
    
    namespace _ns_list {
        
        template<typename A>
        struct List {
            struct Cons : Header {
                A a;
                List b;
            };
            struct Empty {};
            Cons* a;
            
            static List empty() { return List{nullptr}; }
            
        };

        template<typename A>
        List<A>::Cons* makeCons(A x, List<A> xs) {
            auto a = new List<A>::Cons{{}, x, xs};
            deque_garbage.push_back(Garbage{a});
            return a;
        }

        template<typename A>
        bool isEmpty(List<A> a) {
            return !a.a;
        }
        
        template<typename A>
        List<A> cons(A x, List<A> xs) {
            return List<A>{makeCons(x, xs)};
        }
        
        template<typename A>
        List<A> tail(List<A> xs) {
            return xs.a ? xs.a->b : throw typename List<A>::Empty{};
        }
        
        template<typename A>
        A head(List<A> xs) {
            return xs.a ? xs.a->a : throw typename List<A>::Empty{};
        }
        
        template<typename A>
        List<A> cat(List<A> xs, List<A> ys) {
            if (isEmpty(xs))
                return ys;
           return cons(head(xs), cat(tail(xs), ys));
        }
        
        
    } // namespace _list
    
    
    
    namespace _ns_stream {
        
        template<typename A>
        struct Stream {
            struct Cell {
                /*
                 enum State {
                 NIL,
                 CONS,
                 SUSPENSION
                 };
                 State state;
                 */
                struct Cons {
                    A a;
                    Stream b;
                };
                /*
                 Cons ucons;
                 std::function<Cons()> ususp;
                 Cons& get_cons() {
                 if (state != CONS) {
                 ucons = ususp();
                 state = CONS;
                 }
                 return ucons;
                 }*/
                std::variant<Cons, std::function<Cons()>> state;
                
                Cons& get_cons() {
                    if (state.index() != 0) {
                        state.template emplace<Cons>(std::get<1>(state)());
                    }
                    return std::get<0>(state);
                }
                
            };
            Cell* a;
        };
        
        template<typename A, typename F>
        Stream<A>::Cell* makeCell(F&& f) {
            return new Stream<A>::Cell{
                // Stream<A>::Cell::SUSPENSION,
                // typename Stream<A>::Cell::Cons{0, nullptr},
                std::function<typename Stream<A>::Cell::Cons()>(std::forward<F>(f))
            };
        }
        
        template<typename A>
        A head(Stream<A> x) {
            return x.a->get_cons().a;
        }
        
        template<typename A>
        Stream<A> tail(Stream<A> x) {
            return x.a->get_cons().b;
        }
        
        // step 2:
        // we need to construct cell thus:
        //
        // state
        // union
        //    result type
        //    suspension type
        
        // suspension type is
        //
        //  struct {
        //      void (*fp)(Args...);
        //      Args... args;
        //  };
        
        // State needs to be atomic:
        // SUSPENDED -> WORKING -> CONS
        // SUSPENDED -> WORKING -> NULL
        // SUSPENDED -> WORKING -> ERROR
        // SUSPENDED -> WORKING -> AWAITED -> CONS and resume waiters
        
        // or, race to complete
        // SUSPENDED -> CONS / NULL / ERROR
        
        // this is very like a coroutine
        // what we need though is to have the storage accessible to the garbage
        // collector
        
        // coroutines and suspensions can't endure across gc boundaires because
        // we can't serialize them.  are we therefore immune from tracing them?
        
        // can we use the coroutine mechanism for suspension
                

    } // namespace _ns_stream
    
    using _ns_list::List;
    using _ns_stream::Stream;
    
    void test() {
        
        {
            auto a = List<int>::empty();
            auto b = cons(7, a);
            auto c = head(b);
            auto d = tail(b);
            auto e = cat(b, b);
            
            
            
            printf("%d\n", c);
            printf("%d\n", head(tail(e)));
        }
        
        {
            auto a = Stream<int>{nullptr};
            a.a = _ns_stream::makeCell<int>([]{
                return typename Stream<int>::Cell::Cons{8, nullptr};
            });
            printf("%d\n", head(a));
            printf("%p\n", tail(a).a);
        }
        

        
        
    }
    
}

int main(int argc, const char * argv[]) {
    aaa::test();
    return 0;
}

#endif
