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
#include "latch.hpp"
#include "persistent_map.hpp"
#include "skiplist.hpp"
#include "gc.hpp"
#include "termination_detection_barrier.hpp"

namespace aaa {
    
    static constexpr int THREAD_COUNT = 10;
    
    // 1) explicit stop
    std::atomic<bool> q_done = false;
    
    // 2) implicit stop when all threads run out of work
    termination_detection_barrier tdb{THREAD_COUNT - 1}; // we save one core for main
    
    // 3) sleeping mechanism
    
    Atomic<ptrdiff_t> _sleep_generation_global{0};
    Atomic<ptrdiff_t> _sleep_generation_local[THREAD_COUNT];
    ptrdiff_t _sleep_generation_cached[THREAD_COUNT] = {};
    
    // Since a thread trying to sleep has nothing to do anyway, we want to
    // push as much of the cost of the mechanism onto the sleeping thread, and
    // minimize the burden on the work-generating thread that wakes it up
    
    // Once a worker thread has observed all queues to be empty, it
    // - reads the global sleep generation
    // - publishes that value to each queue
    // - atomically waits if the global generation has not changed
    
    // When a worker thread pushes new work, it needs to wake up anybody who
    // is sleeping.  But this is an expensive operation, we can't afford to
    // call it every push.
    // - pushes new work
    // - knows of some sleep generation
    // - reads the local sleep generation
    // - if the local sleep generation is less than the known generation, nobody
    //   has tried to sleep since we last checked and we are done
    // - if the local sleep generation is equal to or greater than the known generation,
    //   somebody has tried to sleep since we last checked, and we need to wake
    //   them up with the new work we just pushed
    // - compare_exchange(expected, local + 1)
    //   we may discover that we are lagging and the generation was already increased
    //   then we are done
    // - otherwise, we are responsible for waking everybody up
    
            
    void worker_entry(int index) {
        tlq_index = index;
        arena_initialize();
        thread_local_random_number_generator = new std::ranlux24_base;
        gc::mutator_enter();
        
        
        std::coroutine_handle<> work = nullptr;
        ptrdiff_t sleep_observed = 0;
    
    POP_OWN:
        if (!work_queues[index]->pop(work))
            goto STEAL_OTHER;
        {
            // HACK: wakeups
            // This code should be in the push, but this is vaguely in the right
            // place
            ptrdiff_t cached = _sleep_generation_cached[index];
            ptrdiff_t observed = _sleep_generation_local[index].load(Ordering::RELAXED);
            if (observed >= cached) {
                ptrdiff_t expected = observed;
                ptrdiff_t desired = observed + 1;
                bool result = _sleep_generation_global
                    .compare_exchange_strong(expected,
                                             desired,
                                             Ordering::RELAXED,
                                             Ordering::RELAXED);
                if (result)
                    _sleep_generation_global.notify_all();
                _sleep_generation_cached[index] = result ? desired : expected;
            }
            
        }
        
    DO_WORK:
        // printf("thread %d is working\n", index);
        work.resume();
        goto POP_OWN;
        
    STEAL_OTHER:
        sleep_observed = _sleep_generation_global.load(Ordering::RELAXED);
        for (int j = 1; j != THREAD_COUNT; ++j) {
            int k = (index + j) % THREAD_COUNT;
            if (work_queues[k]->steal(work))
                goto DO_WORK;
        }
        
    TRY_SLEEP:
        {
            // HACK: sleepdowns
            for (int j = 0; j != THREAD_COUNT; ++j) {
                int k = (index + j) % THREAD_COUNT;
                ptrdiff_t y = _sleep_generation_local[k].max_fetch(sleep_observed, Ordering::RELAXED);
                if (y > sleep_observed)
                    goto STEAL_OTHER;
            }
            // we told every thread we are sleeping without discovering that
            // our observations were out of date
            printf("thread %d is sleeping\n", index);
            // go to sleep only if the generation is what we expect
            _sleep_generation_global.wait_for(sleep_observed, Ordering::RELAXED, 1000000000);
            printf("thread %d is waking\n", index);
            goto STEAL_OTHER;
        }
        
        /*
    TERMINATION_DETECTION:
        // it's now very likely that there is no work available
        // but other threads may be working, and may generate work at any time
        // we should sleep to conserve power and allow other processes to use
        // this core
        // but also, we should spin to ensure we swiftly pick up any new jobs
        tdb.set_inactive();
        std::this_thread::yield();
        while (!tdb.is_terminated()) {
            for (int j = 1; j != THREAD_COUNT; ++j) {
                int k = (index + j) % THREAD_COUNT;
                if (work_queues[k].can_steal()) {
                    tdb.set_active();
                    goto STEAL_OTHER;
                }
            }
        }
         */
        
        gc::mutator_leave();
        arena_finalize();

    }
        
    
    
    
    void worker_entry2(int index) {
        tlq_index = index;
        arena_initialize();
        thread_local_random_number_generator = new std::ranlux24_base;
        gc::mutator_enter();
        
        std::coroutine_handle<> work = nullptr;
        ptrdiff_t sleep_observed = 0;
        
    POP_OWN:
        if (!work_queues[index]->pop(work))
            goto STEAL_OTHER;
        
    DO_WORK:
        // printf("thread %d is working\n", index);
        work.resume();
        goto POP_OWN;
        
    STEAL_OTHER:

        for (int j = 1; j != THREAD_COUNT; ++j) {
            int k = (index + j) % THREAD_COUNT;
            if (work_queues[k]->steal(work))
                goto DO_WORK;
        }
        
    TERMINATION_DETECTION:
        
        // It's now probable (though not certain) that there was no work
        // available.  Other threads may be working and may generate more
        // work.

        // In principle, we should sleep now and be awoken when there is
        // more work or a change in the pool state.  In practice, it is likely
        // (but should be measured!) that the system scheduler will wake up
        // threads too coarsely to participate in 60 Hz work.

        // Thus we have to spin until something changes.  Meanwhile the garbage
        // collector and other subsystems may have work, if we can service them.

        // The fork-join model results in one final job that knows that it
        // completes a workflow and can communicate that fact.  This marks the
        // end of the lifetimes of all the coroutines and ephemeral helper
        // structures like the skiplist, and we can now reuse their
        // allocations.  But this does mean that if the pool mingles other
        // kinds of work, it has to use different allocators for different
        // workloads.
        
        // Garbage collection on the thread pool means it can't wait on...
        
        

                

        
        // The garbage collected objects don't need to be tied to the frame
        // rate, just periodically serviced.
        
        // The arena allocator works for the coroutines and temporary data
        // structures they create per frame so long as we have some kind of
        // consensus barrier that marks the end of their lifetime.
        
        // This consensus barrier forces the threads to spin while out of work
        // until the final job completes, and then spin until all threads have
        // acknowledged this.
        
        // But, we are doing other stuff.  Garbage collection, rendering and network.
        // If they use the pool threads, we lose the rule that all things
        // operate on the same per-frame cycle.  (Though these
        
        
        // An interesting point to note is that all these difficulties result
        // from the (rapid) reuse of memory (as in, before it becomes
        // unreachable).
        
        
        
        // All threads have run out of work; the phase is complete
        
        gc::mutator_handshake();
        
        // our only gc root is the work_queue's circular_array
        work_queues[index]->_array.load(std::memory_order_relaxed)->_object_shade();

        // reuse the arena memory
        arena_advance();
        
        gc::mutator_leave();
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
    latch::signalling_coroutine
    parallel_merge_right(latch&,
                         PersistentIntMap<T> a,
                         frozen_skiplist_map<uint64_t, T> b,
                         PersistentIntMap<T>& c) {
        printf("%s\n", __PRETTY_FUNCTION__);
        latch inner;
        parallel_merge_right<T>(inner,
                                a._root,
                                b.top(),
                                &c._root,
                                (uint64_t)0,
                                ~(uint64_t)0);
        co_await inner;
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
    latch::signalling_coroutine
    parallel_persist_generate_outer(latch&,
                                    PersistentIntMap<T>* target,
                                    uint64_t outer_key_low,
                                    uint64_t outer_key_high,
                                    const F& f) {
        latch inner;
        parallel_persist_generate<T, F>(inner, &(target->_root), outer_key_low, outer_key_high, f);
        co_await inner;
        //for (int i = 0; i != 10; ++i) {
            // work_queues[i].mark_done();
        //}

    }
    
    
    
    
    co_void async_test() {
        
        latch inner;

        uint64_t N = 100000000 / 1;
        uint64_t M = 1000000 / 1; // <-- sparseifier
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
        
        PersistentIntMap<uint64_t> d;
        {
            // parallel_merge_left(a, b);
            //printf("parallel merge_right\n");
            parallel_merge_right<uint64_t>(inner, b, y, d);
        }


        co_await inner;
        
        printf(" ------ ---- - -----------\n");
        
        
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

        q_done.store(true, std::memory_order_release);

    }
    
    
    void test() {

        // start the garbage collector thread
        gc::collector_start();

        
        tlq_index = 0; // thread pool id
        arena_initialize(); // thread-local bump allocator
        thread_local_random_number_generator = new std::ranlux24_base;
        
        
        // get permission to start allocating gc::Objects
        gc::mutator_enter();
        
        // allocate the work stealing deques now we have gc
        for (int i = 0; i != 10; ++i) {
            work_queues[i] = new work_stealing_deque<std::coroutine_handle<>>;
        }
        
        
        
        std::vector<std::thread> threads;
        
        int thread_count = 10; // std::thread::hardware_concurrency();
        for (int i = 1; i != thread_count; ++i)
            threads.emplace_back(worker_entry, i);

        async_test();
        // unfortunately this enqueues on the only thread not being serviced!
        _sleep_generation_global.notify_all();
        
        // main thread blocks until all threads join
        for (auto&& t : threads)
            t.join();
    
        // d should now contain merge_right(b, y) (aka y wins collisions)
    
        gc::mutator_leave();
        arena_finalize();
        
        gc::collector_stop();
              
    }
    
} // namespace aaa

int main(int argc, char** argv) {
    aaa::test();
    return EXIT_SUCCESS;
}






