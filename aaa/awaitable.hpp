//
//  awaitable.hpp
//  aaa
//
//  Created by Antony Searle on 1/1/2025.
//

#ifndef awaitable_hpp
#define awaitable_hpp

#include <cassert>
#include <cstdio>

#include <atomic>
#include <coroutine>
#include <exception>

#include <mutex>

#include "concurrent_deque.hpp"
#include "work_stealing_deque.hpp"

namespace aaa {
    
    inline work_stealing_deque<std::coroutine_handle<>>* work_queues[10] = {};
    
    inline thread_local int tlq_index = 0;
    
    inline void schedule_coroutine_handle(std::coroutine_handle<> handle) {
        // work_queue.emplace(handle);
        // work_queues[tlq_index].emplace(handle);
        work_queues[tlq_index]->push(handle);
    }
        
    inline void schedule_coroutine_handle_from_address(void* address) {
        schedule_coroutine_handle(std::coroutine_handle<>::from_address(address));
    }
    
    
    struct suspend_always_and_schedule {
        constexpr bool await_ready() const noexcept { return false; }
        void await_suspend(std::coroutine_handle<> handle) const noexcept {
            schedule_coroutine_handle(handle);
        }
        void await_resume() const noexcept { }
    };
    
    
    
    // spawns a detached coroutine.  notification of completion must be achieved
    // by some mechanism in the coroutine body.
    
    struct co_void {
        struct promise_type {
            constexpr co_void get_return_object() const noexcept { return co_void{}; }
            auto initial_suspend() const noexcept {
                return suspend_always_and_schedule{};
            }
            constexpr auto final_suspend() const noexcept {
                return std::suspend_never{};
            }
            constexpr void return_void() const noexcept { }
            void unhandled_exception() const noexcept { std::terminate(); }
        }; // struct promise_type
    }; // struct co_void
    
    
   
} // namespace aaa

namespace std {
    
    /*
    template<typename... Args>
    struct std::coroutine_traits<void, Args...> {
        using promise_type = aaa::co_void::promise_type;
    };
     */
    
}

namespace aaa {
    
    struct Task {
        
        struct promise_type {
            
            ~promise_type() {
                printf("Task::promise_type destroyed\n");
            }
            
            Task get_return_object() noexcept {
                return Task(this);
            }
            
            constexpr auto initial_suspend() const noexcept {
                return std::suspend_always{};
            }
            
            constexpr auto final_suspend() const noexcept {
                struct awaitable : std::suspend_always {
                    std::coroutine_handle<> await_suspend(std::coroutine_handle<promise_type> handle) const noexcept {
                        std::coroutine_handle<> result = handle.promise()._continuation;
                        handle.destroy();
                        return result;
                    };
                };
                return awaitable{};
            }
            
            constexpr void return_void() const noexcept {
            };
            
            void unhandled_exception() const noexcept {
                std::terminate();
            }
            
            // returned-to
            std::coroutine_handle<> _continuation = std::noop_coroutine();
                                
        };
        
        promise_type* _promise = nullptr;
        
        Task() : _promise(nullptr) {};
        Task(promise_type* promise) : _promise(promise) { assert(promise); }
        Task(const Task&) = delete;
        Task(Task&& other) : _promise(std::exchange(other._promise, nullptr)) {}
        ~Task() {
            if (_promise)
                std::coroutine_handle<promise_type>::from_promise(*_promise).destroy();
        }
        
        Task& operator=(const Task&) = delete;
        Task& operator=(Task&& other) {
            Task a(std::move(other));
            std::swap(_promise, a._promise);
            return *this;
        }
        
        std::coroutine_handle<promise_type> release() {
            return std::coroutine_handle<promise_type>::from_promise(*std::exchange(_promise, nullptr));
        }
        

        constexpr bool await_ready() const noexcept { return false; }
        
        std::coroutine_handle<> await_suspend(std::coroutine_handle<> handle) const noexcept {
            _promise->_continuation = handle;
            return std::coroutine_handle<promise_type>::from_promise(*_promise);
        }
        
        void await_resume() const noexcept {
            assert(std::coroutine_handle<promise_type>::from_promise(*_promise).done());
        }
        
    };
    
    
    // TODO: replace with an actual scheduler

    
    
    
    // Windows / .NET inspired primitives.  These are a bit lighter-weight than
    // condition variables for coroutines
    
    // We could support any(?) combination of these features
    // - One coroutine or many coroutines can wait on the object
    // - Release one coroutine or all coroutines on signal
    // - Support a signalled state denying waiting
    // - Support a full state denying waiting (for single consumer objects)
    // AutoReset and ManualReset conflate reset behavior with release behavior,
    // but presumably these are just bad names for the useful corners of phase
    // space
    
    // Intrusive linked lists of waiting coroutines give a maximally unfair
    // LIFO stack to resume
    
    
    
    
    struct SingleConsumerManualResetEvent {
        
        enum : intptr_t {
            NONSIGNALED,
            SIGNALED,
        };
        
        std::atomic<intptr_t> _state = NONSIGNALED;
        
        void set() {
            intptr_t observed = _state.exchange(SIGNALED, std::memory_order_release);
            switch (observed) {
                case NONSIGNALED:
                    // expected
                    break;
                case SIGNALED:
                    // redundant set
                    break;
                default:
                    // awaited
                    (void) _state.load(std::memory_order_acquire);
                    schedule_coroutine_handle_from_address((void*) observed);
                    break;
            }
        }
        
        void reset() {
            // reset establishes no memory ordering
            intptr_t expected = SIGNALED;
            (void) _state.compare_exchange_strong(expected,
                                                  NONSIGNALED,
                                                  std::memory_order_relaxed,
                                                  std::memory_order_relaxed);
        }
        
        auto operator co_await() {
            struct awaitable {
                SingleConsumerManualResetEvent* _event;
                bool await_ready() noexcept {
                    intptr_t observed = _event->_state.load(std::memory_order_acquire);
                    assert(observed == NONSIGNALED || observed == SIGNALED);
                    return observed == SIGNALED;
                }
                bool await_suspend(std::coroutine_handle<> handle) {
                    intptr_t expected = NONSIGNALED;
                    intptr_t desired = (intptr_t) handle.address();
                    bool result = _event->_state.compare_exchange_strong(expected,
                                                                         desired,
                                                                         std::memory_order_release,
                                                                         std::memory_order_acquire);
                    assert(expected == NONSIGNALED || expected == SIGNALED);
                    return result;
                }
                void await_resume() {
                }
            };
            return awaitable{this};
        }
        
        ~SingleConsumerManualResetEvent() {
            // relaxed load because the destructor must be sequenced after
            // the release of any waiters
            intptr_t observed = _state.load(std::memory_order_relaxed);
            assert(observed == NONSIGNALED || observed == SIGNALED);
        }
        
    }; // SingleConsumerManualResetEvent

    struct SingleConsumerCountdownEvent {
        
        std::atomic<ptrdiff_t> _count;
        SingleConsumerManualResetEvent _inner;
        
        void decrement() {
            ptrdiff_t n = _count.fetch_sub(1, std::memory_order_release) - 1;
            if (n == 0) {
                (void) _count.load(std::memory_order_acquire);
                _inner.set();
            }
        }
        
        auto operator co_await() {
            return _inner.operator co_await();
        }
                
    };
    
    
    
    struct AutoResetEvent {
        
        // TODO: make this match the semantics of the Win32/.NET version
        //
        struct AwaiterListNode {
            const AwaiterListNode* next;
            std::coroutine_handle<> handle;
        };
        
        std::atomic<const AwaiterListNode*> _state;
        
        void set() {
            const AwaiterListNode* head = _state.exchange(nullptr, std::memory_order_acq_rel);
            for (; head != nullptr; head = head->next)
                schedule_coroutine_handle(head->handle);
        }
                    
        auto operator co_await() {
            struct awaitable {
                AutoResetEvent* _event;
                AwaiterListNode _node;
                constexpr bool await_ready() const noexcept {
                    return false;
                }
                void await_suspend(std::coroutine_handle<> handle) noexcept {
                    _node.next = _event->_state.load(std::memory_order_relaxed);
                    _node.handle = handle;
                    while (!_event->_state.compare_exchange_weak(_node.next,
                                                                 &_node,
                                                                 std::memory_order_release,
                                                                 std::memory_order_relaxed))
                        ;
                }
                void await_resume() {}
            };
            return awaitable{this};
        };
        
        
        
        
    };
    
    
    
    struct ManualResetEvent {
        
        struct AwaiterListNode {
            intptr_t next;
            std::coroutine_handle<> handle;
        };
        
        enum : intptr_t {
            NONSIGNALED = 0,
            SIGNALED,
        };
        
        std::atomic<intptr_t> _state;
        
        void set() {
            intptr_t observed = _state.exchange(SIGNALED, std::memory_order_release);
            switch (observed) {
                case NONSIGNALED:
                    // expected
                    break;
                case SIGNALED:
                    // redundant set
                    break;
                default:
                    // awaited
                    std::atomic_thread_fence(std::memory_order_acquire);
                    // traverse the intrusive list of waiting coroutines and
                    // schedule them
                    for (const AwaiterListNode* current = (const AwaiterListNode*) observed;
                         current != nullptr;
                         current = (const AwaiterListNode*) (current->next)) {
                        schedule_coroutine_handle(current->handle);
                    }
                    break;
            }
        }
        
        void reset() {
            // reset establishes no memory ordering
            intptr_t expected = SIGNALED;
            (void) _state.compare_exchange_strong(expected,
                                                  NONSIGNALED,
                                                  std::memory_order_relaxed,
                                                  std::memory_order_relaxed);
        }
                
        auto operator co_await() {
            struct awaitable {
                ManualResetEvent* _event;
                AwaiterListNode _node;
                bool await_ready() noexcept {
                    _node.next = _event->_state.load(std::memory_order_acquire);
                    return _node.next == SIGNALED;
                }
                bool await_suspend(std::coroutine_handle<> handle) noexcept {
                    // _node.next is the expected value aready
                    _node.handle = handle;
                    intptr_t desired = (intptr_t)(&_node);
                    for (;;) {
                        if (_node.next == SIGNALED)
                            return false;
                        if (_event->_state.compare_exchange_weak(_node.next,
                                                                 desired,
                                                                 std::memory_order_release,
                                                                 std::memory_order_acquire))
                            return true;
                    }
                }
                void await_resume() {}
            };
            return awaitable{this};
        };
        
        
        
        
        
    };
    
    
    struct AsyncMutex {
        
        // This mutex is strictly FIFO
        
        // We might be better off using a true mutex to briefly block
        // the thread, combined with an async condition_variable that suspends
        // the coroutines and then thundering herds them onto the unfair lock
        
        struct Node {
            intptr_t predecessor;
            Node* successor;
            std::coroutine_handle<> handle;
        };

        enum : intptr_t {
            LOCKED = 0, // <-- it's useful to have LOCKED == nullptr
            UNLOCKED = 1,
        };
        
        // atomic state + intrusive queue of waiters
        std::atomic<intptr_t> _state = UNLOCKED;
        // cached queue of waiters, only non-null when locked
        Node* _head = nullptr;
        
        auto operator co_await() {
            struct awaitable {
                AsyncMutex* _mutex;
                Node _node;
                bool await_ready() noexcept {
                    printf("%s\n", __PRETTY_FUNCTION__);
                    _node.predecessor = UNLOCKED;
                    return _mutex->_state.compare_exchange_strong(_node.predecessor,
                                                                  LOCKED,
                                                                  std::memory_order_acquire,
                                                                  std::memory_order_relaxed);
                }
                bool await_suspend(std::coroutine_handle<> handle) noexcept {
                    printf("%s\n", __PRETTY_FUNCTION__);
                    _node.handle = handle;
                    intptr_t desired = (intptr_t)&_node;
                    for (;;) {
                        switch(_node.predecessor) {
                            case UNLOCKED:
                                if (_mutex->_state.compare_exchange_weak(_node.predecessor,
                                                                         LOCKED,
                                                                         std::memory_order_acquire,
                                                                         std::memory_order_relaxed)) {
                                    printf("UNLOCKED -> LOCKED\n");
                                    return false;
                                }
                                break;
                            default:
                                if (_mutex->_state.compare_exchange_weak(_node.predecessor,
                                                                         desired,
                                                                         std::memory_order_release,
                                                                         std::memory_order_relaxed))
                                    printf("LOCKED or AWAITED -> AWAITED\n");
                                    return true;
                                break;
                        }
                    }
                }
                std::unique_lock<AsyncMutex> await_resume() noexcept {
                    printf("%s\n", __PRETTY_FUNCTION__);
                    return std::unique_lock<AsyncMutex>(*_mutex, std::adopt_lock);
                }
            };
            return awaitable{this};
        }
        
        
                
        void _pop_head_and_schedule() {
            printf("%s\n", __PRETTY_FUNCTION__);
            assert(_head);
            std::coroutine_handle<> handle = _head->handle;
            _head = _head->successor;
            schedule_coroutine_handle(handle);
        }

                
        bool try_lock() {
            printf("%s\n", __PRETTY_FUNCTION__);
            intptr_t expected = UNLOCKED;
            return _state.compare_exchange_strong(expected,
                                                  LOCKED,
                                                  std::memory_order_acquire,
                                                  std::memory_order_relaxed);
        }
                
        void unlock() {
            printf("%s\n", __PRETTY_FUNCTION__);
            if (_head) {
                _pop_head_and_schedule();
                return;
            }
            intptr_t expected = LOCKED;
            for (;;) {
                switch (expected) {
                    case UNLOCKED:
                        // precondition violated
                        abort();
                    case LOCKED:
                        if (_state.compare_exchange_weak(expected,
                                                         UNLOCKED,
                                                         std::memory_order_release,
                                                         std::memory_order_relaxed))
                            return;
                        break;
                    default: {
                        if (_state.compare_exchange_weak(expected,
                                                         LOCKED,
                                                         std::memory_order_acq_rel,
                                                         std::memory_order_acquire)) {
                            // we have taken the list of waiting coroutine handles
                            Node* current = (Node*)expected;
                            assert(current);
                            // reverse the list onto head
                            while (current) {
                                current->successor = _head;
                                _head = current;
                            }
                            assert(_head);
                            _pop_head_and_schedule();
                        }
                        break;
                    }

                }
            }
        }
        
    };
    
    struct AsyncConditionVariable {
        
        // keeps a list of waiting things
        
        // transfers them one or all to the related mutex's queue
        
        // to avoid various races we need separate nodes for the condition
        // variable and mutex queues
        struct Node {
            Node* predecessor;
            AsyncMutex* mutex; // <-- very redundant!
            AsyncMutex::Node mutex_node;
        };
        
        std::atomic<Node*> _state = 0;
                
        auto wait(std::unique_lock<AsyncMutex>& lock) {
            struct awaitable {
                AsyncConditionVariable* _condition_variable;
                Node _node;
                constexpr bool await_ready() const noexcept { return false; }
                void await_suspend(std::coroutine_handle<> handle) {
                    _node.mutex_node.handle = handle;
                    _node.predecessor = _condition_variable->_state.load(std::memory_order_relaxed);
                    while (!_condition_variable->_state.compare_exchange_weak(_node.predecessor,
                                                                              &_node,
                                                                              std::memory_order_release,
                                                                              std::memory_order_relaxed))
                        ;
                }
                constexpr void await_resume() const noexcept {}
            };
            assert(lock.owns_lock());
            return awaitable{this, {{}, lock.mutex(), {}}};
        }
        
        void notify_one() {
            Node* expected = _state.load(std::memory_order_acquire);
            for (;;) {
                if (!expected)
                    // no awaiters
                    return;
                if (_state.compare_exchange_weak(expected,
                                                 expected->predecessor,
                                                 std::memory_order_relaxed,
                                                 std::memory_order_acquire)) {
                    // we have popped the node, now push onto mutex
                    expected->mutex_node.predecessor = AsyncMutex::UNLOCKED;
                    for (;;) {
                        if (expected->mutex_node.predecessor == AsyncMutex::UNLOCKED) {
                            // mutex is unlocked; try to lock
                            if (expected->mutex->_state.compare_exchange_weak(expected->mutex_node.predecessor,
                                                                               AsyncMutex::LOCKED,
                                                                               std::memory_order_acquire,
                                                                               std::memory_order_relaxed)) {
                                // lock acquired; resume immediately
                                schedule_coroutine_handle(expected->mutex_node.handle);
                                return;
                            }
                        } else {
                            // mutex is locked; try to wait
                            if (expected->mutex->_state.compare_exchange_weak(expected->mutex_node.predecessor,
                                                                              (intptr_t)&expected->mutex_node,
                                                                              std::memory_order_release,
                                                                              std::memory_order_relaxed)) {
                                // wait queue joined
                                return;
                            }
                        }
                    }
                }
            }
        }
        
        void notify_all() {
            // unconditionally take the queue
            Node* observed = _state.exchange(nullptr, std::memory_order_acquire);
            if (!observed)
                // no awaiters; nothing to do
                return;
            // find the head and fixup the links
            Node* current = observed;
            while (current->predecessor) {
                current->mutex_node.predecessor = (intptr_t)(&current->predecessor->mutex_node);
                current->predecessor->mutex_node.successor = &current->mutex_node;
                current = current->predecessor;
                assert(current->mutex == observed->mutex);
            }
            AsyncMutex::Node* head = &current->mutex_node;
            AsyncMutex::Node* tail = &observed->mutex_node;
            head->predecessor = AsyncMutex::LOCKED;
            tail->successor = nullptr;
            AsyncMutex* mutex = observed->mutex;
            intptr_t expected = mutex->_state.load(std::memory_order_relaxed);
            for (;;) {
                switch (head->predecessor) {
                    case AsyncMutex::UNLOCKED:
                        // the mutex was not locked
                        if (mutex->_state.compare_exchange_weak(expected, AsyncMutex::LOCKED, std::memory_order_acquire, std::memory_order_relaxed)) {
                            // the cache should be empty
                            assert(mutex->_head = nullptr);
                            // install most of the queue in the cache
                            mutex->_head = head->successor;
                            // schedule the head of the queue, transferring our ownership to it
                            schedule_coroutine_handle(head->handle);
                            return;
                        }
                        break;
                    case AsyncMutex::LOCKED:
                    default:
                        // mutex is locked and possibly awaited
                        // atomically splice the whole queue in
                        head->predecessor = expected;
                        if (mutex->_state.compare_exchange_weak(expected, (intptr_t)tail, std::memory_order_release, std::memory_order_relaxed))
                            return;
                        // try again
                        break;
                }
            }
            
        }
        
        
    };
    
    
    // concurrent (not parallel) future
    //
    // useful for composing blocking coroutines
    template<typename T>
    struct co_future {
        struct promise_type {
            
            // result machinery
            enum : intptr_t {
                EMPTY,
                VALUE,
                EXCEPTION,
            };
            std::atomic<intptr_t> _state;
            union {
                T _value;
                std::exception_ptr _exception_ptr;
            };
            
            // continuation slot
            std::coroutine_handle<> _continuation;
            
            
            ~promise_type() {
                switch (_state.load(std::memory_order_relaxed)) {
                    case EMPTY:
                        break;
                    case VALUE:
                        _value.~T();
                        break;
                    case EXCEPTION:
                        _exception_ptr.~exception_ptr();
                }
            }
            
            co_future<T> get_return_object() const {
                return co_future{this};
            }
            constexpr auto initial_suspend() const noexcept {
                return std::suspend_always{};
            }
            void return_value(auto&& value) {
                new((void*)&_value) T(std::forward<decltype(value)>(value));
                _state.exchange(VALUE, std::memory_order_release);
            }
            void unhandled_exception() const noexcept {
                new((void*)&_exception_ptr) std::exception_ptr{std::current_exception()};
                _state.exchange(EXCEPTION, std::memory_order_release);
            }
            constexpr auto final_suspend() const noexcept {
                struct awaitable {
                    constexpr bool await_ready() const noexcept { return true; }
                    std::coroutine_handle<> await_suspend(std::coroutine_handle<promise_type> promise) const noexcept {
                        
                    }
                    
                };
                return awaitable{};
            }
        }; // struct promise_type
        
        promise_type* _promise;
        
        
        void swap(co_future& other) {
            using std::swap;
            swap(_promise, other._promise);
        }
        
        explicit co_future(promise_type* promise)
        : _promise() {
        }
        
        co_future(co_future&& other)
        : _promise(std::exchange(other._promise, nullptr)) {
        }
        
        ~co_future() {
            std::coroutine_handle<promise_type>::from_promise(*_promise).destroy();
        }
        
        co_future& operator=(co_future&& other) {
            co_future{std::move(other)}.swap(*this);
            return *this;
        }
                    
        bool await_ready() const noexcept {
            return false;
        }
        
        std::coroutine_handle<> await_suspend(std::coroutine_handle<> handle) {
            assert(_promise);
            _promise->_continuation = handle;
            return std::coroutine_handle<promise_type>::from_promise(*_promise);
        }
        
        T await_resume() const {
            switch (_promise->_state.load(std::memory_order_acquire)) {
                case promise_type::VALUE:
                    return std::move(_promise->_value);
                case promise_type::EXCEPTION:
                    std::rethrow_exception(_promise->_exception_ptr);
                default:
                    abort();
            }
        }
        
    }; // struct co_future
    
    // todo: we can decouple starting, awaiting and completing the co_future at
    // the cost of some additional manipulations
    
} // namespace aaa

#endif /* awaitable_hpp */
