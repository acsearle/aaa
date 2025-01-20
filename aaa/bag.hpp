//
//  bag.hpp
//  aaa
//
//  Created by Antony Searle on 30/12/2024.
//

#ifndef bag_hpp
#define bag_hpp

#include <cassert>
#include <cstddef>
#include <utility>

namespace aaa {
    
    //  template<typename T>
    //  struct Bag {
    //
    //      void push(T);
    //      bool try_pop(T&);
    //
    //      void splice(Bag&&);
    //
    //  };
    
    using std::exchange;
    using std::forward;
    using std::move;
    
    // Unordered storage with worst-case O(1) push, pop and splice.
    //
    // In the worst case, push will allocate a new chunk of storage of a fixed
    // size.  Multiple subsequent pushes will be able to trivially use the new
    // chunk.  The optimal underlying chunk size will depend on both the
    // platform allocator and hardware cache sizes.
    //
    // Unlike std::deque, the nodes may be partially empty and manage their own
    // counts.  This permits efficient splicing.
    //
    // We don't attempt to manually recycle empty chunks, and just rely on the
    // allocator.
    
    template<typename T, size_t NODE_BYTES = 128>
    struct Bag {
        
        // Each node inlines a bounded stack
        
        struct alignas(NODE_BYTES) Node {
            
            static constexpr size_t CAPACITY = (NODE_BYTES - 16) / sizeof(T);
            static_assert(CAPACITY > 0);
            
            Node* next;
            size_t size;
            T data[CAPACITY];
            
            void assert_invariant() const {
                assert(size <= CAPACITY);
            }
            
            bool try_pop(T& item) {
                return size && (item = data[--size], true);
            }
            
            bool try_push(T item) {
                return (size != CAPACITY) && (data[size++] = item, true);
            }
            
        }; // Node
        
        static_assert(sizeof(Node) == NODE_BYTES);
        
        Node* head;
        Node* tail;
        
        void assert_invariant() const {
            assert(!head == !tail);
            for (const Node* current = head; current; current = current->next) {
                assert_invariant(*current);
                if (!current->next)
                    assert(tail == current);
            }
        }
        
        void swap(Bag& other) {
            using std::swap;
            swap(head, other.head);
            swap(tail, other.tail);
        }
        
        void clear() {
            while (head)
                delete exchange(head, head->next);
            tail = nullptr;
        }
        
        Bag()
        : head(nullptr)
        , tail(nullptr) {
        }
        
        Bag(const Bag& other) = delete;
        
        Bag(Bag&& other)
        : head(exchange(other.head, nullptr))
        , tail(exchange(other.tail, nullptr)) {
        }
        
        ~Bag() {
            clear();
        }
        
        Bag& operator=(const Bag&) = delete;
        
        Bag& operator=(Bag&& other) {
            Bag(move(other)).swap(this);
            return *this;
        }
        
        void _push_node() {
            head = new Node{head, 0};
            if (!tail)
                tail = head;
        }
        
        void _pop_node() {
            if (head) {
                delete exchange(head, head->next);
                if (!head)
                    tail = nullptr;
            }
        }
        
        void push(T x) {
            while (!head || !head->try_push(x))
                _push_node();
        }
        
        bool try_pop(T& x) {
            // This loop repeats discarding empty nodes; we can get one empty
            // node by popping the node's last item; to get multiple empty
            // nodes we must have spliced multiple bags whose last item was
            // just popped.  Worst-case O(1) unless we do the latter.
            for (;;) {
                if (!head)
                    return false;
                if (head->try_pop(x))
                    return true;
                _pop_node();
            }
        }
        
        void splice(Bag&& other) {
            if (other->head) {
                assert(other->tail);
                (head ? tail->next : head) = exchange(other->head, nullptr);
                tail = exchange(other->tail, nullptr);
            }
        }
        
    };
    
    template<typename T, size_t NODE_BYTES>
    void swap(Bag<T, NODE_BYTES>& a, Bag<T, NODE_BYTES>& b) {
        a.swap(b);
    }
    
    
} // namespace aaa

#endif /* bag_hpp */
