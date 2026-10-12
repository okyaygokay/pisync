/**
 * Intrusive-free doubly linked queue with O(1) push/shift/remove-by-node.
 * Used for the task queue and per-agent request queues, where waiters must be
 * removable in O(1) on abort/timeout without scanning.
 */
export interface QueueNode<T> {
  readonly value: T;
  prev: QueueNode<T> | null;
  next: QueueNode<T> | null;
  /** Owning queue, or null once removed. Guards against double-remove. */
  owner: LinkedQueue<T> | null;
}

export class LinkedQueue<T> implements Iterable<T> {
  private head: QueueNode<T> | null = null;
  private tail: QueueNode<T> | null = null;
  private count = 0;

  get size(): number {
    return this.count;
  }

  peek(): T | undefined {
    return this.head?.value;
  }

  pushBack(value: T): QueueNode<T> {
    const node: QueueNode<T> = { value, prev: this.tail, next: null, owner: this };
    if (this.tail) this.tail.next = node;
    else this.head = node;
    this.tail = node;
    this.count++;
    return node;
  }

  pushFront(value: T): QueueNode<T> {
    const node: QueueNode<T> = { value, prev: null, next: this.head, owner: this };
    if (this.head) this.head.prev = node;
    else this.tail = node;
    this.head = node;
    this.count++;
    return node;
  }

  shift(): T | undefined {
    const node = this.head;
    if (!node) return undefined;
    this.remove(node);
    return node.value;
  }

  /** Returns true if the node was in this queue and has been removed. */
  remove(node: QueueNode<T>): boolean {
    if (node.owner !== this) return false;
    if (node.prev) node.prev.next = node.next;
    else this.head = node.next;
    if (node.next) node.next.prev = node.prev;
    else this.tail = node.prev;
    node.prev = node.next = null;
    node.owner = null;
    this.count--;
    return true;
  }

  *[Symbol.iterator](): Iterator<T> {
    for (let n = this.head; n; n = n.next) yield n.value;
  }

  toArray(): T[] {
    return [...this];
  }
}
