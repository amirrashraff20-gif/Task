export class LinkedListNode<T> {
  constructor(
    public value: T,
    public next: LinkedListNode<T> | null = null,
    public prev: LinkedListNode<T> | null = null
  ) {}
}

export class LinkedList<T> {
  private head: LinkedListNode<T> | null = null;
  private tail: LinkedListNode<T> | null = null;
  private _size: number = 0;

  public get size(): number {
    return this._size;
  }

  public isEmpty(): boolean {
    return this._size === 0;
  }

  public peek(): T | undefined {
    return this.head ? this.head.value : undefined;
  }

  public clear(): void {
    this.head = null;
    this.tail = null;
    this._size = 0;
  }

  public push(value: T): void {
    const node = new LinkedListNode(value);
    if (!this.tail) {
      this.head = this.tail = node;
    } else {
      this.tail.next = node;
      node.prev = this.tail;
      this.tail = node;
    }
    this._size++;
  }

  public shift(): T | undefined {
    if (!this.head) return undefined;
    
    const value = this.head.value;
    this.head = this.head.next;
    
    if (this.head) {
      this.head.prev = null;
    } else {
      this.tail = null;
    }
    
    this._size--;
    return value;
  }

  public *[Symbol.iterator](): Iterator<T> {
    let current = this.head;
    while (current) {
      yield current.value;
      current = current.next;
    }
  }
}
