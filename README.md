# concurrent_charpool
Concurrent cache-friendly memory pool for strings, using adaptive free-lists: stacks when strings are smaller than a pointer and in-block for strings that are pointer-size or larger.
