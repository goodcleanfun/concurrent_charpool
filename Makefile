install:
	clib install --dev

test:
	@$(CC) $(CFLAGS) test.c -g -I src -I deps $(LDFLAGS) -o $@
	@./$@

.PHONY: test
