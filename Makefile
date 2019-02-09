PREFIX = /usr/local

CC ?= gcc
BIN = grofs
SRC = $(wildcard *.c)
OBJ = $(SRC:.c=.o)
CCFLAGS = -Wall -Wextra $(shell pkg-config libgit2 --cflags --libs) $(shell pkg-config fuse --cflags --libs)

$(BIN): $(OBJ)
	$(CC) -o $(BIN) $(OBJ) $(CCFLAGS)

%.o: %.c
	$(CC) -c $< $(CCFLAGS) -o $@

.PHONY: clean
clean:
	rm -f $(OBJ) $(BIN)

.PHONY: install
install: $(BIN)
	mkdir -p $(DESTDIR)$(PREFIX)/bin
	cp $(BIN) $(DESTDIR)$(PREFIX)/bin/$(BIN)

.PHONY: uninstall
uninstall:
	rm -f $(DESTDIR)$(PREFIX)/bin/$(BIN)
