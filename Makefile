# mx-queued Makefile
# Copyright(c) YukChung Li

DEBUG?= -g
CFLAGS?= -std=c99 -pedantic -O2 -Wall -W -DSDS_ABORT_ON_OOM -llua -lm -ldl
CCOPT= $(CFLAGS)

OBJ = main.o ae.o hash.o skiplist.o db.o utils.o lua.o
PRGNAME = mx-queued

all: server

server: $(OBJ)
	$(CC) -o $(PRGNAME) $(DEBUG) $(OBJ) $(CCOPT)

main.o: main.c global.h
	$(CC) -c main.c

utils.o: utils.c utils.h
	$(CC) -c utils.c

ae.o: ae.c ae.h
	$(CC) -c ae.c

skiplist.o: skiplist.c skiplist.h
	$(CC) -c skiplist.c

hash.o: hash.c hash.h list.h
	$(CC) -c hash.c

db.o: db.c global.h
	$(CC) -c db.c

lua.o: lua.c global.h
	$(CC) -c lua.c

clean:
	rm -rf $(PRGNAME) *.o
