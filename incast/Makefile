CC = gcc
CFLAGS = -O2 -Wall -I . -g

# This flag includes the Pthreads library on a Linux box.
# Others systems will probably require something different.
LIB = -lpthread -lrt -lm

all: server client

server: incast_server.c csapp.o
	$(CC) $(CFLAGS) -o incast_server incast_server.c csapp.o $(LIB)

client: incast_client.c csapp.o
	$(CC) $(CFLAGS) -o incast_client incast_client.c csapp.o $(LIB)

csapp.o:
	$(CC) $(CFLAGS) -c csapp.c

clean:
	rm -f *.o incast_server incast_client
