CFLAGS  := -O3 -g 
LD      := gcc
LDFLAGS := ${LDFLAGS} -lrdmacm -libverbs -lrt -lpthread -lmnl -lm


#SRC = $(wildcard *.c *.h)
#OBJ = $(SRC:.c=.o)

#OBJ = measure_timestamp.o main.o common.o  rudp.o rtt.o share.o tcp_vegas.o timely_reno.o queue.o 

#TARGET = measure_timestamp main

#all: $(TARGET)

#$(TARGET): $(OBJ)
#	${LD} -o $@ $^ ${LDFLAGS}


#measure_timestamp: measure_timestamp.o rudp.o rtt.o share.o tcp_vegas.o timely_reno.o queue.o 
#	${LD} -o $@ $^ ${LDFLAGS}

#main: common.o  main.o rudp.o 
#	${LD} -o $@ $^ ${LDFLAGS}

#rudp.o : rudp.c share.o tcp_vegas.o timely_reno.o queue.o 
#	$(LD) $(CFLAGS) -c rudp.c share.o tcp_vegas.o timely_reno.o queue.o

all: server client 

server: server.o 
	${LD} -o $@ $^ ${LDFLAGS}
client: client.o 
	${LD} -o $@ $^ ${LDFLAGS}


	
PHONY: clean
clean:
	rm -f *.o server client 
