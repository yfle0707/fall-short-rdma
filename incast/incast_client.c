#include <assert.h>
#include <time.h>
#include <stdlib.h>
#include <stdint.h>
#include <math.h> //for exp()
#include <sys/time.h> //for gettimeofday()
#include <sys/epoll.h>
#include "csapp.h"

#define XCLOCK_GETTIME(tp)   ({  \
    if (clock_gettime(CLOCK_REALTIME, (tp)) != 0) {                   \
        printf("XCLOCK_GETTIME failed!\n");              \
        exit(-1);                                            \
    }})

#define XEPOLL_CTL(efd, flags, fd, event)   ({  \
    if (epoll_ctl(efd, flags, fd, event) < 0) { \
        perror ("epoll_ctl");                   \
        exit (-1);                              \
    }})


#define WRITE_BUF_SIZE 128	/* Per-connection internal buffer size for writes. */
#define READ_BUF_SIZE (1<<25) /* Read buffer size. 32MB. */
#define MAXEVENTS 1024  /* Maximum number of epoll events per call */

static uint8_t read_buf[READ_BUF_SIZE];

int startSecs = 0;

/*
 * Data structure to keep track of server connection state.
 *
 * The connection objects are also maintained in a global doubly-linked list.
 * There is a dummy connection head at the beginning of the list.
 */
struct conn {
    /* Points to the previous connection object in the doubly-linked list. */
    struct conn *prev;	
    /* Points to the next connection object in the doubly-linked list. */
    struct conn *next;	
    /* File descriptor associated with this connection. */
    int fd;			
    /* Internal buffer to temporarily store the contents of a read. */
    char buffer[WRITE_BUF_SIZE];	
    /* Size of the data stored in the buffer. */
    size_t size;			
    /* The incast that this connection is a part of. */
    struct incast *incast;

    /* Number of bytes requested for the current message. */
    uint64_t request_bytes;
    /* Number of bytes of the current message read. */
    uint64_t read_bytes;		

    /* The startting time of the connection. */
    struct timespec start;
    /* The finishing time of the connection. */
    struct timespec finish;
};

/*
 * Data structure to keep track of each independent incast event.
 * Each incast holds the completion count and timers.
 */
struct incast {
    /* Points to the previous incast object in the doubly-linked list. */
    struct incast *prev;
    /* Points to the next incast object in the doubly-linked list. */
    struct incast *next;
    /* The server request unit. */
    uint64_t sru;
    /* The total number of requests sent out. */
    uint32_t numreqs;
    /* The number of completed requests so far. */
    uint32_t completed;
    /* Doubly-linked list of active server connection objects. */
    struct conn *conn_head;

    /* The startting time of the incast. */
    struct timespec start;
    /* The finishing time of the incast. */
    struct timespec finish;
};

/* 
 * Data structure to keep track of active server connections.
 */
struct incast_pool { 
    /* The epoll file descriptor. */
    int efd;
    /* The epoll events. */
    struct epoll_event events[MAXEVENTS];
    /* Number of ready events returned by epoll. */
    int nevents;  	  		
    /* Doubly-linked list of active server connection objects. */
    struct incast *incast_head;
    /* Number of active incasts. */
    //unsigned int nr_incasts;
    /* Number of active connections. */
    unsigned int nr_conns;

}; 

/* Set verbosity to 1 for debugging. */
static int verbose = 0;

/* Local function definitions. */
static void increment_and_check_incast(struct incast *inc);

double get_secs(struct timespec time)
{
    return (time.tv_sec) + (time.tv_nsec * 1e-9);
}

double get_secs_since_start(struct timespec time, long startsecs)
{
    return get_secs(time) - startsecs;
}

/*
 * open_server - open connection to server at <hostname, port> 
 *   and return a socket descriptor ready for reading and writing.
 *   Returns -1 and sets errno on Unix error. 
 *   Returns -2 and sets h_errno on DNS (gethostbyname) error.
 */
int open_server(char *hostname, int port) 
{
    int serverfd;
    struct hostent *hp;
    struct sockaddr_in serveraddr;
    int opts = 0;
    int optval = 1;

    if ((serverfd = socket(AF_INET, SOCK_STREAM, 0)) < 0)
        return -1; /* check errno for cause of error */

    /* Set the connection descriptor to be non-blocking. */
    opts = fcntl(serverfd, F_GETFL);
    if (opts < 0) {
        printf("fcntl error.\n");
        exit(-1);
    }
    opts = (opts | O_NONBLOCK);
    if (fcntl(serverfd, F_SETFL, opts) < 0) {
        printf("fcntl set error.\n");
        exit(-1);
    }
    
    /* Set the connection to allow for reuse */
    if (setsockopt(serverfd, SOL_SOCKET, SO_REUSEADDR, &optval, sizeof(optval)) < 0) {
        printf("setsockopt error.\n");
        exit(-1);
    }

    /* Fill in the server's IP address and port */
    if ((hp = gethostbyname(hostname)) == NULL)
        return -2; /* check h_errno for cause of error */
    bzero((char *) &serveraddr, sizeof(serveraddr));
    serveraddr.sin_family = AF_INET;
    bcopy((char *)hp->h_addr, 
          (char *)&serveraddr.sin_addr.s_addr, hp->h_length);
    serveraddr.sin_port = htons(port);

    /* Establish a connection with the server */
    if (connect(serverfd, (SA *) &serveraddr, sizeof(serveraddr)) < 0) {
        if (errno !=EINPROGRESS) {
            printf("server connect error");
            perror("");
            return -1;
        }     
    }
    return serverfd;
}

/*******************************************************************************
 * Maintaining Client Connections.
 ******************************************************************************/

/* 
 * Requires:
 * c should be a connection object and not be NULL.
 * p should be a connection pool and not be NULL.
 *
 * Effects:
 * Adds the connection object to the tail of the doubly-linked list.
 */
static void
add_conn_list(struct conn *c, struct incast *i)
{
	c->next = i->conn_head->next;
	c->prev = i->conn_head;
	i->conn_head->next->prev = c;
	i->conn_head->next = c;
}

/* 
 * Requires:
 * c should be a connection object and not be NULL.
 *
 * Effects:
 * Removes the connection object from the doubly-linked list.
 */
static void
remove_conn_list(struct conn *c)
{
	c->next->prev = c->prev;
	c->prev->next = c->next;
}

/* 
 * Requires:
 * c should be a connection object and not be NULL.
 * inc should be an incast and not be NULL.
 * p should be an incast pool and not be NULL.
 *
 * Effects:
 * Closes a server connection and cleans up the associated state. Removes it
 * from the doubly-linked list and frees the connection object.
 */
void
remove_server(struct conn *c, struct incast_pool *p)
{
	if (verbose)
		printf("Closing connection fd %d...\n", c->fd);
 
        /* Supposedly closing the file descriptor cleans up epoll,
         * but do it first anyways to be nice... */
         XEPOLL_CTL(p->efd, EPOLL_CTL_DEL, c->fd, NULL);

	/* Close the file descriptor. */
	Close(c->fd); 
	
	/* Decrement the number of connections. */
	p->nr_conns--;

	/* Remove the connection from the list. */
	remove_conn_list(c);

        /* Stop Timing and Log. */
        XCLOCK_GETTIME(&c->finish);
        double tot_time = get_secs(c->finish) - get_secs(c->start);
        double bandwidth = (c->read_bytes * 8.0 )/(1024.0 * 1024.0 * 1024.0 * tot_time);
        printf("- [%.9g, %lu, %.9g, 1]\n", tot_time, c->read_bytes, bandwidth);

        /* Error Checking. */
        if (c->read_bytes != c->request_bytes) {
            printf("Read bytes (%lu) != Request bytes (%lu) for fd %d\n",
                c->read_bytes, c->request_bytes, c->fd);
            fprintf(stderr, "Read bytes (%lu) != Request bytes (%lu) for fd %d\n",
                c->read_bytes, c->request_bytes, c->fd);
        }

        /* Increment and check the number of finished connections. */
        increment_and_check_incast(c->incast);

	/* Free the connection object. */
	Free(c);
}

/* 
 * Requires:
 * connfd should be a valid connection descriptor.
 * inc should be an incast and not be NULL.
 *
 * Effects:
 * Allocates a new connection object and initializes the associated state. Adds
 * it to the doubly-linked list.
 */
static void 
add_server(int connfd, struct timespec start, struct incast *inc, struct incast_pool *p) 
{
    struct conn *new_conn;
    struct epoll_event event;

    /* Allocate a new connection object. */
    new_conn = Malloc(sizeof(struct conn));

    new_conn->fd = connfd;
    new_conn->size = 0;
    new_conn->incast = inc;

    /* No bytes have been requested or written yet. */
    new_conn->request_bytes = 0;
    new_conn->read_bytes = 0;

    /* Add this descriptor to the write descriptor set. */
    event.data.fd = connfd;
    event.data.ptr = new_conn;
    event.events = EPOLLOUT;
    XEPOLL_CTL(p->efd, EPOLL_CTL_ADD, connfd, &event);

    /* Update the number of server connections. */
    p->nr_conns++;

    new_conn->start = start;

    add_conn_list(new_conn, inc);
}

/* 
 * Requires:
 * hostname should be a valid hostname
 * port should be a valid port
 * p should be a connection pool and not be NULL.
 *
 * Effects:
 * Accepts a new server connection. Sets the resulting connection file
 * descriptor to be non-blocking. Adds the server to the connection pool.
 */
static void
start_new_connection(char *hostname, int port, struct incast *inc, struct incast_pool *p)
{
    int connfd;
    struct timespec start;

    //XXX: Wrong spot for this
    /* Start Timing. */
    XCLOCK_GETTIME(&start);

    /* Accept the new connection. */
    connfd = open_server(hostname, port);
    if (connfd < 0) {
        printf("# Unable to open connection to (%s, %d)\n", hostname, port);
        return;
        //exit(-1);
    }

    if (verbose) {
        printf("Started new connection with (%s %d) new fd %d...\n",
            hostname, port, connfd);
    }

    /* Create new connection object and add it to the connection pool. */
    add_server(connfd, start, inc, p);
}

/*******************************************************************************
 * Manage Incast Events.
 ******************************************************************************/

/* 
 * Requires:
 * sru should be the server request unit in bytes.
 * numreqs should be the number of requests
 *
 * Effects:
 * Allocates an incast object and initializes it. 
 */
static struct incast *
alloc_incast(uint64_t sru, uint32_t numreqs)
{
	struct incast *inc;
	
	if (verbose)
		printf("Allocating incast\n");
	
	inc = Malloc(sizeof(struct incast));
        inc->sru = sru;
        inc->numreqs = numreqs;
        inc->completed = 0;

	/* Allocate and initialize the dummy connection head. */
	inc->conn_head = Malloc(sizeof(struct conn));
	inc->conn_head->next = inc->conn_head;
	inc->conn_head->prev = inc->conn_head;

        /* Start Timing. */
        XCLOCK_GETTIME(&inc->start);

	return (inc);
}

/* 
 * Requires:
 * inc should be an incast object and not be NULL.
 *
 * Effects:
 * Frees the incast object and the memory holding the contents of the message.
 */
static void
free_incast(struct incast *inc)
{
	if (verbose)
		printf("Freeing incast\n");

        Free(inc->conn_head);
	Free(inc);
    exit(0);
}

/* 
 * Requires:
 * inc should be an incast object and not be NULL.
 *
 * Effects:
 * Removes the incast object from the doubly-linked list.
 */
static void
remove_incast_list(struct incast *inc)
{
	inc->next->prev = inc->prev;
	inc->prev->next = inc->next;
}

/* 
 * Requires:
 * inc should be an incast object and not be NULL.
 *
 * Effects:
 * Adds the incast object to the tail of the doubly-linked list.
 */
static void
add_incast_list(struct incast *inc, struct incast_pool *p)
{
	inc->next = p->incast_head;
	inc->prev = p->incast_head->prev;
	p->incast_head->prev->next = inc;
	p->incast_head->prev = inc;
}

/* 
 * Requires:
 * sru is the server request unit
 * numreqs is the number of servers to the request from
 * hosts is an array of the form [hostname, port]*
 * hostsc is the length of the hosts array
 * p is a valid incast pool and is not NULL
 *
 * Effects:
 * Starts the incast event.
 */
static void
start_incast(uint64_t sru, uint32_t numreqs, char **hosts, struct incast_pool *p)
{
    int i, port;
    char *hostname;
    struct incast *inc;

    /* Alloc the incast and add it to the incast_pool */
    inc = alloc_incast(sru, numreqs);
    add_incast_list(inc, p);
    //p->nr_incasts++;

    /* Connect to the servers */
    for (i = 0; i < (numreqs*2); i=i+2) {
        hostname = hosts[i];
        port = atoi(hosts[i + 1]);
        if (verbose)
            printf("Starting new connection to (%s %d)...\n", hostname, port);
        start_new_connection(hostname, port, inc, p);
    }
}

/* 
 * Requires:
 * inc is a valid incast object ans is not NULL
 * p is a valid incast pool and is not NULL
 *
 * Effects:
 * Finishes the incast event.
 */
static void
//finish_incast(struct incast *inc, struct incast_pool *p)
finish_incast(struct incast *inc)
{
    /* Assert that there are no connections left for this incast. */
    assert(inc->conn_head->next == inc->conn_head);

    /* Assert that the incast finished. */
    assert(inc->numreqs == inc->completed);

    /* Remove this incast from the pool. */
    remove_incast_list(inc);
    //p->nr_incasts--;

    if (verbose)
        printf("Finished receiving %lu bytes from each of the %d servers\n",
            inc->sru, inc->numreqs);

    /* Stop Timing and Log. */
    XCLOCK_GETTIME(&inc->finish);
    double tot_time = get_secs(inc->finish) - get_secs(inc->start);
    double bandwidth = (inc->sru * inc->numreqs * 8.0)/(1024.0 * 1024.0 * 1024.0 * tot_time);
    printf("- [%.9g, %lu, %.9g, %d]\n", tot_time, inc->sru * inc->numreqs, bandwidth, inc->numreqs);

    /* Free the incast. */
    free_incast(inc);
}

/* 
 * Requires:
 * inc is a valid incast object ans is not NULL
 *
 * Effects:
 * Increments the completed count for the incast.
 * Finishes the incast event if it is finished.
 */
static void
increment_and_check_incast(struct incast *inc)
{
    inc->completed++;
    if (inc->completed == inc->numreqs)
        finish_incast(inc);
}

/* 
 * Requires:
 * p should be an incast pool and not be NULL.
 *
 * Effects:
 * Initializes an empty incast pool. Allocates and initializes dummy list
 * heads.
 */
static void 
init_pool(struct incast_pool *p) 
{
    /* Initially, there are no connected descriptors. */
    //p->nr_incasts = 0;                   
    p->nr_conns = 0;

    /* Allocate and initialize the dummy connection head. */
    p->incast_head = Malloc(sizeof(struct incast));
    p->incast_head->next = p->incast_head;
    p->incast_head->prev = p->incast_head;

    /* Init epoll. */
    p->efd = epoll_create1(0);
    if (p->efd < 0) {
        printf ("epoll_create error!\n");
        exit(1);
    }
}

/*******************************************************************************
 * Read and Write Messages.
 ******************************************************************************/

/* 
 * Requires:
 * p should be a connection pool and not be NULL.
 *
 * Effects:
 * Reads from each ready file descriptor in the read set and handles the
 * incoming messages appropriately.
 */
static void
read_message(struct conn *c, struct incast_pool *p)
{
    int n;

    /* Read from that socket. */
    n = recv(c->fd, read_buf, READ_BUF_SIZE, 0);

    /* Data read. */
    if (n > 0) {

        c->read_bytes += n;
        if (verbose) {
            printf("Read %d bytes from fd %d:\n", n, c->fd);
        }

        if (c->read_bytes >= c->request_bytes) {
            /* We have finished the request. */
            if (verbose) {
                printf("Finished reading %lu bytes from fd %d:\n", c->read_bytes, c->fd);
            }
            remove_server(c, p);
        }
    }
    /* Error (possibly). */
    else if (n < 0) {
         /* If errno is EAGAIN, it just means we need to read again. */
        if (errno != EAGAIN) {
            fprintf(stderr, "Unable to read response from fd %d!\n",
                c->fd);
            remove_server(c, p);
        }
    }
    /* Connection closed by server. */
    else {
        remove_server(c, p);
    }
}

/* 
 * Requires:
 * p should be a connection pool and not be NULL.
 *
 * Effects:
 * Writes the appropriate messages to each ready file descriptor in the write set.
 */
static void
write_message(struct conn *c, struct incast_pool *p)
{
    int n;
    uint64_t sru, net_sru;
    struct epoll_event event;

    /* Perform the write system call. */
    sru = c->incast->sru;
    net_sru = htobe64(sru);
    n = write(c->fd, &net_sru, sizeof(net_sru));

    /* Data written. */
    if (n == sizeof(sru)) {
        if (verbose)
            printf("Finished requesting %lu bytes from fd %d.\n",
                sru, c->fd);
        c->request_bytes = sru;
        /* The request size has been received, update epoll.
         * Remove it from the write set and add it to the 
         * read set. */
        event.data.fd = c->fd;
        event.data.ptr = c;
        event.events = EPOLLIN;
        XEPOLL_CTL(p->efd, EPOLL_CTL_MOD, c->fd, &event);
    }
    /* Error (possibly). */
    else if (n < 0) {
        /* If errno is EAGAIN, it just means we have to write again. */
        if (errno != EAGAIN) {
            fprintf(stderr, "Unable to write request to fd %d!\n",
                c->fd);
            remove_server(c, p);
        }
    }
    /* Connection closed by server. */
    else {
        fprintf(stderr, "Unable to write request to fd %d!\n",
            c->fd);
        remove_server(c, p);	
    }
}

int main(int argc, char **argv) 
{
    uint64_t sru;
    struct incast_pool pool;
    struct conn *connp;
    int i;
        
    /* initialize random() */
    srandom(time(0));

    if (verbose)
            printf("Starting incast client...\n");

    if ((argc % 2) != 0 || argc < 4) {
        fprintf(stderr, "usage: %s <server-request-unit-bytes> [<hostname> <port>]+\n", argv[0]);
        exit(0);
    }

    sru = strtoll(argv[1], NULL, 10);

    /* print the command invoked with args */
    printf("# %s", argv[0]);
    for(i=1 ; i<argc ; i++){
        printf(" %s", argv[i]);
    }
    printf("\n");

    /* Initialize the connection pool. */
    init_pool(&pool);

    /* Start an incast. */
    start_incast(sru, (argc-2)/2, &argv[2], &pool);

    printf("- [totTime, totBytes, bandwidthGbps, numSockets]\n");
    while(1)
    { 
        /* 
         * Wait until:
         * 1. Socket is ready for request to be written.
         * 2. Data is available to be read from a socket. 
         */
        pool.nevents = epoll_wait (pool.efd, pool.events, MAXEVENTS, 0);
        for (i = 0; i < pool.nevents; i++) {
            if ((pool.events[i].events & EPOLLERR) ||
                (pool.events[i].events & EPOLLHUP)) {
                    /* An error has occured on this fd */
                fprintf (stderr, "epoll error\n");
                perror("");
                close (pool.events[i].data.fd);
                continue;
            }

            /* Handle Reads. */
            if (pool.events[i].events & EPOLLIN) {
                connp = (struct conn *) pool.events[i].data.ptr;
                read_message(connp, &pool);
            }

            /* Handle Writes. */
            if (pool.events[i].events & EPOLLOUT) {
                connp = (struct conn *) pool.events[i].data.ptr;
                write_message(connp, &pool);
            }
        }
    }
        
    return (0);
}
