#define __USE_GNU
#define _GNU_SOURCE

#include <sched.h>


#include "util.h"

void bindingCPU(int num){
	int result;
	cpu_set_t mask;
	CPU_ZERO(&mask);
	CPU_SET(num, &mask);
	result = sched_setaffinity(0, sizeof(mask), &mask);
	if (result < 0){
		printf("binding CPU fails\n");
		exit(1);
	}

}

void die(const char *reason)
{
	fprintf(stderr, "%s\n", reason);
	exit(EXIT_FAILURE);
}
