//stav barazani 
#include <stdio.h>
#include <stdlib.h>
#include <signal.h>
#include <unistd.h>
#include <sys/wait.h>
#include "threadpool.h"

/**
 * create_threadpool creates a fixed-sized thread
 * pool.  If the function succeeds, it returns a (non-NULL)
 * "threadpool", else it returns NULL.
 * this function:
 * 1. input sanity check 
 * 2. initialize the threadpool structure
 * 3. initialized mutex and conditional variables
 * 4. create the threads, the thread init function is do_work and its argument is the initialized threadpool. 
 */

threadpool* create_threadpool(int num_threads_in_pool)
{
	if(num_threads_in_pool <= 0 || num_threads_in_pool > MAXT_IN_POOL)
	{
		fprintf(stderr,"error input\n");
		return(NULL);
	}

	threadpool* thread_pool = (threadpool*)calloc(1,sizeof(threadpool));
	if(thread_pool == NULL)
	{
		perror("calloc threadpool");
		return(NULL);
	}
	
	thread_pool->num_threads = num_threads_in_pool;
	thread_pool->qsize = 0;
	thread_pool->qhead = NULL;
	thread_pool->qtail = NULL;
	
	if(pthread_mutex_init(&thread_pool->qlock, NULL) != 0)
	{
		fprintf(stderr,"mutex init\n");
		free(thread_pool);
		return(NULL);
	} 
	if(pthread_cond_init(&thread_pool->q_not_empty, NULL) != 0)
	{
		fprintf(stderr,"condition init\n");
		free(thread_pool);
		return(NULL);
	}
	if(pthread_cond_init(&thread_pool->q_empty, NULL) != 0)
	{
		fprintf(stderr,"condition init\n");
		free(thread_pool);
		return(NULL);	
	}
	
	thread_pool->shutdown = 0;
	thread_pool->dont_accept = 0;
 
	thread_pool->threads = (pthread_t*)calloc(num_threads_in_pool,sizeof(pthread_t));
	if(thread_pool->threads == NULL)
	{
		perror("calloc threads");
		free(thread_pool);
		return(NULL);
	}

	int i, status;
	for(i = 0;i < num_threads_in_pool; i++) 
	{ 
		status = pthread_create(&(thread_pool->threads[i]),NULL,do_work,thread_pool);
		 if (status != 0) 
		{
			fprintf(stderr,"pthread_create failed %d, thread_pool %p \n",i,thread_pool);
			thread_pool->num_threads=i;
			destroy_threadpool(thread_pool);
			return NULL;
		}
	 }
	return(thread_pool);
}
/**
 * dispatch enter a "job" of type work_t into the queue.
 * when an available thread takes a job from the queue, it will
 * call the function "dispatch_to_here" with argument "arg".
 * this function:
 * 1. create and init work_t element
 * 2. lock the mutex
 * 3. add the work_t element to the queue
 * 4. unlock mutex
 *
 */
void dispatch(threadpool* from_me, dispatch_fn dispatch_to_here, void *arg)
{
	if(from_me == NULL ||dispatch_to_here == NULL)
		return;	

	if(pthread_mutex_lock(&(from_me->qlock)) != 0)
	{
		fprintf(stderr,"error lock\n");
		return;
	}

	if(from_me->dont_accept)
	{
		if(pthread_mutex_unlock(&(from_me->qlock)) != 0)
			fprintf(stderr,"error in unlock\n");
		return;
	}
	
	work_t *work;
	work = (work_t*)calloc(1,sizeof(work_t));
	if(work == NULL)
	{
		perror("calloc work");
		if(pthread_mutex_unlock(&(from_me->qlock)) != 0)
			fprintf(stderr,"error in unlock\n");
		return;
	}
	work->arg = arg;
	work->routine = dispatch_to_here;
	work->next = NULL;

	if(!from_me->qsize)
	{
		from_me->qhead = work;
		from_me->qtail = work;
	}
	else
	{
		from_me->qtail->next = work;
		from_me->qtail = work;
	}
	
	from_me->qsize++;

	if(pthread_cond_signal(&from_me->q_empty) != 0)
	{
		fprintf(stderr,"signal\n");
		return;
	}
	if(pthread_mutex_unlock(&(from_me->qlock)) != 0)
	{
		fprintf(stderr,"error in unlock\n");
		return;
	}
}
/**
 * The work function of the thread
 * this function:
 * 1. lock mutex
 * 2. if the queue is empty, wait
 * 3. take the first element from the queue (work_t)
 * 4. unlock mutex
 * 5. call the thread routine
 *
 */
void* do_work(void* p)
{
	if(p == NULL)
		return (void*)-1;

	work_t *work;
	threadpool *thread_pool;
	thread_pool = (threadpool*)p;
	
	while(1)
	{
		if(pthread_mutex_lock(&(thread_pool->qlock)) != 0)
		{
			fprintf(stderr,"error in lock\n");
			return (void*)-1;
		}
		if(thread_pool->shutdown)
		{
			if(pthread_mutex_unlock(&(thread_pool->qlock))!=0)
			{
				fprintf(stderr,"error unlock");
				return (void*)-1;
			}
			return (void*)0;
		}

		if(thread_pool->qsize == 0) 
		{
			if(pthread_cond_wait(&(thread_pool->q_empty),&(thread_pool->qlock)) != 0)
			{
				fprintf(stderr,"error wait_con\n");
				return (void*)-1;
			}
		}
		if(thread_pool->shutdown)
		{
			if(pthread_mutex_unlock(&(thread_pool->qlock)) != 0)
			{
				fprintf(stderr,"error unlock\n");
				return (void*)-1;
			}
			return (void*)0;
		}

		work = thread_pool->qhead;
		if(work == NULL)
		{
			printf("null\n");
			if(pthread_mutex_unlock(&(thread_pool->qlock)) !=0)
			{
				fprintf(stderr,"error unlock\n");
				return (void*)-1;
			}
		}
		else
		{
			thread_pool->qsize--;
			thread_pool->qhead = thread_pool->qhead->next;
			if(thread_pool->qsize == 0)
			{
				thread_pool->qtail = NULL;
				if(pthread_cond_signal(&thread_pool->q_not_empty) != 0)
				{
					fprintf(stderr,"error signal\n");
					free(work);
					return (void*)-1;
				}
			}
			if(pthread_mutex_unlock(&(thread_pool->qlock)) !=0)
			{
				fprintf(stderr,"error unlock\n");
				free(work);
				return (void*)-1;
			}
			(work->routine)(work->arg); //do work
			free(work);
		}
	}
	return (void*)0;
}
/**
 * destroy_threadpool kills the threadpool, causing
 * all threads in it to commit suicide, and then
 * frees all the memory associated with the threadpool.
 */
void destroy_threadpool(threadpool* destroyme)
{
	if(destroyme == NULL)
		return;
	if(pthread_mutex_lock(&destroyme->qlock) !=0)
	{
		fprintf(stderr,"error lock\n");
		return;
	}

	//dont accetpt new work- for the thispach, dont insert to queue new job.	
	destroyme->dont_accept =1;  
	
	//queue not empty- wait until all jobs will be completed
	if(destroyme->qsize != 0)
	{
		if(pthread_cond_wait(&destroyme->q_not_empty,&destroyme->qlock) != 0)
		{
			fprintf(stderr,"error in wait\n");
			return;
		}
	}
	//queue empty	
	 destroyme->shutdown =1;  //shout down- for the theads-to do exit()
	
	//signal for all threads
	if(pthread_cond_broadcast(&(destroyme->q_empty)) != 0)
	{
		fprintf(stderr,"error in broadcast\n");
		return; 
	}
	if(pthread_mutex_unlock(&destroyme->qlock) != 0)
	{
		fprintf(stderr,"error in unlock\n");
		return;
	}
	 
	//join for each treads to check they finish their lobs
	void *join;
	int i;
	for (i= 0; i< destroyme->num_threads; i++) 
		pthread_join(destroyme->threads[i], &join);
		
	free(destroyme->threads);
	destroyme->threads = NULL;
	pthread_mutex_destroy(&(destroyme->qlock));
	pthread_cond_destroy(&(destroyme->q_empty));
	pthread_cond_destroy(&(destroyme->q_not_empty));
	free(destroyme);
}
