#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <unistd.h>
#include <pthread.h>
struct key_value {
  int key;
  char* value; // start of 100 bytes
};

struct key_value * input_kv;

size_t size_of_record = sizeof(struct key_value);

struct qsort_args {
    struct key_value * base; 
    size_t nel;
};

struct merge_args {
    struct run* run1;
    struct run* run2;
};

// in each pass, merge multiple runs by 2
struct run {
    int start; 
    int end; 
    // end - start = numRecords_in_run
};

struct pass {
    struct run *runs;
    int numRuns;
};

//define compare function according to key converted to int
static int compare(const void *a, const void *b) {
    struct key_value *ia = (struct key_value *)a;
    struct key_value *ib = (struct key_value *)b;
    if (ia->key < ib->key) {
        return -1;
    } else if (ia->key > ib->key) {
        return 1;
    } else {
        return 0;
    }
}

// merge two sorted arrays
void merge(struct run run1, struct run run2)
{   
    struct key_value *buffer = malloc(size_of_record * (run1.end - run1.start+ run2.end - run2.start + 2));
    int i = run1.start; // index of left array
    int j = run2.start; // index of right array
    int k = 0; // index of buffer
    while (i <= run1.end && j <= run2.end) { 
        if (compare(input_kv + i, input_kv + j) < 0) {
            buffer[k++] = input_kv[i++];
        } else {
            buffer[k++] = input_kv[j++];
        }
        // fprintf(stdout, "%d ", buffer[k-1].key);
    }
    while (i <= run1.end) {
        buffer[k++] = input_kv[i++];
    }
    while (j <= run2.end) {
        buffer[k++] = input_kv[j++];
    }
    memcpy(input_kv + run1.start, buffer, (run1.end - run1.start + 1) * size_of_record);
    memcpy(input_kv + run2.start, buffer + (run1.end - run1.start + 1), (run2.end - run2.start + 1) * size_of_record); 
    free(buffer);
}

// enclosed function of merge that can be called by pthread_create
void * merge_enclosed(void * args) {
    struct merge_args* merge_args = (struct merge_args*) args;
    struct run* run1 = merge_args->run1;
    struct run* run2 = merge_args->run2;
    // printf("Merging ");
    // fprintf(stdout, "run1: %d - %d, ", run1->start, run1->end);
    // fprintf(stdout, "run2: %d - %d\n", run2->start, run2->end);
    merge(*run1, *run2);
    // fprintf(stdout, "merge_enclosed done.\n");
    return NULL;
};

// In each pass, allocate threads to merge runs by 2 in parallel
// There is room for improvement, as parallelism is limited in each pass
void mergeAll(struct pass* pass) {
    int i;
    // merge by 2, leave the last run if there is an odd number of runs
    pthread_t* pthreads = malloc(pass->numRuns / 2 * sizeof(pthread_t));
    // prepare an array of merge_args
    struct merge_args * merge_args = malloc(pass->numRuns / 2 * sizeof(struct merge_args));

    for (int j = 0; j < pass->numRuns / 2; j++) {
        merge_args[j].run1 = &pass->runs[2*j];
        merge_args[j].run2 = &pass->runs[2*j+1];
    }
    // create threads
    for(int j = 0; j < pass->numRuns / 2; j++) {
        // fprintf(stdout, "thread %d: merge_args: run1: %d - %d, run2: %d - %d\n", j, merge_args[j].run1->start, merge_args[j].run1->end, merge_args[j].run2->start, merge_args[j].run2->end);
        pthread_create(&pthreads[j], NULL, merge_enclosed, &merge_args[j]);
    }
    // join all threads
    for (int j = 0; j < pass->numRuns/2; j++) {
        pthread_join(pthreads[j], NULL);
    }
    // Update pass in place, preparing for the next pass
    for (i = 0; i < pass->numRuns / 2; i++) {
        pass->runs[i].start = pass->runs[2*i].start;
        pass->runs[i].end = pass->runs[2*i+1].end;
    }
    if (pass -> numRuns % 2 == 1) {
        pass->runs[pass->numRuns / 2].start = pass->runs[pass->numRuns-1].start;
        pass->runs[pass->numRuns / 2].end = pass->runs[pass->numRuns-1].end;
    }
    pass->numRuns = pass->numRuns / 2 + pass->numRuns % 2;

    free(pthreads);
    free(merge_args);
};

// enclosed function of qsort that can be called by pthread_create
void *qsort_enclosed(void *args) {
    // fprintf(stdout, "qsort_enclosed\n");
    struct qsort_args * range = (struct qsort_args *) args;
    struct key_value * base = range->base;
    size_t nel = range->nel;
    qsort(base, nel, size_of_record, compare);
    return NULL;
};

void parallel_sort(size_t numRecords, int numThreads)
{
    // ---------------------- stage 1: qsort ----------------------
    // divide data to chunks for qsort
    int numRecords_per_chunk = numRecords / numThreads;
    int numRecords_last_chunk = numRecords - numRecords_per_chunk * (numThreads - 1);
    
    struct run * runs = malloc(numThreads * sizeof(struct run));    
    
    pthread_t * pthreads = malloc(sizeof(pthread_t) * numThreads);
    struct qsort_args * range = malloc(sizeof(struct qsort_args) * numThreads);
    for (int i = 0; i < numThreads; i++) {
        range[i].base = (void *) input_kv + i * numRecords_per_chunk * size_of_record; // address of the first element in the chunk
        runs[i].start = i * numRecords_per_chunk; // index of the first element in the chunk
        range[i].nel = numRecords_per_chunk; // number of elements in the chunk
        if (i == numThreads - 1) {
            range[i].nel = numRecords_last_chunk;
        }
        runs[i].end = runs[i].start + range[i].nel - 1; // index of the last element in the chunk, inclusive 
    }
    // allocate threads to qsort
    for (int i = 0; i < numThreads; i++) {
        pthread_create(&pthreads[i], NULL, qsort_enclosed, &range[i]);
    }
    // join all the thread
    for (int i = 0; i < numThreads; i++) {
        pthread_join(pthreads[i], NULL);
    }
    free(pthreads);
    free(range);

    // ---------------------- stage 1: merge sort ----------------------
    struct pass* pass = malloc(sizeof(struct pass));
    pass->numRuns = numThreads;
    pass->runs = runs;
    // merge the sorted chunks through a few passes
    while (pass->numRuns > 1) {
        mergeAll(pass);
    }
    free(runs);
    free(pass);
}

// Your parallel sort (`psort`) will take three command-line arguments.
// input                  The input file to read records for sort
// output               The output file where records will be written after sort
// numThreads      Number of threads that shall perform the sort operation.
int main(int argc, char *argv[])
{
    if (argc != 4) {
        fprintf(stderr, "Usage: %s input output numThreads\n", argv[0]);
        exit(1);
    }
    char* input = argv[1];
    char* output = argv[2];
    int numThreads = atoi(argv[3]);
    printf("numThreads: %d", numThreads);

    // open input file
    int input_fd = open(input, O_RDONLY);
    if (input_fd < 0) {
        fprintf(stderr, "Error: cannot open file %s", input);
        exit(1);
    }
    // get file size
    struct stat sb;
    if (fstat(input_fd, &sb) == -1) {
        perror("Error getting file size");
        close(input_fd);
        return 1;
    }
    unsigned int fileSize = sb.st_size;

    // Memory map the input file
    void *input_data = mmap(NULL, fileSize, PROT_READ, MAP_PRIVATE, input_fd, 0);
    if (input_data == MAP_FAILED) {
        fprintf(stderr, "Error: cannot mmap file %s", input);
        exit(1);
    }

    // Open output file
    int output_fd = open(output, O_RDWR | O_CREAT | O_TRUNC, 0666);
    if (output_fd == -1) {
        perror("Error opening output file");
        munmap(input_data, fileSize);
        close(input_fd);
        return 1;
    }

    // Set output file size
    if (ftruncate(output_fd, fileSize) == -1) {
        perror("Error setting output file size");
        munmap(input_data, fileSize);
        close(input_fd);
        close(output_fd);
        return 1;
    }

    // Memory map the output file
    void *output_data = mmap(NULL, fileSize, PROT_WRITE, MAP_SHARED, output_fd, 0);
    if (output_data == MAP_FAILED) {
        perror("Error mapping output file to memory");
        munmap(input_data, fileSize);
        close(input_fd);
        close(output_fd);
        return 1;
    }
    
    void *ptr = input_data;
    input_kv = malloc(fileSize/100 * sizeof(struct key_value));
    for (int i = 0; i < fileSize/100; i++) {
        int key = *(int*)ptr;
        input_kv[i].key = key;
        input_kv[i].value = ptr;
        ptr += 100;
    }
    
    long start, end;
    struct timeval timecheck;

    gettimeofday(&timecheck, NULL);
    start = (long)timecheck.tv_sec * 1000 + (long)timecheck.tv_usec / 1000;

    // sort value according to key
    parallel_sort(fileSize/100, numThreads);
    // qsort(input_kv, fileSize/100, size_of_record, compare);
    // simple qsort
    // qsort(input_kv, fileSize/100, sizeof(struct key_value), compare);

    gettimeofday(&timecheck, NULL);
    end = (long)timecheck.tv_sec * 1000 + (long)timecheck.tv_usec / 1000;

    printf(" %ld milliseconds elapsed\n", (end - start));

    // for (int i = 0; i < fileSize/100; i++) {
    //     printf("%d\n", input_kv[i].key);
    // }
    // printf("writing out.\n");
    // write to output_data according to input_kv
    for (int i = 0; i < fileSize/100; i++) {
        memcpy(output_data + i*100, input_kv[i].value, 100);
    }
    free(input_kv);
    // Sync output file to disk
    if (msync(output_data, fileSize, MS_SYNC) == -1) {
        perror("Error syncing output file to disk");
    }
    // Unmap memory and close files
    munmap(input_data, fileSize);
    munmap(output_data, fileSize);
    close(input_fd);
    close(output_fd);

    return 0;
}