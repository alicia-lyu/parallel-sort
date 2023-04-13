#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/stat.h>
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

// in each pass, merge multiple runs by 2
struct run {
    int start; 
    int end; 
    // end - start = numRecords_in_run
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
void merge(struct run run1, struct run run2, struct key_value *buffer)
{   
    int i = run1.start; // index of left array
    int j = run2.start; // index of right array
    int k = 0; // index of buffer
    while (i <= run1.end && j <= run2.end) { 
        if (compare(input_kv + i, input_kv + j) < 0) {
            buffer[k++] = input_kv[i++];
        } else {
            buffer[k++] = input_kv[j++];
        }
    }
    while (i <= run1.end) {
        buffer[k++] = input_kv[i++];
    }
    while (j <= run2.end) {
        buffer[k++] = input_kv[j++];
    }
    memcpy(input_kv + run1.start, buffer, (run1.start - run1.end + 1) * size_of_record);
    memcpy(input_kv + run2.start, buffer + (run1.start - run1.end + 1), (run2.start - run2.end + 1) * size_of_record); 
}


struct run* runs mergeAll(struct run * runs, int numRuns) {
    int i;
    for(i = 0; i += 2; i < numRuns) {
        struct run run1 = runs[i*2];
        struct run run2 = runs[i*2+1];
        struct key_value *buffer = malloc(size_of_record * (run1.start - run1.end + run2.start - run2.end));
        // To-do: allocate pthread to do this
        merge(run1, run2, buffer); // the last run might not have another run to merge with
    }
    // To-do: return runs (or modify runs in place?)
};


// void merge(struct key_value *input_kv, int start1, int mid, int end, struct key_value *buffer)
// {
//     int i = start; // index of left array
//     int j = mid + 1; // index of right array
//     int k = 0; // index of buffer
//     while (i <= mid && j <= end) { 
//         if (compare(input_kv + i, input_kv + j) < 0) {
//             buffer[k++] = input_kv[i++];
//         } else {
//             buffer[k++] = input_kv[j++];
//         }
//     }
//     while (i <= mid) {
//         buffer[k++] = input_kv[i++];
//     }
//     while (j <= end) {
//         buffer[k++] = input_kv[j++];
//     }
//     memcpy(input_kv + start * size_of_record, buffer, (end - start + 1) * size_of_record);
// }

void qsort_enclosed(void *args) {
    struct qsort_args * range = (struct qsort_args *) args;
    struct key_value * base = range->base;
    size_t nel = range->nel;
    qsort(base, nel, size_of_record, compare);
}

// int get_merge_num_round(int numChunks) {
//     int numChunks_new;
//     int numRounds = 0;
//     while (numChunks > 1) {
//         numRounds++;
//         numChunks_new = numChunks / 2;
//         if (numChunks % 2) numChunks_new++;
//         numChunks = numChunks_new;
//     }
// };

// parallel_sort(fileSize/100, numThreads);
void parallel_sort(struct key_value * input_kv, size_t numRecords, int numThreads)
{
    int numRecords_per_chunk = numRecords / numThreads;
    int numRecords_last_chunk = numRecords - numRecords_per_chunk * (numThreads - 1);
    
    // assign each thread a chunk of data and use qsort to sort
    struct run * runs = malloc(sizeof(struct run) * numThreads);
    int numRuns = numThreads;
    pthread_t * pthreads = malloc(sizeof(pthread_t) * numThreads);
    for (int i = 0; i < numThreads; i++) {
        struct qsort_args range;
        range.base = input_kv + i * numRecords_per_chunk; // address of the first element in the chunk
        runs[i].start = i * numRecords_per_chunk; // index of the first element in the chunk
        range.nel = numRecords_per_chunk; // number of elements in the chunk
        if (i == numThreads - 1) {
            range.nel = numRecords_last_chunk;
        }
        runs[i].end = runs[i].start + range.nel; // index of the last element in the chunk, inclusive
        pthread_create(&pthreads[i], NULL, qsort_enclosed, &range);
    }
    // join all the thread
    for (int i = 0; i < numThreads; i++) {
        pthread_join(pthreads[i], NULL);
    }
    
    // To-do: merge the sorted chunks in the parent thread through a few passes
    while (numRuns > 1) {
        // one pass
    }
    
}


// Your parallel sort (`psort`) will take three command-line arguments.
// input                  The input file to read records for sort
// output               The output file where records will be written after sort
// numThreads      Number of threads that shall perform the sort operation.
int main(int argc, char *argv[])
{
    if (argc != 4) {
        fprintf(stderr, "Usage: %s input output numThreads", argv[0]);
        exit(1);
    }
    char* input = argv[1];
    char* output = argv[2];
    int numThreads = atoi(argv[3]);

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

    // sort value according to key
    parallel_sort(input_kv, fileSize/100, numThreads);

    // write to output_data according to input_kv
    for (int i = 0; i < fileSize/100; i++) {
        memcpy(output_data + i*100, input_kv[i].value, 100);
    }
    
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