#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>

struct key_value {
  int key;
  char* value; // start of 100 bytes
};

// parallel_sort(input_kv, fileSize/100, sizeof(struct key_value), compare, numThreads);
void parallel_sort(struct key_value *input_kv, size_t number_of_records, size_t size,
                  int (*compar)(const void *, const void *), int numThreads)
{
    return qsort(input_kv, number_of_records, size, compar);
}


//define compare function according to key converted to int
int compare(const void *a, const void *b) {
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
    struct key_value* input_kv = malloc(fileSize/100 * sizeof(struct key_value));
    for (int i = 0; i < fileSize/100; i++) {
        int key = *(int*)ptr;
        input_kv[i].key = key;
        input_kv[i].value = ptr;
        ptr += 100;
        // printf("key: %d, value: %s\n", key, input_kv[i].value);
    }

    // sort value according to key
    parallel_sort(input_kv, fileSize/100, sizeof(struct key_value), compare, numThreads);

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