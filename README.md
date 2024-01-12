# Parallel Sorting

This C program utilizes multiple threads for efficient data sorting.

Technical Specifications

- Concurrency: Linux pthreads
- Sorting Algorithm: Quick Sort and Merge Sort

## Usage

- Compilation: `gcc -Wall -Werror -pthread -O psort.c -o psort`
- Execution Example: `./psort input_file output_file num_threads`

Developed and tested on Linux using multi-processor machines.

Acknowledgements: Thanks to CS 537 staff at University of Wisconsin–Madison for providing project description and test cases.