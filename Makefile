.PHONY: clean

psort: psort.c
	gcc psort.c -Wall -Werror -pthread -O -o psort

psort-gdb: psort.c
	gcc psort.c -Wall -Werror -pthread -g -o psort
	gdb psort

clean:
	rm -f psort-noopt psort-no-warning psort