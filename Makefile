psort:
	gcc psort.c -Wall -Werror -pthread -O -o psort

clean:
	rm -f psort-noopt psort-no-warning psort