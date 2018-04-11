// Test of different hashing functions
#include <stdio.h>

unsigned int hashstring(unsigned char *str)
{
    unsigned long hash = 5381;
    int c;

    while (c = *str++)
        hash = ((hash << 5) + hash) + (c - 'a'); /* hash * 33 + c */
	return hash;
    return (hash+(hash>>32));
}

unsigned long sdbm(unsigned char *str)
    {
        unsigned long hash = 0;
        int c;

        while (c = *str++)
            hash = c + (hash << 6) + (hash << 16) - hash;

        return hash;
    }

int main () {
	printf("%d\n", hashstring("zzzzz"));
}
