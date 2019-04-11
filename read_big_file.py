import multiprocessing
from multiprocessing import Pool
import string
from collections import defaultdict
import time
import os
import math

# A better solution would be to pass the position in
# the file to each process, and then each process could seek and read the relevant chunk


def process_chunk(chuck: str):
    word_counter = defaultdict(int)
    for word in chuck.lower().translate(str.maketrans('', '', string.punctuation)).split():
            word_counter[word] += 1
    return word_counter


# Helper function for adding up all the sub counters (dicts)
def add_dicts(dicts: list):
    res = defaultdict(int)
    for d in dicts:
        for key in d:
            res[key] += d[key]
    return res


# Helper function for splitting the file to equal sizes
def split_equal(file, byte_count):
    content = file.read()
    return (content[i: i + byte_count] for i in range(0, len(content), byte_count))


def process_chunk_by_pos(pos):
    word_counter = defaultdict(int)
    with open('C:/Users/lenove/Desktop/idc/big.txt') as f:
        f.seek(pos * 1000000)
        chunk = f.read(1000000)
        for word in chunk.lower().translate(str.maketrans('', '', string.punctuation)).split():
            word_counter[word] += 1
    return word_counter


if __name__ == "__main__":

    start = time.time()
    num_of_cores = multiprocessing.cpu_count()  # GETTING THE NUMBER OF CORES IN THE SYSTEM
    pool = Pool(num_of_cores)

    with open('C:/Users/lenove/Desktop/idc/big.txt') as f:

        # SPLIT THE FILE INTO EQUAL CHUNKS of a megabyte
        num_of_chunks = int(math.ceil(os.path.getsize('C:/Users/lenove/Desktop/idc/big.txt') / 1000000))
        results = pool.map(process_chunk_by_pos, range(num_of_chunks))

        # ADDING THE DICTIONARIES
        final_result = add_dicts(results)

    end = time.time()
    print("with multiprocessing:", end - start, "seconds")




