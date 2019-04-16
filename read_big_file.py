import multiprocessing
from multiprocessing import Pool
import string
from collections import defaultdict
import time
import os
import math

# A better solution would be to pass the position in
# the file to each process, and then each process could seek and read the relevant chunk


# Helper function for adding up all the sub counters (dicts)
def add_dicts(dicts: list):
    res = defaultdict(int)
    for d in dicts:
        for key in d:
            res[key] += d[key]
    return res


def process_chunk_by_pos(pos):
    word_counter = defaultdict(int)
    with open(big_file_path) as f:
        f.seek(pos * size_of_chunk)
        chunk = f.read(size_of_chunk)
        for word in chunk.lower().translate(str.maketrans('', '', string.punctuation)).split():
            word_counter[word] += 1
    return word_counter



start = time.time()
num_of_cores = multiprocessing.cpu_count()  # GETTING THE NUMBER OF CORES IN THE SYSTEM
pool = Pool(num_of_cores)

with open(big_file_path) as f:

    # SPLIT THE FILE INTO EQUAL CHUNKS of a megabyte
    num_of_chunks = int(math.ceil(os.path.getsize(big_file_path) / 1000000))
    results = pool.map(process_chunk_by_pos, range(num_of_chunks))

    # ADDING THE DICTIONARIES
    final_result = add_dicts(results)

end = time.time()
print("with multiprocessing:", end - start, "seconds")




