import multiprocessing
import os
import glob
from collections import defaultdict

# Step-1 Map function (by worker nodes)
def map_worker(filename):
    """Reads a file and emits (word, 1) pairs."""
    with open(filename, "r") as f:
        words = f.read().split()

    return [(word.lower(), 1) for word in words]
    
# Step-2 Shuffle phase
def shuffle(mapped_data):
    """Groups values by key (word)."""
    group_data = defaultdict(list)
    for word, count in mapped_data:
        group_data[word].append(count)
    return group_data

# Step-3 Reduce function (by worker)
def reduce_worker(word_counts):
    """Aggregates word counts."""
    word, counts = word_counts
    return (word, sum(counts))

# Master process
def master(input_folder):
    """Manages the entire MapReduce process."""

    # Step-1 find all the text files
    files = glob.glob(os.path.join(input_folder, "*.txt"))

    # Step-2 Parallel Map Execution
    with multiprocessing.Pool(processes=4) as pool:
        mapped_results = pool.map(map_worker, files)

    # flatten results
    mapped_data = [pair for sublist in mapped_results for pair in sublist]

    # Step-3 Shuffle phase (group by key)
    group_data = shuffle(mapped_data)

    # Step-4 Parallel reduce execution
    with multiprocessing.Pool(processes=4) as pool:
        final_results = pool.map(reduce_worker, group_data.items())

    print("\nFinal Word Count:")
    for word, count in sorted(final_results, key=lambda x: -x[1]):
        print(f"{word}: {count}")

# Run the Master Process
if __name__ == "__main__":
    master("input_data") # folder containing text files