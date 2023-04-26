import threading

# Read the input text document from a file
with open('input.txt', 'r') as f:
    input_text = f.read()

# Define the Map function
def map_func(input_str):
    words = input_str.split()
    intermediate = []
    for word in words:
        intermediate.append((word, 1))
    return intermediate

# Define the Shuffle function
def shuffle_func(intermediate):
    # Group the intermediate key-value pairs by key
    shuffled = {}
    for (key, value) in intermediate:
        if key not in shuffled:
            shuffled[key] = []
        shuffled[key].append(value)
    return shuffled

# Define the Reduce function
def reduce_func(key, values):
    # Sum the counts for each key
    return (key, sum(values))

# Create a list of threads to simulate the MapReduce framework
threads = []

# Split the input text into chunks for parallel processing
chunk_size = len(input_text) // 4
chunks = [input_text[i:i+chunk_size] for i in range(0, len(input_text), chunk_size)]

# Start the Map threads
map_intermediate = []
for chunk in chunks:
    t = threading.Thread(target=lambda intermediate, chunk: intermediate.extend(map_func(chunk)), args=(map_intermediate, chunk))
    threads.append(t)
    t.start()

# Wait for all Map threads to finish
for t in threads:
    t.join()

# Start the Shuffle threads
shuffle_intermediate = {}
for (key, value) in shuffle_func(map_intermediate).items():
    t = threading.Thread(target=lambda shuffled, key, values: shuffled.update({key: reduce_func(key, values)}), args=(shuffle_intermediate, key, value))
    threads.append(t)
    t.start()

# Wait for all Shuffle threads to finish
for t in threads[len(chunks):]:
    t.join()

# Store the final output in a dictionary
output = dict(shuffle_intermediate)

# Print the output
for (key, value) in output.items():
    print(f'{key}: {value}'
