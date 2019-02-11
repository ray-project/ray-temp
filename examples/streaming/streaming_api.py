import time
import logging

from ray.slib.streaming import *

logger = logging.getLogger(__name__)
logger.setLevel("INFO")

# Test functions
def splitter(line):
    return line.split(' ')

def filter_fn(word):
     if "f" in word: return True
     return False


if __name__ == '__main__':
    ray.init()
    ray.register_custom_serializer(BatchedQueue, use_pickle=True)
    ray.register_custom_serializer(OpType, use_pickle=True)
    ray.register_custom_serializer(PStrategy, use_pickle=True)

    start = time.time()

    # A Ray streaming environment with the default configuration
    env = Environment()

    # Stream represents the ouput of the filter and can be forked into other dataflows
    stream = env.read_text_file("test_input.txt") \
                    .shuffle() \
                    .flat_map(splitter) \
                    .set_parallelism(4) \
                    .filter(filter_fn) \
                    .set_parallelism(2) \
                    .inspect(print)         # Prints the contents of the stream
                    # .key_by(0) \
                    # .set_parallelism(2) \
                    # .time_window(10**4) \
                    # .broadcast()
                    # .set_parallelism(4) \
                    # .filter(filter_fn) \
                    # .set_parallelism(2) \

    # print("Branch 1")
    # stream2 = stream.shuffle().filter(different_split) \
    #         .set_parallelism(4) \
    #         .key_by(0) \
    #         .set_parallelism(2) \
    #         .time_window(10**4) #milliseconds
    #
    # # print("Branch 2")
    # stream.filter(split2) \
    #         .set_parallelism(2) \
    #         .key_by(0) \
    #         .time_window(10**4) \
    #         .sink()

    # print("Branch 3")
    # stream.filter(different_split) \
    #         .set_parallelism(2) \
    #         .key_by(0) \
    #         .time_window(10**4) \
    #         .sink()
    #
    # print("Branch 4")
    # stream.partition(split).filter(different_split) \
    #         .set_parallelism(2) \
    #         .key_by(0) \
    #         .time_window(10**4) \
    #         .sink()

    end = time.time()
    print("Assembled logical dataflow in {} secs\n".format(end-start))
    env.print_logical_graph()
    start = time.time()
    env.execute()
    end = time.time()
    print("Elapsed time: {} secs".format(end-start))
    #env.print_physical_graph()

    #time.sleep(20)
