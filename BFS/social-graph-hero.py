from pyspark import SparkConf, SparkContext


# Setup local Context with defined AppName
conf = SparkConf().setMaster("local").setAppName("DegreesOfSeparation")
sc = SparkContext(conf=conf)


start_character_id = 5306 # SpiderMan
target_character_id = 14 # Adam 3031

hit_counter = sc.accumulator(0)


# Helper functions
def convert_breadth_first_search(line):
    """
    Return an array containing the first field hero ID, all the connections it has and distance.
    if the start_character_id is equal to the hero_id, it will have distance 0 and color gray
    :param line:
    :return: hero_id connections distance color
    """
    fields = line.split()
    hero_id = int(fields[0])
    connections = []
    for hero in fields[1:]:
        connections.append(int(hero))

    distance = 9999
    color = "white"

    if hero_id == start_character_id:
        distance = 0
        color = "gray"

    return (hero_id, (connections, distance, color))


def create_start_RDD():
    input_file = sc.textFile("Marvel-Graph.txt")
    return input_file.map(convert_breadth_first_search)

def bfs_map(node):
    character_id = node[0]
    data = node[1]
    connections = data[0]
    distance = data[1]
    color = data[2]

    results = []

    # if node needs expansion
    if color == "gray":
        for connection in connections:
            new_character_id = connection
            new_distance = distance + 1
            new_color = "gray"
            if target_character_id == connection:
                hit_counter.add(1)

            new_entry = (new_character_id, ([], new_distance, new_color))
            results.append(new_entry)

        # node is processed, so color it black
        color = "black"

    # emit the input node for tracking
    results.append((character_id, (connections, distance, color)))
    return results

def bfs_reduce(data1, data2):
    edges1 = data1[0]
    edges2 = data2[0]
    distance1 = data1[1]
    distance2 = data2[1]
    color1 = data1[2]
    color2 = data2[2]

    distance = 9999
    color = color1
    edges = []

    # see if one is the original node with its connections.
    # if so, preserve it.
    if len(edges1) > 0:
        edges = edges1
    if len(edges2) > 0:
        edges = edges2
    # preserver minimun distance
    if distance1 < distance:
        distance = distance1
    if distance2 < distance:
        distance = distance2
    # preserve darkest color
    if color1 == "white" and (color2 in ("gray", "black")):
        color = color2
    if color1 == 'gray' and color2 == 'black':
        color = color2
    if color2 == "white" and (color1 in ("gray", "black")):
        color = color1
    if color2 == 'gray' and color1 == 'black':
        color = color1

    return edges, distance, color


##########################################################################################
### MAIN PROGRAM
##########################################################################################

iteration_rdd = create_start_RDD()

for iteration in range(0,15):
    print("Running BFS iteration", str(iteration + 1))

    # Create new vertices as needed to darken or reduce distances. If we encounter the GRAY node we are looking for,
    # increment the acummulator to know we're done
    mapped = iteration_rdd.flatMap(bfs_map)

    # Since the rdd is lazy, count() is an ACTION makes it to be evaluated and update the hitCounter variable
    print("Processing", str(mapped.count()), "values.")

    if hit_counter.value > 0:
        print("Hit the target character! It took", str(iteration + 1),
              "iterations to go from", str(start_character_id),"to",str(target_character_id),
              "! It came from",str(hit_counter.value), "different direction(s).")
        break

    # Reducer combines data from each characterID, preserving the darkest color and shortest path
    iteration_rdd = mapped.reduceByKey(bfs_reduce)