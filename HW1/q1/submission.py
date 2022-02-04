import http.client
import json
import csv


#############################################################################################################################
# cse6242 s21
# All instructions, code comments, etc. contained within this notebook are part of the assignment instructions.
# Portions of this file will auto-graded in Gradescope using different sets of parameters / data to ensure that values are not
# hard-coded.
#
# Instructions:  Implement all methods in this file that have a return
# value of 'NotImplemented'. See the documentation within each method for specific details, including
# the expected return value
#
# Helper Functions:
# You are permitted to write additional helper functions/methods or use additional instance variables within
# the `Graph` class or `TMDbAPIUtils` class so long as the originally included methods work as required.
#
# Use:
# The `Graph` class  is used to represent and store the data for the TMDb co-actor network graph.  This class must
# also provide some basic analytics, i.e., number of nodes, edges, and nodes with the highest degree.
#
# The `TMDbAPIUtils` class is used to retrieve Actor/Movie data using themoviedb.org API.  We have provided a few necessary methods
# to test your code w/ the API, e.g.: get_move_detail(), get_movie_cast(), get_movie_credits_for_person().  You may add additional
# methods and instance variables as desired (see Helper Functions).
#
# The data that you retrieve from the TMDb API is used to build your graph using the Graph class.  After you build your graph using the
# TMDb API data, use the Graph class write_edges_file & write_nodes_file methods to produce the separate nodes and edges
# .csv files for use with the Argo-Lite graph visualization tool.
#
# While building the co-actor graph, you will be required to write code to expand the graph by iterating
# through a portion of the graph nodes and finding similar artists using the TMDb API. We will not grade this code directly
# but will grade the resulting graph data in your Argo-Lite graph snapshot.
#
#############################################################################################################################


class Graph:

    # Do not modify
    def __init__(self, with_nodes_file=None, with_edges_file=None):
        """
        option 1:  init as an empty graph and add nodes
        option 2: init by specifying a path to nodes & edges files
        """
        self.nodes = []
        self.edges = []

        if with_nodes_file and with_edges_file:
            nodes_CSV = csv.reader(open(with_nodes_file))
            nodes_CSV = list(nodes_CSV)[1:]
            self.nodes = [(n[0], n[1]) for n in nodes_CSV]

            edges_CSV = csv.reader(open(with_edges_file))
            edges_CSV = list(edges_CSV)[1:]
            self.edges = [(e[0], e[1]) for e in edges_CSV]

    def add_node(self, id: str, name: str) -> None:
        """
        add a tuple (id, name) representing a node to self.nodes if it does not already exist
        The graph should not contain any duplicate nodes
        """

        if (id, name) not in self.nodes:
            self.nodes.append((id, name))

        return list(set(self.nodes))

    def add_edge(self, source: str, target: str) -> None:
        """
                Add an edge between two nodes if it does not already exist.
                An edge is represented by a tuple containing two strings: e.g.: ('source', 'target').
                Where 'source' is the id of the source node and 'target' is the id of the target node
                e.g., for two nodes with ids 'a' and 'b' respectively, add the tuple ('a', 'b') to self.edges
                """

        if source != target:
            src = (source, target)
            tar = (target, source)
            if src not in self.edges and tar not in self.edges:
                self.edges.append(src)

        return self.edges

    def total_nodes(self) -> int:
        """
        Returns an integer value for the total number of nodes in the graph
        """
        a = len(self.nodes)

        return a

    def total_edges(self) -> int:
        """
        Returns an integer value for the total number of edges in the graph
        """
        b = len(self.edges)
        return b

    def max_degree_nodes(self) -> dict:
        """
        Return the node(s) with the highest degree
        Return multiple nodes in the event of a tie
        Format is a dict where the key is the node_id and the value is an integer for the node degree
        e.g. {'a': 8}
        or {'a': 22, 'b': 22}
        """
        import collections
        dic1 = collections.defaultdict(int)
        for i in self.edges:
            dic1[i[0]] += 1
            dic1[i[1]] += 1
        dic11 = dict((sorted(dic1.items(), key=lambda i: i[1], reverse=True)))
        m = max(dic1.values())
        dic2 = {}
        for x, y in dic1.items():
            if y == m:
                dic2[x] = y

        return dic2

    def print_nodes(self):
        """
        No further implementation required
        May be used for de-bugging if necessary
        """
        print(self.nodes)

    def print_edges(self):
        """
        No further implementation required
        May be used for de-bugging if necessary
        """
        print(self.edges)

    # Do not modify
    def write_edges_file(self, path="edges.csv") -> None:
        """
        write all edges out as .csv
        :param path: string
        :return: None
        """
        edges_path = path
        edges_file = open(edges_path, 'w', encoding='utf-8')

        edges_file.write("source" + "," + "target" + "\n")

        for e in self.edges:
            edges_file.write(e[0] + "," + e[1] + "\n")

        edges_file.close()
        print("finished writing edges to csv")

    # Do not modify
    def write_nodes_file(self, path="nodes.csv") -> None:
        """
        write all nodes out as .csv
        :param path: string
        :return: None
        """
        nodes_path = path
        nodes_file = open(nodes_path, 'w', encoding='utf-8')

        nodes_file.write("id,name" + "\n")
        for n in self.nodes:
            nodes_file.write(n[0] + "," + n[1] + "\n")
        nodes_file.close()
        print("finished writing nodes to csv")


class TMDBAPIUtils:

    # Do not modify
    def __init__(self, api_key: str):
        self.api_key = api_key

    def get_movie_cast(self, movie_id: str, limit: int = None, exclude_ids: list = None) -> list:
        """
        Get the movie cast for a given movie id, with optional parameters to exclude an cast member
        from being returned and/or to limit the number of returned cast members
        documentation url: https://developers.themoviedb.org/3/movies/get-movie-credits

        :param integer movie_id: a movie_id
        :param integer limit: maximum number of returned cast members by their 'order' attribute
            e.g., limit=5 will attempt to return the 5 cast members having 'order' attribute values between 0-4
            If after exluding, there are fewer cast members than the specified limit or the limit not specified, return all cast members.
            If cast members with 'order' attribute in the specified limit range have been excluded, do not include more cast members to reach the limit.
            e.g., if limit=5 and the actor whose id corresponds to cast member with order=1 is to be excluded,
            return cast members with order values [0, 2, 3, 4], not [0, 2, 3, 4, 5]
        :param list exclude_ids: a list of ints containing ids (not cast_ids) of cast members  that should be excluded from the returned result
            e.g., if exclude_ids are [353, 455] then exclude these from any result.
        :rtype: list
            return a list of dicts, one dict per cast member with the following structure:
                [{'cast_id': '97909' # the id of the cast member
                'character': 'John Doe' # the name of the character played
                'credit_id': '52fe4249c3a36847f8012927' # id of the credit}, ... ]
        """

        conn = http.client.HTTPSConnection("api.themoviedb.org")
        conn.request("GET", "/3/movie/" + str(movie_id) + "/credits?api_key=" + self.api_key + "&language=en-US")

        r1 = conn.getresponse()
        data1 = r1.read()
        json_data = json.loads(data1)
        cast = json_data['cast']
        list1 = []
        dict1 = {}
        list2 = []

        if limit != None:
            if limit == 0:
                list1.append(limit)
            else:
                for j in range(limit):
                    list1.append(j)

            for i in json_data['cast']:
                if i['order'] in list1:
                    if exclude_ids != None:
                        # print("b")
                        if not i['id'] in exclude_ids:
                            dict1['id'] = i['id']
                            dict1['character'] = i['character']
                            dict1['credit_id'] = i['credit_id']
                            dict1['name'] = i['name'].replace(',', '')
                            list2.append(dict1.copy())
                    else:
                        # print("C")
                        dict1['id'] = i['id']
                        dict1['character'] = i['character']
                        dict1['credit_id'] = i['credit_id']
                        dict1['name'] = i['name'].replace(',', '')
                        list2.append(dict1.copy())
        else:
            # print("inside else")
            for i in json_data['cast']:
                if exclude_ids != None:
                    if not i['id'] in exclude_ids:
                        dict1['id'] = i['id']
                        dict1['character'] = i['character']
                        dict1['credit_id'] = i['credit_id']
                        dict1['name'] = i['name'].replace(',', '')
                        list2.append(dict1.copy())
                else:
                    dict1['id'] = i['id']
                    dict1['character'] = i['character']
                    dict1['credit_id'] = i['credit_id']
                    dict1['name'] = i['name'].replace(',', '')
                    list2.append(dict1.copy())

        return list2

    def get_movie_credits_for_person(self, person_id: str, vote_avg_threshold: float = None) -> list:
        """
        Using the TMDb API, get the movie credits for a person serving in a cast role
        documentation url: https://developers.themoviedb.org/3/people/get-person-movie-credits

        :param string person_id: the id of a person
        :param vote_avg_threshold: optional parameter to return the movie credit if it is >=
            the specified threshold.
            e.g., if the vote_avg_threshold is 5.0, then only return credits with a vote_avg >= 5.0
        :rtype: list
            return a list of dicts, one dict per movie credit with the following structure:
                [{'id': '97909' # the id of the movie credit
                'title': 'Long, Stock and Two Smoking Barrels' # the title (not original title) of the credit
                'vote_avg': 5.0 # the float value of the vote average value for the credit}, ... ]
        """

        conn = http.client.HTTPSConnection("api.themoviedb.org")
        conn_stmt = "/3/person/" + str(person_id) + "/movie_credits?api_key=" + self.api_key + "&language=en-US"
        conn.request("GET", conn_stmt)
        r1 = conn.getresponse()
        data1 = r1.read()
        data2 = json.loads(data1)

        l = []
        dict1 = {}

        for k in data2['cast']:
            if k['vote_average'] >= vote_avg_threshold:
                dict1['id'] = k['id']
                dict1['title'] = k['title']
                dict1['vote_avg'] = k['vote_average']
                l.append(dict1.copy())

        return l

    def base_graph(self, person_id: str, vote_avg_threshold: float = None):
        print('inside 3')
        # get the Laurence Fishburne's movie credits for a given  w avg>= 8.0
        credit_persons = self.get_movie_credits_for_person(person_id, vote_avg_threshold)

        # get movie_credits for each id
        list_credits = []
        for k in credit_persons:
            credits = self.get_movie_cast(k['id'], 3)
            list_credits.append(credits)

            for i in list_credits:
                for j in i:
                    id = j['id']
                    name = j['name']

                    graph.add_node(str(id), str(name))

            for i in graph.nodes:
                if i[0] != person_id:
                    graph.add_edge(person_id, str(i[0]))

    def cast_order(self, movie_id:str) -> list:

        url1 ='api.themoviedb.org'

        conn = http.client.HTTPSConnection(url1)

        url2 = "/3/movie/"+str(movie_id)+"/credits?api_key="+self.api_key

        conn.request("GET", url2)

        res = conn.getresponse()

        data = res.read().decode("utf-8")

        response = json.loads(data)

        cast = response['cast']

        movie_cast_order = []

        for cst in cast:

            items = cst

            if (items['order'] == 0) or  (items['order'] == 1) or  (items['order'] == 2):

                d = {'id':items['id'], 'name':items['name'], 'order': items['order']}

                movie_cast_order.append(d)



        return movie_cast_order



#############################################################################################################################
#
# BUILDING YOUR GRAPH
#
# Working with the API:  See use of http.request: https://docs.python.org/3/library/http.client.html#examples
#
# Using TMDb's API, build a co-actor network for the actor's/actress' highest rated movies
# In this graph, each node represents an actor
# An edge between any two nodes indicates that the two actors/actresses acted in a movie together
# i.e., they share a movie credit.
# e.g., An edge between Samuel L. Jackson and Robert Downey Jr. indicates that they have acted in one
# or more movies together.
#
# For this assignment, we are interested in a co-actor network of highly rated movies; specifically,
# we only want the top 3 co-actors in each movie credit of an actor having a vote average >= 8.0.
# Build your co-actor graph on the actor 'Laurence Fishburne' w/ person_id 2975.
#
# You will need to add extra functions or code to accomplish this.  We will not directly call or explicitly grade your
# algorithm. We will instead measure the correctness of your output by evaluating the data in your argo-lite graph
# snapshot.
#
# GRAPH SIZE
# With each iteration of your graph build, the number of nodes and edges grows approximately at an exponential rate.
# Our testing indicates growth approximately equal to e^2x.
# Since the TMDB API is a live database, the number of nodes / edges in the final graph will vary slightly depending on when
# you execute your graph building code. We take this into account by rebuilding the solution graph every few days and
# updating the auto-grader.  We establish a bound for lowest & highest encountered numbers of nodes and edges with a
# margin of +/- 100 for nodes and +/- 150 for edges.  e.g., The allowable range of nodes is set to:
#
# Min allowable nodes = min encountered nodes - 100
# Max allowable nodes = max allowable nodes + 100
#
# e.g., if the minimum encountered nodes = 507 and the max encountered nodes = 526, then the min/max range is 407-626
# The same method is used to calculate the edges with the exception of using the aforementioned edge margin.
# ----------------------------------------------------------------------------------------------------------------------
# BEGIN BUILD CO-ACTOR NETWORK
#
# INITIALIZE GRAPH
#   Initialize a Graph object with a single node representing Laurence Fishburne
#
# BEGIN BUILD BASE GRAPH:
#   Find all of Laurence Fishburne's movie credits that have a vote average >= 8.0
#   FOR each movie credit:
#   |   get the movie cast members having an 'order' value between 0-2 (these are the co-actors)
#   |
#   |   FOR each movie cast member:
#   |   |   using graph.add_node(), add the movie cast member as a node (keep track of all new nodes added to the graph)
#   |   |   using graph.add_edge(), add an edge between the Laurence Fishburne (actress) node
#   |   |   and each new node (co-actor/co-actress)
#   |   END FOR
#   END FOR
# END BUILD BASE GRAPH
#
#
# BEGIN LOOP - DO 2 TIMES:
#   IF first iteration of loop:
#   |   nodes = The nodes added in the BUILD BASE GRAPH (this excludes the original node of Laurence Fishburne!)
#   ELSE
#   |    nodes = The nodes added in the previous iteration:
#   ENDIF
#
#   FOR each node in nodes:
#   |  get the movie credits for the actor that have a vote average >= 8.0
#   |
#   |   FOR each movie credit:
#   |   |   try to get the 3 movie cast members having an 'order' value between 0-2
#   |   |
#   |   |   FOR each movie cast member:
#   |   |   |   IF the node doesn't already exist:
#   |   |   |   |    add the node to the graph (track all new nodes added to the graph)
#   |   |   |   ENDIF
#   |   |   |
#   |   |   |   IF the edge does not exist:
#   |   |   |   |   add an edge between the node (actor) and the new node (co-actor/co-actress)
#   |   |   |   ENDIF
#   |   |   END FOR
#   |   END FOR
#   END FOR
# END LOOP
#
# Your graph should not have any duplicate edges or nodes
# Write out your finished graph as a nodes file and an edges file using:
#   graph.write_edges_file()
#   graph.write_nodes_file()
#
# END BUILD CO-ACTOR NETWORK
# ----------------------------------------------------------------------------------------------------------------------

# Exception handling and best practices
# - You should use the param 'language=en-US' in all API calls to avoid encoding issues when writing data to file.
# - If the actor name has a comma char ',' it should be removed to prevent extra columns from being inserted into the .csv file
# - Some movie_credits may actually be collections and do not return cast data. Handle this situation by skipping these instances.
# - While The TMDb API does not have a rate-limiting scheme in place, consider that making hundreds / thousands of calls
#   can occasionally result in timeout errors. If you continue to experience 'ConnectionRefusedError : [Errno 61] Connection refused',
#   - wait a while and then try again.  It may be necessary to insert periodic sleeps when you are building your graph.

def nodes_edges(person_id:str,)->None:


    m_credits = tmdb_api_utils.get_movie_credits_for_person(person_id, 8.0)

    for movies in m_credits:

        cast_order = tmdb_api_utils.cast_order(movies['id'])

        for mc in cast_order:

            graph.add_node(id=str(mc['id']), name=mc['name'].replace(",",""))

            graph.add_edge(source=person_id, target=str(mc['id']) )



    return None


def return_name() -> str:
    """
    Return a string containing your GT Username
    e.g., gburdell3
    Do not return your 9 digit GTId
    """
    name = "lrajasekar3"
    return name


def return_argo_lite_snapshot() -> str:
    """
    Return the shared URL of your published graph in Argo-Lite
    """

    url = "https://poloclub.github.io/argo-graph-lite/#bd9433bb-6ead-4f59-b0be-d0c84e2fa931"
    return url


# You should modify __main__ as you see fit to build/test your graph using  the TMDBAPIUtils & Graph classes.
# Some boilerplate/sample code is provided for demonstration. We will not call __main__ during grading.

if __name__ == "__main__":

    graph = Graph()
    graph.add_node(id='2975', name='Laurence Fishburne')
    tmdb_api_utils = TMDBAPIUtils(api_key='e1fd88f9ffdde3392a8ede7410462e98')

    # print((tmdb_api_utils.base_graph('2975',8.0)))

    #a = tmdb_api_utils.base_graph('2975', 8.0)

    # for i in range(1,):

    # if i == 1:
    # print('inside 1')

    initialnode = graph.nodes
    print(initialnode)
    tmdb_api_utils.base_graph(initialnode[0][0], 8.0)

    l1 = []
    l1 = [i for i in graph.nodes if i != initialnode[0]]
    print(l1)
    print(len(l1))


    for n in l1 :
        nodes_edges(n[0])


    l1.append(initialnode[0])

    print(l1)
    print(len(l1))



    l2 =[]

    graph.print_nodes

    l2 = [i for i in graph.nodes if i not in l1 ]
    print(l2)
    print(len(l2))


    for n in l2 :
        nodes_edges(n[0])

    print(len(l2))


    # call functions or place code here to build graph (graph building code not graded)
    # Suggestion: code should contain steps outlined above in BUILD CO-ACTOR NETWORK

    graph.write_edges_file()
    graph.write_nodes_file()

    # If you have already built & written out your graph, you could read in your nodes & edges files
    # to perform testing on your graph.
    # graph = Graph(with_edges_file="edges.csv", with_nodes_file="nodes.csv")