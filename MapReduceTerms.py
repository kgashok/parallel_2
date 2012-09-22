# Excellent article! 
# http://mikecvet.wordpress.com/2010/07/02/parallel-mapreduce-in-python/
# http://docs.python.org/library/multiprocessing.html - please read the notes
#
a = [1, 2, 3]
b = [4, 5, 6, 7]
c = [8, 9, 1, 2, 3]
L = map(lambda x:len(x), [a, b, c])
# L == [3, 4, 5]
N = reduce(lambda x, y: x+y, L)
# N == 12
# Or, if we want to be fancy and do it in one line
N = reduce(lambda x, y: x+y, map(lambda x:len(x), [a, b, c]))

"""
Given a list of tokens, return a list of tuples of
titlecased (or proper noun) tokens and a count of '1'.
Also remove any leading or trailing punctuation from
each token.
"""
def Map(L):

  results = []
  for w in L:
    # True if w contains non-alphanumeric characters
    if not w.isalnum():
      w = sanitize (w)

    # True if w is a title-cased token
    if w.istitle():
      results.append ((w, 1))

  return results

"""
Group the sublists of (token, 1) pairs into a term-frequency-list
map, so that the Reduce operation later can work on sorted
term counts. The returned result is a dictionary with the structure
{token : [(token, 1), ...] .. }
"""
def Partition(L):
  tf = {}
  for sublist in L:
    for p in sublist:
      # Append the tuple to the list in the map
      try:
        tf[p[0]].append (p)
      except KeyError:
        tf[p[0]] = [p]
  return tf

"""
Given a (token, [(token, 1) ...]) tuple, collapse all the
count tuples from the Map operation into a single term frequency
number for this token, and return a final tuple (token, frequency).
"""
def Reduce(Mapping):
  return (Mapping[0], sum(pair[1] for pair in Mapping[1]))

################# IMPORTS ###################
import sys
import time
from multiprocessing import Pool
#############################################

"""
If a token has been identified to contain
non-alphanumeric characters, such as punctuation,
assume it is leading or trailing punctuation
and trim them off. Other internal punctuation
is left intact.
"""
def sanitize(w):

  # Strip punctuation from the front
  while len(w) > 0 and not w[0].isalnum():
    w = w[1:]

  # String punctuation from the back
  while len(w) > 0 and not w[-1].isalnum():
    w = w[:-1]

  return w
"""
Load the contents the file at the given
path into a big string and return it.
"""
def load(path):

  word_list = []
  f = open(path, "r")
  
  for line in f:
    word_list.append (line)

  # Efficiently concatenate Python string objects
  return (''.join(word_list)).split ()

"""
A generator function for chopping up a given list into chunks of
length n.
"""
def chunks(l, n):
  for i in xrange(0, len(l), n):
    yield l[i:i+n]               # instantly makes chunks a "generator function" instead of a normal function

"""
Sort tuples by term frequency, and then alphabetically.
"""
def tuple_sort (a, b):
  if a[1] < b[1]:
    return 1
  elif a[1] > b[1]:
    return -1
  else:
    return cmp(a[0], b[0])

# Not used here, but provided here for future use	
def time_execution(code):
    start = time.clock()
    result = eval(code) # evaluate any string as if it is a python command
    run_time = time.clock() - start
    return result, run_time
		
 

#
#  Usage: At the shell prompt, for single file processing (e.g. Pride.txt) type in 
#     python MapReduceTerms.py ./Pride.txt 
#  For multiple files,    
#     python MapReduceTerms.py ./Pride.txt ./Ramayana.txt
#
if __name__ == '__main__':

  #if (len(sys.argv) != 2):
  if (len(sys.argv) < 2):
    print "Program requires path to file for reading!"
    sys.exit(1)
  
  n = 8
  #echo = False # choose whether to echo results of the processing
  echo = True # choose whether to echo results of the processing
  
  text = []
  for e in sys.argv[1:]:
      # Load file, stuff it into a string
      text += load (e)

  t = [0]
  for c in range (1,n+1):
    total = 0
    start = time.clock()
   	  
    # Build a pool of 'c' processes
    pool = Pool(processes=c)
    	
    # Fragment the string data into 'c' chunks
    partitioned_text = list(chunks(text, len(text) / c))

    # Generate count tuples for title-cased tokens
    single_count_tuples = pool.map(Map, partitioned_text)
    
    # Organize the count tuples; lists of tuples by token key
    token_to_tuples = Partition(single_count_tuples)

    # Collapse the lists of tuples into total term frequencies
    term_frequencies = pool.map(Reduce, token_to_tuples.items())

		
    t.append(time.clock() - start)
    if c == 1:
      print "Parallel", c, "Total time: %.2f ----" % float(t[c])
    else:
      print "Parallel", c, "Total time: %.2f %.2f %.2f ---" % (t[c], 100*(pt-t[c])/pt, 100*(t[1]-t[c])/t[1])
    pt = t[c]
    pool.terminate()

  if echo:
    # Sort the term frequencies in nonincreasing order
    term_frequencies.sort (tuple_sort)
    for pair in term_frequencies[:20]:
      print pair[0], ":", pair[1]	
  # print "Number of tuples processed :", len(single_count_tuples)
  print "term frequencies:", len(term_frequencies)


