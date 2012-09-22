# Excellent article! 
# http://mikecvet.wordpress.com/2010/07/02/parallel-mapreduce-in-python/
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
print "Total count:", N

#-------------------------------------------
# Some more experiments
# with using sets 
a = [1, 2, 3, 4, 4, 4, 5]
b = [4, 5, 6, 7, 8, 4]
c = [8, 9, 1, 2, 3, 3]

A = map(lambda x:set(x), [a, b, c])
print A
B = reduce(lambda list1, list2: list1|list2, A)
print B

#------------------------------
# an example from the documentation - http://docs.python.org/library/sets.html
# Just provided here - for contrast with map/reduce example above
# 
from sets import Set
engineers = Set(['John', 'Jane', 'Jack', 'Janice', 'Jane'])
programmers = Set(['Jack', 'Sam', 'Susan', 'Janice', 'Sam'])
managers = Set(['Jane', 'Jack', 'Susan', 'Zack'])

employees = engineers | programmers | managers           # union
engineering_management = engineers & managers            # intersection
fulltime_management = managers - engineers - programmers # difference
engineers.add('Marvin')                                  # add element
print engineers 
#Set(['Jane', 'Marvin', 'Janice', 'John', 'Jack'])
employees.issuperset(engineers)     # superset test
#False
employees.update(engineers)         # update from another set
employees.issuperset(engineers)
#True
for group in [engineers, programmers, managers, employees]: 
  group.discard('Susan')          # unconditionally remove element
print group

#import itertools
#for item in itertools.chain(listone, listtwo):
#    {do something }
