

README file for parallel_2

Code for the snippets available in the article http://mikecvet.wordpress.com/2010/07/02/parallel-mapreduce-in-python/
Please read the documentation at http://docs.python.org/library/multiprocessing.html

------ TEXT FORMAT of books for processing ------------

Pride.txt  - Pride and Prejudice 
Ramayana.txt - Story of Rama and Sita

------------------------
MapReduceEx.py - to practice using Map and Reduce in python
MapReduceTerms.py - actual program which launches the multiprocessing

------------------
Usage: At the shell prompt, for single file processing (e.g. Pride.txt) type in 
  python MapReduceTerms.py ./Pride.txt 
For multiple files,    
  python MapReduceTerms.py ./Pride.txt ./Ramayana.txt

------------------------
Output Capture. PNG

Shows the terms that were extracted by processing the Pride.txt file

---
 23 Sep 2012, 1:45 AM 
* 
