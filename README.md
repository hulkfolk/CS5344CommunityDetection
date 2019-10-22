# CS5344CommunityDetection
Seed Set Expansion:

    - Dependencies: python-louvain, networkx
    - How to run: python seed_expansion.py ../data/<data file>

COPRA:
```
  coprs.js
    - Dependencies: networkx
    - How to run: python copra/copra.py --filename <data file> --communities <number of communities>
    
  copra-spark.js
    - How to run: spark-submit copra-spark.js <data file> <number of communities>
```

SLPA:
```
- Dependencies: slpa.py
- How to run: spark-submit slpa.py --percentage <criteria of community for a user> --iteration <number of iterations> --filename <data file>
```
