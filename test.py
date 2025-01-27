import sys
from queryMongo import main

# Save the original sys.argv
original_argv = sys.argv

try:

    sys.argv = [
        "queryMongo.py", 
        "--denomination", "Evangelical",
        "--size", "10",
        "--returns", "name", "size", "denomination"
    ]

 
    main()

finally:
    # Restore the original sys.argv
    sys.argv = original_argv
