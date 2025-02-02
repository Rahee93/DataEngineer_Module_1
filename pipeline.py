import sys
import pandas as pd

# Prints the arguments passed to the script
print(sys.argv)  
day = sys.argv[1]

# Use f-string for proper string interpolation
print(f'Work is done successfully for day = {day}')  

