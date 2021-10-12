import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.getcwd(), '..')))
sys.path.append(os.path.abspath(os.path.join(os.getcwd(), '../..')))

def append_csv(df, output_file):
    if not os.path.isfile(output_file):
        df.to_csv(output_file, index=False, header=True, sep=",")
    else:
        df.to_csv(output_file, mode='a', index=False, header=False, sep=",")
