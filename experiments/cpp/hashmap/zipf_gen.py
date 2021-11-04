import numpy as np
import sys

def main():
    s = np.random.zipf(1.5, 100000000).tolist();
    result = ' '.join(str(x) for x in s)
    f = open("zipf.txt", "w")
    f.write(result)
    f.close()

if __name__ == "__main__":
    main()
