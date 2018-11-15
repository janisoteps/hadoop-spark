# %%writefile mapper_2.py
# #!/usr/bin/env python3

import sys

for line in sys.stdin:
    try:
        print(line)
    except ValueError as e:
        continue
