import lru
import time


ld = lru.LRUDict(5)
ld["test"] = "5 seconds until deletion"

print ld

time.sleep(4)
print ld["test"]

time.sleep(2)
print ld["test"]
