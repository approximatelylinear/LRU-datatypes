import heapq
from collections import defaultdict, deque
import datetime
import time


class LRUList(object):
    """
    This class acts as a container that has a maximum age for all its elements.
    Once an element is older than this age, it is flushed out.  However, if you
    try to re-add an existing element, its age is reset, and it will live longer
    in the container.
    
    If age is 0, age must be specified per-append, otherwise the element won't
    persist.
    
    If length is 0, it holds unlimited objects.
    """
    def __init__(self, age=0, length=0):
        self.age = age
        self.length = length
        self.heap = []
        self.set = set()
        self._updated_times = {}
        
    def append(self, v, age=0, cb=None):
        self.touch(v) # this does a flush too, so we don't do it here
        
        # we've exceeded the length, pop off the oldest value
        # if our length is 0, this stuff is never called
        if self.length and len(self.set) > self.length:
            old_t, old_v, old_insertion, old_cb = heapq.heappop(self.heap)
            self.set.remove(old_v)
            try: del self._updated_times[old_v]
            except: pass
            
        if v not in self.set: 
            self.set.add(v)
            now = time.time()
            heapq.heappush(self.heap, (now + (age or self.age), v, now, cb))
        
            
    def remove(self, v):
        self.set.remove(v)
        for item in list(self.heap):
            t, el, now = item
            if el is v:
                self.heap.remove(item)
                break
        heapq.heapify(self.heap)
        try: del self._updated_times[v]
        except: pass
        
            
    def __len__(self):
        self.flush()
        return len(self.heap)
    
    def __iter__(self):
        """
        this is so inefficient
        """
        self.flush()
        temp = list()
        
        while self.heap:
            el = heapq.heappop(self.heap)
            temp.append(el)
            yield el[1]
            
        heapq.heapify(temp)
        self.heap = temp
        
            
    def extend(self, l, individual_expirations=False):
        for item in l:
            if individual_expirations: self.append(*item)
            else: self.append(item)
            
            
    def _pop(self):
        # the first part gets how long the object is to be stored for.
        # then we add that to the time it was "inserted" (or touched) to get
        # the final expiration time.
        # why this was so hard for my mind to wrap around is beyond me.
        expiration_time, k, insertion, cb = heapq.heappop(self.heap)
        adjusted_expiration_time = (expiration_time - insertion) + self._updated_times.get(k, insertion)
        offset = adjusted_expiration_time - expiration_time
        expiration_time += offset
        insertion += offset
        
        try: del self._updated_times[k]
        except: pass
        
        return expiration_time, k, insertion, cb
    
        
    def flush(self):
        """ flushes out old entries"""
        try: expiration_time, v, insertion, cb = self._pop()
        except IndexError: return
        
        now = time.time()
                
        while now > expiration_time:
            self.set.discard(v)
            
            # we have to pass in the name of the method, because instance
            # methods won't pickle
            if cb: cb = getattr(v, cb, None)
            if callable(cb): cb()
            
            try: expiration_time, v, insertion, cb = self._pop()
            except IndexError: return

        heapq.heappush(self.heap, (expiration_time, v, insertion, cb))
        
        
    def touch(self, v):
        """updates the insert time of an existing object if we try to insert
        it twice"""
        self.flush()
        if v in self.set:
            self._updated_times[v] = time.time()

    def __contains__(self, v):
        self.flush()
        return v in self.set
    
    def __repr__(self):
        self.flush()
        return repr(list(self.set))
    
    
class LRUDict(object):
    """
    This class acts as a container that has a maximum age for all its elements.
    Once an element is older than this age, it is flushed out.  However, if you
    try to re-add an existing element, its age is reset, and it will live longer
    in the container.
    """
    def __init__(self, age=0):
        self.age = age
        self.heap = []
        self._dict = {}
        self._updated_times = {}
        
    def set(self, k, v, age=0):
        self.touch(k) # this does a flush too, so we don't do it here
            
        if k not in self._dict: 
            self._dict[k] = v
            now = time.time()
            heapq.heappush(self.heap, (now + (age or self.age), k, now))
    __setitem__ = set
    
    def get(self, k, default=None):
        try: return self[k]
        except KeyError: return default
    
    def __getitem__(self, k):
        self.flush()
        return self._dict[k]
            
    def __delitem__(self, k):
        try: del self._dict[k]
        except: pass
        for item in list(self.heap):
            t, el, now = item
            if el is k:
                self.heap.remove(item)
                break
        heapq.heapify(self.heap)
        try: del self._updated_times[k]
        except: pass
        
            
    def __len__(self):
        self.flush()
        return len(self.heap)
    
    def __iter__(self):
        """
        this is so inefficient
        """
        self.flush()
        temp = list()
        
        while self.heap:
            el = heapq.heappop(self.heap)
            temp.append(el)
            yield el[1]
            
        heapq.heapify(temp)
        self.heap = temp
        
    
    def _pop(self):
        # the first part gets how long the object is to be stored for.
        # then we add that to the time it was "inserted" (or touched) to get
        # the final expiration time.
        # why this was so hard for my mind to wrap around is beyond me.
        expiration_time, k, insertion = heapq.heappop(self.heap)
        adjusted_expiration_time = (expiration_time - insertion) + self._updated_times.get(k, insertion)
        offset = adjusted_expiration_time - expiration_time
        expiration_time += offset
        insertion += offset
        
        try: del self._updated_times[k]
        except: pass
        
        return expiration_time, k, insertion
    
        
    def flush(self):
        """ flushes out old entries"""
        try: expiration_time, k, insertion = self._pop()
        except IndexError: return        

        now = time.time()        
                
        while now > expiration_time:
            
            try: del self._dict[k]
            except: pass            
            try: expiration_time, k, insertion = self._pop()
            except IndexError: return

        heapq.heappush(self.heap, (expiration_time, k, insertion))
        
        
    def touch(self, k):
        """updates the insert time of an existing object if we try to insert
        it twice"""
        self.flush()
        if k in self._dict:
            self._updated_times[k] = time.time()

    def __contains__(self, k):
        self.flush()
        return k in self._dict
    
    def __repr__(self):
        self.flush()
        return repr(self._dict)
